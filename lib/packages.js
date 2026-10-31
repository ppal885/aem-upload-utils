/**
 * Content packages built from a folder the user sent from the browser: build jobs, the per-owner list,
 * downloads, expiry and installing into AEM through CRX Package Manager.
 * Packages live in PACKAGES_DIR (default ./packages) for PACKAGE_KEEP_DAYS (default 7) days.
 */
const crypto = require('crypto');
const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');
const { buildPackage } = require('../package-builder');

const PACKAGES_DIR = path.resolve(process.env.PACKAGES_DIR || path.join(__dirname, '..', 'packages'));
const INDEX_FILE = path.join(PACKAGES_DIR, 'packages.json');
const KEEP_MS = (parseInt(process.env.PACKAGE_KEEP_DAYS, 10) || 7) * 24 * 60 * 60 * 1000;
// Content copied from another AEM server (often a customer's) is kept only briefly
const COPY_KEEP_MS = (parseInt(process.env.COPY_PACKAGE_KEEP_HOURS, 10) || 24) * 60 * 60 * 1000;
const INSTALL_TIMEOUT_MS = 30 * 60 * 1000;

fs.mkdirSync(PACKAGES_DIR, { recursive: true });

let packages = [];
try { packages = JSON.parse(fs.readFileSync(INDEX_FILE, 'utf8')); } catch (_) { packages = []; }
// A build that was running when the server stopped will never finish
packages.forEach((p) => { if (p.state === 'building') { p.state = 'failed'; p.error = 'The server restarted while building. Build it again.'; } });

function save() {
    const tmp = INDEX_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(packages, null, 2));
    fs.renameSync(tmp, INDEX_FILE);
}

function publicPackage(p) {
    const { file, ...rest } = p;
    return rest;
}

function list(owner, isAdmin) {
    return packages.filter((p) => isAdmin || p.owner === owner).map(publicPackage);
}

function get(id) {
    return packages.find((p) => p.id === id) || null;
}

function fileOf(p) {
    return path.join(PACKAGES_DIR, p.file);
}

/**
 * Starts building in the background; returns the package record right away (state "building").
 * onDone(err) runs after the build, so the caller can remove the staged files.
 */
function startBuild({ owner, ownerName, contentDir, damPath, packageName, version, group, description, fileCount, copiedFrom, keepNames }, onDone) {
    const id = crypto.randomUUID();
    const safe = `${packageName}-${version}`.replace(/[^A-Za-z0-9._-]+/g, '-');
    const record = {
        id, owner, ownerName, name: packageName, version, group, description: description || '',
        damPath: '/' + damPath.replace(/^\/+/, ''), file: `${id}/${safe}.zip`, downloadName: `${safe}.zip`,
        state: 'building', progress: { files: 0, totalFiles: fileCount || 0 }, createdAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + (copiedFrom ? COPY_KEEP_MS : KEEP_MS)).toISOString(), installs: [],
        ...(copiedFrom ? { copiedFrom } : {}),
    };
    packages.unshift(record);
    save();
    fs.mkdirSync(path.dirname(fileOf(record)), { recursive: true });
    const started = Date.now();
    buildPackage({
        sourceDir: contentDir, damRoot: record.damPath, packageName, version, group, description,
        createdBy: ownerName || owner, skipTopFolder: true, keepNames: !!copiedFrom || keepNames === true, outFile: fileOf(record),
        onProgress: ({ files, totalFiles }) => { record.progress = { files, totalFiles }; },
    }).then((res) => {
        Object.assign(record, {
            state: 'ready', files: res.files, folders: res.folders, bytes: res.bytes, zipBytes: res.zipBytes,
            filterRoots: res.filterRoots, skipped: res.skipped.slice(0, 50), skippedCount: res.skipped.length,
            buildSeconds: Math.round((Date.now() - started) / 100) / 10, progress: { files: res.files, totalFiles: res.files },
        });
        save();
        onDone(null, record);
    }).catch((e) => {
        Object.assign(record, { state: 'failed', error: e.message });
        save();
        fsp.rm(path.dirname(fileOf(record)), { recursive: true, force: true }).catch(() => {});
        onDone(e, record);
    });
    return record;
}

/** Uploads and installs the package with CRX Package Manager; resolves to { ok, message, log }. */
async function install(p, { baseUrl, username, password }) {
    const form = new FormData();
    // fs.openAsBlob reads the zip from disk as it is sent; Node 18 does not have it, so the zip is read into memory there
    const blob = typeof fs.openAsBlob === 'function'
        ? await fs.openAsBlob(fileOf(p), { type: 'application/zip' })
        : new Blob([await fsp.readFile(fileOf(p))], { type: 'application/zip' });
    form.append('file', blob, p.downloadName);
    form.append('force', 'true');
    form.append('install', 'true');
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), INSTALL_TIMEOUT_MS);
    try {
        const r = await fetch(`${baseUrl.replace(/\/+$/, '')}/crx/packmgr/service.jsp`, {
            method: 'POST',
            headers: { Authorization: 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64') },
            body: form,
            signal: ac.signal,
        });
        const text = await r.text();
        if (r.status === 401) return { ok: false, message: 'AEM did not accept the username or password.' };
        if (r.status === 403 || r.status === 404) {
            return { ok: false, message: `AEM refused Package Manager (HTTP ${r.status}). The account needs Package Manager rights; or download the package and install it in CRX Package Manager.` };
        }
        const code = /<status code="(\d+)">([^<]*)<\/status>/.exec(text);
        const ok = r.ok && code && code[1] === '200';
        return {
            ok,
            message: ok
                ? (/\.adobeaemcloud\.com/i.test(baseUrl) ? 'Installed. Run Reprocess Assets on the folder in AEM so the files get renditions and Guides processing.' : 'Installed in AEM.')
                : `AEM could not install the package: ${code ? code[2] : 'HTTP ' + r.status}`,
            log: (/<log>([\s\S]*?)<\/log>/.exec(text) || [])[1]?.slice(-4000) || '',
        };
    } finally {
        clearTimeout(timer);
    }
}

function recordInstall(p, entry) {
    p.installs.unshift(entry);
    p.installs.length = Math.min(p.installs.length, 20);
    save();
}

/** Whether the zip is still on disk (it can be gone after a PACKAGES_DIR change or a manual clean-up). */
function fileExists(p) {
    try { return fs.statSync(fileOf(p)).isFile(); } catch (_) { return false; }
}

/** A ready package whose zip has disappeared: it cannot be downloaded or installed any more. */
function markMissing(p) {
    Object.assign(p, { state: 'failed', error: 'The package file is no longer on the server. Build it again.' });
    save();
}

/** Removes the package and its zip. */
async function remove(p) {
    packages = packages.filter((x) => x !== p);
    save();
    await fsp.rm(path.dirname(fileOf(p)), { recursive: true, force: true }).catch(() => {});
}

async function sweepExpired() {
    const now = Date.now();
    const expired = packages.filter((p) => p.state !== 'building' && Date.parse(p.expiresAt) < now);
    for (const p of expired) await fsp.rm(path.dirname(fileOf(p)), { recursive: true, force: true }).catch(() => {});
    if (expired.length) {
        packages = packages.filter((p) => !expired.includes(p));
        save();
    }
    return expired.length;
}

module.exports = { PACKAGES_DIR, list, get, fileOf, fileExists, markMissing, remove, startBuild, install, recordInstall, sweepExpired, publicPackage };
