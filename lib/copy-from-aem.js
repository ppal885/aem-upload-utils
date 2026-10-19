/**
 * Read-only copy of a DAM folder from an AEM server. Only GET requests are sent to the source: one QueryBuilder
 * search lists the assets (with AEM's own dam:sha1), then the original renditions are downloaded in parallel
 * and each file is checked against that SHA-1. Nothing is created, changed or locked on the source server.
 */
const crypto = require('crypto');
const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');
const refs = require('./guides-refs');
const adaptive = require('./adaptive-pipelines');

const DEFAULT_CONCURRENCY = 8;
const MAX_CONCURRENCY = 16;
const ATTEMPTS = 3;
const LIST_TIMEOUT_MS = 10 * 60 * 1000;
const FILE_TIMEOUT_MS = 10 * 60 * 1000;
// GUIDs pointing outside the copied folder are looked up one by one on the source
const MAX_OUTSIDE_LOOKUPS = 2000;
// Downloads at a time are measured every window (lib/adaptive-pipelines.js); a retried 429/5xx counts as overload
const ADAPT_WINDOW_MS = parseInt(process.env.COPY_ADAPT_WINDOW_MS, 10) || 15000;

/** "/content/dam/a/b" (no trailing slash) or { error }. Only folders inside /content/dam can be copied. */
function normalizeSourceFolder(raw) {
    const p = '/' + String(raw || '').trim().replace(/\\/g, '/').replace(/^\/+|\/+$/g, '');
    if (!/^\/content\/dam\/[^/]/.test(p)) return { error: 'Choose a folder inside /content/dam on the source server.' };
    if (p.split('/').some((s) => s === '.' || s === '..')) return { error: 'The folder path must not contain . or .. segments.' };
    return { path: p };
}

const encodePath = (p) => p.split('/').map(encodeURIComponent).join('/');

function authHeader(username, password) {
    return { Authorization: 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64') };
}

function timeoutSignal(signal, ms) {
    const t = AbortSignal.timeout(ms);
    return signal ? AbortSignal.any([signal, t]) : t;
}

/** Lists the assets under folder: [{ path, rel, sha1, size }], rel being relative to the folder's parent. */
async function listAssets({ baseUrl, username, password, folder, signal }) {
    const q = new URLSearchParams({
        path: folder, type: 'dam:Asset', 'p.limit': '-1', 'p.hits': 'selective',
        'p.properties': 'jcr:path jcr:content/fmUuid jcr:content/metadata/dam:sha1 jcr:content/metadata/dam:size',
    });
    const r = await fetch(`${baseUrl}/bin/querybuilder.json?${q}`, {
        headers: authHeader(username, password), signal: timeoutSignal(signal, LIST_TIMEOUT_MS), redirect: 'error',
    });
    if (r.status === 401) throw new Error('The source AEM did not accept the username or password.');
    if (!r.ok) throw new Error(`The source AEM search answered HTTP ${r.status}.`);
    const j = await r.json();
    const parent = folder.slice(0, folder.lastIndexOf('/'));
    return (j.hits || []).filter((h) => typeof h['jcr:path'] === 'string' && h['jcr:path'].startsWith(folder + '/')).map((h) => {
        const m = (h['jcr:content'] && h['jcr:content'].metadata) || {};
        return {
            path: h['jcr:path'],
            rel: h['jcr:path'].slice(parent.length + 1),
            sha1: m['dam:sha1'] ? String(m['dam:sha1']).toLowerCase() : null,
            guid: (h['jcr:content'] && h['jcr:content'].fmUuid) || null,
            size: Number(m['dam:size']) || 0,
        };
    });
}

/** Downloads one original rendition to dest; resolves to { bytes, sha1 }. Throws with .retry for retryable failures. */
async function downloadOne({ baseUrl, username, password, asset, dest, signal }) {
    const r = await fetch(`${baseUrl}${encodePath(asset.path)}/jcr:content/renditions/original`, {
        headers: authHeader(username, password), signal: timeoutSignal(signal, FILE_TIMEOUT_MS),
    });
    if (!r.ok) {
        const e = new Error(`HTTP ${r.status}`);
        e.retry = r.status === 429 || r.status >= 500;
        throw e;
    }
    const expected = r.headers.get('content-length');
    const hash = crypto.createHash('sha1');
    let bytes = 0;
    const body = Readable.fromWeb(r.body);
    body.on('data', (c) => { hash.update(c); bytes += c.length; });
    await pipeline(body, fs.createWriteStream(dest));
    if (expected != null && Number(expected) !== bytes) {
        throw Object.assign(new Error(`incomplete download (${bytes} of ${expected} bytes)`), { retry: true });
    }
    return { bytes, sha1: hash.digest('hex') };
}

/** Path of the asset with this Guides GUID anywhere in the source DAM, or null. */
async function findGuid({ baseUrl, username, password, guid, signal }) {
    const q = new URLSearchParams({
        path: '/content/dam', type: 'dam:Asset', property: 'jcr:content/fmUuid', 'property.value': guid,
        'p.limit': '1', 'p.hits': 'selective', 'p.properties': 'jcr:path',
    });
    const r = await fetch(`${baseUrl}/bin/querybuilder.json?${q}`, { headers: authHeader(username, password), signal: timeoutSignal(signal, 60000) });
    if (!r.ok) return null;
    const hit = ((await r.json()).hits || [])[0];
    return hit && hit['jcr:path'] ? hit['jcr:path'] : null;
}

/**
 * DITA files from a UUID-based Guides server point at each other by GUID: rewrite those links to relative
 * paths (see guides-refs.js) and note the DOCTYPE and any link that leaves the copied folder.
 */
async function fixDitaFile({ file, asset, folder, guidPaths, lookupOutside, report }) {
    const buf = await fsp.readFile(file);
    if (buf[0] === 0xff || buf[0] === 0xfe) { report.unreadable.push(asset.path); return; }
    const text = buf.toString('utf8');
    const dt = refs.doctypeOf(text);
    if (dt) report.doctypes[dt.publicId] = (report.doctypes[dt.publicId] || 0) + 1;
    const guids = new Set((text.match(new RegExp(refs.GUID.source, 'g')) || []));
    for (const g of guids) if (!guidPaths.has(g)) await lookupOutside(g);
    const out = refs.rewriteGuidRefs(text, asset.path, (g) => guidPaths.get(g) || null);
    out.unresolved.forEach((g) => report.unresolvedGuids.add(g));
    out.targets.forEach((t) => {
        if (!t.startsWith(folder + '/')) report.outside[t] = (report.outside[t] || 0) + 1;
    });
    if (out.changed) {
        await fsp.writeFile(file, out.text);
        report.rewrittenFiles++;
        report.rewrittenLinks += out.changed;
    }
}

/**
 * Copies every asset under `folder` into stagingDir (flat names c0, c1, ...). For each finished file calls
 * onFile({ path, originalname, relativePath, bytes }) so the caller can register it like a browser upload.
 * Resolves to { files, bytes, verified, unverified, failed: [{ path, error }] }; rejects only when the listing
 * fails or the copy is cancelled (signal).
 */
async function copyFolder({ baseUrl, username, password, folder, stagingDir, concurrency, maxConcurrency, signal, onListed, onFile, onProgress, onConcurrency }) {
    const assets = await listAssets({ baseUrl, username, password, folder, signal });
    if (onListed) await onListed({ files: assets.length, bytes: assets.reduce((s, a) => s + a.size, 0) });
    await fsp.mkdir(stagingDir, { recursive: true });
    const width = Math.max(1, Math.min(MAX_CONCURRENCY, parseInt(concurrency, 10) || DEFAULT_CONCURRENCY));
    // Never above what was chosen (maxConcurrency, default the start): on a customer server the choice is a ceiling
    const ceiling = Math.max(width, Math.min(MAX_CONCURRENCY, parseInt(maxConcurrency, 10) || width));
    const speed = adaptive.createController({ start: width, max: ceiling });
    let want = Math.min(width, ceiling);
    let running = 0;
    let retries = 0;
    const summary = { files: 0, bytes: 0, verified: 0, unverified: 0, failed: [] };
    const report = { doctypes: {}, outside: {}, unresolvedGuids: new Set(), unreadable: [], rewrittenFiles: 0, rewrittenLinks: 0 };
    const guidPaths = new Map(assets.filter((a) => a.guid).map((a) => [a.guid, a.path]));
    const lookups = new Map();
    const lookupOutside = (guid) => {
        if (!lookups.has(guid)) {
            lookups.set(guid, lookups.size >= MAX_OUTSIDE_LOOKUPS ? Promise.resolve(null)
                : findGuid({ baseUrl, username, password, guid, signal }).catch(() => null)
                    .then((p) => { if (p) guidPaths.set(guid, p); return p; }));
        }
        return lookups.get(guid);
    };
    let next = 0;
    const worker = async () => {
        running++;
        try {
            await work();
        } finally {
            running--;
        }
    };
    const work = async () => {
        while (next < assets.length && running <= want) {
            if (signal && signal.aborted) return;
            const i = next++;
            const asset = assets[i];
            const dest = path.join(stagingDir, 'c' + i);
            let lastError = null;
            for (let attempt = 1; attempt <= ATTEMPTS; attempt++) {
                try {
                    const got = await downloadOne({ baseUrl, username, password, asset, dest, signal });
                    // Guides rewrites DITA files after AEM computed dam:sha1, so a different SHA-1 is normal there;
                    // the download itself is still checked against the size AEM sent
                    const verified = !!asset.sha1 && got.sha1 === asset.sha1;
                    if (refs.isXmlFile(asset.path)) await fixDitaFile({ file: dest, asset, folder, guidPaths, lookupOutside, report });
                    summary.files++;
                    summary.bytes += got.bytes;
                    if (verified) summary.verified++; else summary.unverified++;
                    await onFile({ path: dest, originalname: path.posix.basename(asset.path), relativePath: asset.rel, bytes: got.bytes });
                    lastError = null;
                    break;
                } catch (e) {
                    if (signal && signal.aborted) return;
                    lastError = e;
                    await fsp.rm(dest, { force: true }).catch(() => {});
                    if (!e.retry && !/fetch failed|ECONNRESET|ETIMEDOUT|terminated|aborted due to timeout/i.test(e.message)) break;
                    retries++;
                    if (attempt < ATTEMPTS) await new Promise((res) => setTimeout(res, 1000 * 2 ** (attempt - 1)));
                }
            }
            if (lastError) summary.failed.push({ path: asset.path, error: lastError.message });
            if (onProgress) onProgress({ done: summary.files, failed: summary.failed.length, total: assets.length, bytes: summary.bytes });
        }
    };
    // Workers leave when there are more than wanted; more are started when the controller allows
    const workers = new Set();
    const fill = () => {
        while (running < want && next < assets.length && !(signal && signal.aborted)) {
            const w = worker();
            workers.add(w);
            w.finally(() => workers.delete(w));
        }
    };
    let lastDone = 0;
    let lastRetries = 0;
    let lastAt = Date.now();
    const timer = setInterval(() => {
        const now = Date.now();
        const d = speed.next({ rate: (summary.files - lastDone) / ((now - lastAt) / 1000), errors: retries - lastRetries, canGrow: true });
        lastDone = summary.files; lastRetries = retries; lastAt = now;
        if (d.changed) {
            want = d.target;
            if (onConcurrency) onConcurrency({ concurrency: want, reason: d.reason });
            fill();
        }
    }, ADAPT_WINDOW_MS);
    if (timer.unref) timer.unref();
    try {
        fill();
        while (workers.size) await Promise.all([...workers]);
    } finally {
        clearInterval(timer);
    }
    if (signal && signal.aborted) throw Object.assign(new Error('Copy cancelled'), { cancelled: true });
    summary.dita = {
        doctypes: report.doctypes,
        outside: Object.entries(report.outside).sort((a, b) => b[1] - a[1]).map(([path, links]) => ({ path, links })),
        unresolvedGuids: [...report.unresolvedGuids].slice(0, 100), unresolvedCount: report.unresolvedGuids.size,
        rewrittenFiles: report.rewrittenFiles, rewrittenLinks: report.rewrittenLinks, unreadable: report.unreadable.slice(0, 50),
        withGuid: guidPaths.size,
    };
    return summary;
}

module.exports = { normalizeSourceFolder, listAssets, copyFolder, DEFAULT_CONCURRENCY, MAX_CONCURRENCY };
