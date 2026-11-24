/**
 * DITA maps in a content package. A package install puts every file into AEM at once and Guides then processes
 * them in any order, so a parent map can be processed before its submaps and topics have their GUIDs and end up
 * pointing at GUIDs no file has (see lib/dita-order.js). After the tool installs such a package it links the maps
 * again in order: it waits until Guides has processed the package's DITA files, then sends the maps back to their
 * own paths, innermost submaps first, each wave after Guides has processed the one before. The maps carry the
 * relative links of the source files, so Guides converts them to the GUIDs the files really have.
 *
 * At build time `prepare` keeps a copy of each map next to the zip, with the upload order; `linkMaps` runs after
 * a successful install.
 */
const fsp = require('fs').promises;
const path = require('path');
const { uploadWaves } = require('./dita-order');
const { waitForGuides } = require('./guides-wait');
const guidesStatus = require('./guides-status');
const { uploadToPaths } = require('../upload-service');

const POLL_MS = parseInt(process.env.GUIDES_WAIT_POLL_MS, 10) || 3000;
// After a package install AEM Guides starts on its own; on AEM as a Cloud Service only after Reprocess Assets
const INSTALL_GRACE_MS = parseInt(process.env.PACKAGE_MAPS_GRACE_MS, 10) || 2 * 60 * 1000;
const CLOUD_GRACE_MS = parseInt(process.env.PACKAGE_MAPS_CLOUD_GRACE_MS, 10) || 10 * 60 * 1000;
const INSTALL_MAX_MS = parseInt(process.env.PACKAGE_MAPS_MAX_MS, 10) || 30 * 60 * 1000;
const WAVE_GRACE_MS = parseInt(process.env.GUIDES_WAIT_GRACE_MS, 10) || 60 * 1000;
const WAVE_MAX_MS = parseInt(process.env.GUIDES_WAIT_MAX_MS, 10) || 15 * 60 * 1000;
const MAPS_DIR = 'maps';
const DITA_LIST = 'dita-paths.json';

/**
 * The fewest folders that hold these AEM paths. A package's filter roots can be files (a package built without its
 * top folder has one root per top-level file and folder), and a QueryBuilder path search lists only what is below.
 */
function foldersOf(paths) {
    const dirs = [...new Set(paths.map((p) => path.posix.dirname(p)))].sort((a, b) => a.length - b.length);
    const out = [];
    for (const d of dirs) if (!out.some((k) => d === k || d.startsWith(k + '/'))) out.push(d);
    return out;
}

const duration = (secs) => (secs < 120 ? `${secs} s` : `${Math.round(secs / 60)} min`);
const isCloud = (baseUrl) => /\.adobeaemcloud\.com/i.test(String(baseUrl || ''));

/**
 * Keeps the maps of a just-built package in pkgDir/maps and returns the upload order:
 * [[{ path: AEM path, file: path relative to pkgDir }, ...], ...], or null when the package has no maps.
 * The AEM paths of all DITA files go to pkgDir/dita-paths.json (what linkMaps waits for).
 */
async function prepare(buildResult, pkgDir) {
    const maps = buildResult.maps || [];
    if (!maps.length) return null;
    const waves = await uploadWaves(maps, (m) => m.absPath);
    await fsp.mkdir(path.join(pkgDir, MAPS_DIR), { recursive: true });
    let n = 0;
    const out = [];
    for (const wave of waves) {
        const w = [];
        for (const m of wave) {
            const file = `${MAPS_DIR}/${n++}.xml`;
            // eslint-disable-next-line no-await-in-loop
            await fsp.copyFile(m.absPath, path.join(pkgDir, file));
            w.push({ path: m.jcrPath, file });
        }
        out.push(w);
    }
    await fsp.writeFile(path.join(pkgDir, DITA_LIST), JSON.stringify(buildResult.dita || []));
    return out;
}

/**
 * Links the maps of an installed package in order. server: { baseUrl, username, password }; onStatus({ state, message }) is called as it goes (state running, done, skipped or failed).
 * Resolves to the last status; never rejects.
 */
async function linkMaps({ waves, pkgDir, server, onStatus = () => {} }) {
    const count = waves.reduce((n, w) => n + w.length, 0);
    const status = (state, message) => { const s = { state, message, at: new Date().toISOString() }; onStatus(s); return s; };
    const cloud = isCloud(server.baseUrl);
    try {
        let dita = [];
        try { dita = JSON.parse(await fsp.readFile(path.join(pkgDir, DITA_LIST), 'utf8')); } catch (_) { dita = waves.flat().map((m) => m.path); }
        const folders = foldersOf([...dita, ...waves.flat().map((m) => m.path)]);
        if (cloud) {
            status('running', 'Starting Reprocess Assets: AEM as a Cloud Service does not process package content by itself');
            for (const root of folders) {
                try {
                    // eslint-disable-next-line no-await-in-loop
                    await guidesStatus.reprocessFolder(server, root);
                } catch (e) {
                    return status('failed', `Could not start Reprocess Assets on ${root} (${e.message}). Run it in AEM, then install the package again so its ${count} map(s) are linked in order.`);
                }
            }
        }
        status('running', `Waiting for AEM Guides to process the ${dita.length} installed DITA file(s) before linking ${count} map(s) in order`);
        const first = await waitForGuides({ server, roots: folders, targets: dita, pollMs: POLL_MS, graceMs: cloud ? CLOUD_GRACE_MS : INSTALL_GRACE_MS, maxMs: INSTALL_MAX_MS });
        if (first.outcome === 'error') return status('failed', `Could not read the AEM Guides state (${first.message}). The ${count} map(s) were not linked again; check them with Check in AEM.`);
        if (first.outcome === 'no-guides') return status('skipped', `AEM Guides did not process the installed files within ${duration(first.secs)}, so the ${count} map(s) were left as installed.`);
        const late = first.outcome === 'timeout' ? first.pending.length : 0;

        const errors = [];
        for (let i = 0; i < waves.length; i++) {
            const wave = waves[i];
            status('running', `Linking maps in order: step ${i + 1} of ${waves.length} (${wave.length} map(s))`);
            // eslint-disable-next-line no-await-in-loop
            const up = await uploadToPaths(server, wave.map((m) => ({ localPath: path.join(pkgDir, m.file), aemPath: m.path })));
            errors.push(...up.errors);
            if (i === waves.length - 1) break;
            // eslint-disable-next-line no-await-in-loop
            const w = await waitForGuides({ server, roots: folders, targets: up.uploaded, pollMs: POLL_MS, graceMs: WAVE_GRACE_MS, maxMs: WAVE_MAX_MS });
            if (w.outcome === 'error') errors.push(`Could not read the AEM Guides state (${w.message})`);
        }
        if (errors.length) {
            return status('failed', `Linked the maps in order, but ${errors.length} could not be sent: ${errors.slice(0, 3).join('; ')}${errors.length > 3 ? '; …' : ''}`);
        }
        return status('done', `Linked ${count} map(s) in order after AEM Guides processed the files they link to`
            + (late ? ` (${late} file(s) were still processing after ${Math.round(INSTALL_MAX_MS / 60000)} min; check them with Check in AEM)` : '') + '.');
    } catch (e) {
        return status('failed', `Linking the maps stopped: ${e.message}`);
    }
}

module.exports = { prepare, linkMaps, isCloud, foldersOf };
