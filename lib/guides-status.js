/**
 * "Is it ready in AEM?" for a DAM folder after an upload or a package install: how many files AEM and AEM Guides
 * have processed, whether the links in the processed maps point at files in the folder, and whether the editor can
 * load the DTD of a few topics. Read-only (GET requests), except reprocessFolder, which starts AEM's Reprocess Assets.
 */
const DITA = /\.(dita|ditamap|xml|bookmap)$/i;
const MAP = /\.(ditamap|bookmap)$/i;
const MAX_MAPS = 100;
const TIMEOUT_MS = 60 * 1000;

const auth = (s) => ({ Authorization: 'Basic ' + Buffer.from(`${s.username}:${s.password}`).toString('base64') });
const enc = (p) => p.split('/').map(encodeURIComponent).join('/');

async function get(server, path, asJson) {
    const r = await fetch(server.baseUrl + path, { headers: auth(server), signal: AbortSignal.timeout(TIMEOUT_MS) });
    if (r.status === 401) throw new Error('AEM did not accept the username or password.');
    if (!r.ok) throw new Error(`AEM answered HTTP ${r.status} for ${path.split('?')[0]}`);
    return asJson ? r.json() : r.text();
}

async function listAssets(server, folder) {
    const q = new URLSearchParams({
        path: folder, type: 'dam:Asset', 'p.limit': '-1', 'p.hits': 'selective',
        'p.properties': 'jcr:path jcr:content/fmUuid jcr:content/guides:assetState jcr:content/dam:assetState',
    });
    const j = await get(server, `/bin/querybuilder.json?${q}`, true);
    return (j.hits || []).filter((h) => typeof h['jcr:path'] === 'string');
}

async function pool(items, width, fn) {
    let next = 0;
    await Promise.all(Array.from({ length: Math.min(width, items.length) }, async () => {
        while (next < items.length) await fn(items[next++]);
    }));
}

/** Counts, link check and DTD check for folder ("/content/dam/..."). */
async function folderStatus(server, folder) {
    const hits = await listAssets(server, folder);
    const c = (h) => h['jcr:content'] || {};
    const dita = hits.filter((h) => DITA.test(h['jcr:path']));
    const guides = { done: 0, processing: 0, waiting: 0, failed: 0, failedSamples: [] };
    for (const h of dita) {
        const s = String(c(h)['guides:assetState'] || '').toUpperCase();
        if (s === 'SUCCESS') guides.done++;
        else if (s === 'PROCESSING') guides.processing++;
        else if (!s) guides.waiting++;
        else { guides.failed++; if (guides.failedSamples.length < 10) guides.failedSamples.push({ path: h['jcr:path'], state: s }); }
    }
    const dam = { processed: 0, processing: 0, none: 0 };
    for (const h of hits) {
        const s = String(c(h)['dam:assetState'] || '').toLowerCase();
        if (s === 'processed') dam.processed++; else if (!s) dam.none++; else dam.processing++;
    }

    // Links: GUID hrefs in processed maps must point at a file of this folder
    const guids = new Map(hits.filter((h) => c(h).fmUuid).map((h) => [c(h).fmUuid, h['jcr:path']]));
    const maps = dita.filter((h) => MAP.test(h['jcr:path']) && String(c(h)['guides:assetState']).toUpperCase() === 'SUCCESS');
    const links = { maps: Math.min(maps.length, MAX_MAPS), mapsTotal: maps.length, ok: 0, notInFolder: 0, samples: [] };
    await pool(maps.slice(0, MAX_MAPS), 4, async (m) => {
        let text;
        try { text = await get(server, `${enc(m['jcr:path'])}/jcr:content/renditions/original`); } catch (_) { return; }
        for (const [, href] of text.matchAll(/\s(?:href|conref)\s*=\s*"(GUID-[0-9a-fA-F-]{36})/g)) {
            const target = guids.get(href);
            if (target && target.startsWith(folder + '/')) links.ok++;
            else { links.notInFolder++; if (links.samples.length < 5) links.samples.push({ map: m['jcr:path'], href }); }
        }
    });

    // DTD: can the editor load the DTD of a few processed topics?
    const topics = dita.filter((h) => !MAP.test(h['jcr:path']) && String(c(h)['guides:assetState']).toUpperCase() === 'SUCCESS').slice(0, 3);
    const dtd = [];
    for (const t of topics) {
        try {
            const text = await get(server, `${enc(t['jcr:path'])}/jcr:content/renditions/original`);
            const m = /<!DOCTYPE\s+\S+\s+PUBLIC\s+"([^"]+)"\s+"([^"]+)"/.exec(text.slice(0, 4096));
            if (!m) continue;
            const r = await fetch(`${server.baseUrl}/bin/xsd2json?${new URLSearchParams({ publicId: m[1], systemId: m[2], file: t['jcr:path'] })}`,
                { headers: auth(server), signal: AbortSignal.timeout(TIMEOUT_MS) });
            // only the status matters; the answer is the whole schema as JSON (often MBs): do not keep the connection busy with it
            try { await r.body?.cancel(); } catch (_) { /* already closed */ }
            dtd.push({ path: t['jcr:path'], publicId: m[1], ok: r.ok });
        } catch (_) { /* not checked */ }
    }

    const finished = guides.processing === 0 && guides.waiting === 0;
    return {
        folder, checkedAt: new Date().toISOString(), total: hits.length, dita: dita.length, other: hits.length - dita.length,
        guides, dam, links, dtd, finished,
        // links leaving the folder can be fine (shared content elsewhere), so they are a warning, not "not ready"
        ready: finished && guides.failed === 0 && dtd.every((d) => d.ok) && hits.length > 0,
    };
}

/** Starts AEM's Reprocess Assets (Full Process + Run Post Process) on folder, as the Assets UI button does. */
async function reprocessFolder(server, folder) {
    const body = new URLSearchParams({
        'profile-select': 'full-process', runPostProcess: 'true', operation: 'PROCESS',
        description: `Process-${folder}-${Date.now()}`, folder,
    });
    const r = await fetch(`${server.baseUrl}/bin/asynccommand`, { method: 'POST', headers: auth(server), body, signal: AbortSignal.timeout(TIMEOUT_MS) });
    const text = await r.text();
    if (!r.ok) throw new Error(`AEM refused Reprocess Assets (HTTP ${r.status})`);
    const m = /id="Message">([^<]*)/.exec(text);
    return { message: m ? m[1].trim() : 'Reprocess started' };
}

module.exports = { folderStatus, reprocessFolder };
