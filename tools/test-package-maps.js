// DITA maps of an installed package are linked again in order (lib/package-maps.js). A package install puts all
// files into AEM at once, so Guides can process a parent map before its submaps have GUIDs; after the install the
// tool sends the maps back to their own paths, innermost first, each after Guides processed what it links to.
//   node tools/test-package-maps.js
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

process.env.GUIDES_WAIT_POLL_MS = '200';
process.env.GUIDES_WAIT_GRACE_MS = '1500';
process.env.PACKAGE_MAPS_GRACE_MS = '1500';
process.env.LOG_LEVEL = 'error';
const { buildPackage } = require('../package-builder');
const packageMaps = require('../lib/package-maps');

const results = [];
// upload-service.js hides console.log lines that are not its own (library chatter), so results go to stdout directly
const say = (line) => process.stdout.write(line + '\n');
const record = (name, ok, detail) => { results.push(ok); say(`[${ok ? 'PASS' : 'FAIL'}] ${name}: ${detail}`); };

const MAP = (...refs) => `<?xml version="1.0"?><!DOCTYPE map PUBLIC "-//OASIS//DTD DITA Map//EN" "map.dtd"><map>${refs.join('')}</map>`;
const BOOK = {
    'kubernetes-master.ditamap': MAP('<mapref href="maps/gateway.ditamap"/>', '<mapref href="maps/redis-cache.ditamap"/>'),
    'maps/gateway.ditamap': MAP('<topicref href="../topics/t1.dita"/>', '<mapref href="inner/deep.ditamap"/>'),
    'maps/redis-cache.ditamap': MAP('<topicref href="../topics/t2.dita"/>'),
    'maps/inner/deep.ditamap': MAP('<topicref href="../../topics/t2.dita"/>'),
    'topics/t1.dita': '<topic id="t1"><title>t1</title></topic>',
    'topics/t2.dita': '<topic id="t2"><title>t2</title></topic>',
    'images/a.png': 'png',
};
const ROOT = '/content/dam/k8s';

function tree(files) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pkg-maps-'));
    for (const [rel, text] of Object.entries(files)) {
        fs.mkdirSync(path.dirname(path.join(root, rel)), { recursive: true });
        fs.writeFileSync(path.join(root, rel), text);
    }
    return root;
}

async function withMockAem(port, env, seed, fn) {
    const seedFile = path.join(os.tmpdir(), `pkg-maps-seed-${port}.json`);
    fs.writeFileSync(seedFile, JSON.stringify(seed));
    const proc = spawn(process.execPath, [path.join(__dirname, '..', 'test', 'mock-aem.js'), String(port), seedFile], { env: { ...process.env, ...env }, stdio: 'ignore' });
    try {
        for (let i = 0; ; i++) {
            try { await fetch(`http://127.0.0.1:${port}/__completed`); break; } catch (e) { if (i > 50) throw e; await new Promise((r) => setTimeout(r, 100)); }
        }
        return await fn(`http://127.0.0.1:${port}`);
    } finally {
        proc.kill('SIGKILL');
        fs.rmSync(seedFile, { force: true });
    }
}

/** What a package install leaves in AEM: every file there, Guides still processing the DITA ones. */
function installedSeed(res, processMs) {
    const assets = {};
    for (const p of res.dita) assets[p] = { text: '<x/>', guid: 'GUID-' + require('crypto').randomUUID(), ...(processMs ? { processMs } : {}) };
    return { assets };
}

(async () => {
    const src = tree(BOOK);
    const work = fs.mkdtempSync(path.join(os.tmpdir(), 'pkg-maps-out-'));
    const res = await buildPackage({ sourceDir: src, damRoot: ROOT, packageName: 'k8s', version: '1.0', group: 'test', skipTopFolder: true, outFile: path.join(work, 'k8s.zip') });
    const mapPaths = res.maps.map((m) => m.jcrPath).sort();
    record('the build lists the maps with their AEM paths', JSON.stringify(mapPaths) === JSON.stringify([
        `${ROOT}/kubernetes-master.ditamap`, `${ROOT}/maps/gateway.ditamap`, `${ROOT}/maps/inner/deep.ditamap`, `${ROOT}/maps/redis-cache.ditamap`]), JSON.stringify(mapPaths));
    record('the build lists every DITA file and no image', res.dita.length === 6 && !res.dita.some((p) => p.endsWith('.png')), JSON.stringify(res.dita));

    const waves = await packageMaps.prepare(res, work);
    const order = waves.map((w) => w.map((m) => m.path.slice(ROOT.length + 1)).sort());
    record('maps are kept in order: innermost first, the top map last and alone',
        JSON.stringify(order) === JSON.stringify([['maps/inner/deep.ditamap', 'maps/redis-cache.ditamap'], ['maps/gateway.ditamap'], ['kubernetes-master.ditamap']]), JSON.stringify(order));
    record('each kept map is the source file', waves.flat().every((m) => fs.readFileSync(path.join(work, m.file), 'utf8') === BOOK[m.path.slice(ROOT.length + 1)]), waves.flat().map((m) => m.file).join(','));
    record('a package without maps keeps nothing', (await packageMaps.prepare({ maps: [], dita: [] }, work)) === null, 'null');

    // Installed content that Guides finishes after 800 ms; re-sent maps take 500 ms each
    const seed = installedSeed(res, 800);
    await withMockAem(4695, { MOCK_GUIDES_DELAY_MS: '500' }, seed, async (aem) => {
        const statuses = [];
        const s = await packageMaps.linkMaps({ waves, pkgDir: work, server: { baseUrl: aem, username: 'admin', password: 'admin' }, onStatus: (x) => statuses.push(x.state) });
        record('linking finishes as done', s.state === 'done' && statuses[0] === 'running', `${s.state}: ${s.message}`);
        const t = await (await fetch(`${aem}/__timeline`)).json();
        const at = (rel) => t[`${ROOT}/${rel}`] || {};
        const topicsDone = Math.max(at('topics/t1.dita').seededProcessed, at('topics/t2.dita').seededProcessed);
        const first = Math.min(...waves.flat().map((m) => at(m.path.slice(ROOT.length + 1)).completed));
        record('no map is sent before Guides processed the installed files', first >= topicsDone, `first map ${first - topicsDone} ms after the topics`);
        record('a map is sent only after Guides processed the maps it links to',
            at('maps/inner/deep.ditamap').processed <= at('maps/gateway.ditamap').completed
            && at('maps/gateway.ditamap').processed <= at('kubernetes-master.ditamap').completed
            && at('maps/redis-cache.ditamap').processed <= at('kubernetes-master.ditamap').completed, JSON.stringify(t[`${ROOT}/kubernetes-master.ditamap`]));
        const completed = await (await fetch(`${aem}/__completed`)).json();
        record('only the maps are sent again, once each', completed.length === 4 && new Set(completed).size === 4 && completed.every((p) => /\.ditamap$/.test(p)), completed.join(', '));
        const after = await (await fetch(`${aem}/bin/querybuilder.json?path=${ROOT}`, { headers: { Authorization: 'Basic ' + Buffer.from('admin:admin').toString('base64') } })).json();
        const guidOf = (p) => (after.hits.find((h) => h['jcr:path'] === p) || {})['jcr:content'].fmUuid;
        record('the maps keep their GUIDs (updated in place, not replaced)', mapPaths.every((p) => guidOf(p) === seed.assets[p].guid), mapPaths.map(guidOf).join(','));
    });

    // AEM without Guides processing: nothing is sent, the status says why
    await withMockAem(4696, { MOCK_GUIDES: 'off' }, { assets: Object.fromEntries(res.dita.map((p) => [p, { text: '<x/>' }])) }, async (aem) => {
        const started = Date.now();
        const s = await packageMaps.linkMaps({ waves, pkgDir: work, server: { baseUrl: aem, username: 'admin', password: 'admin' } });
        const completed = await (await fetch(`${aem}/__completed`)).json();
        record('without Guides the maps are left as installed, after the grace time', s.state === 'skipped' && completed.length === 0 && Date.now() - started < 6000, `${s.state}: ${s.message}`);
    });

    // AEM cannot be asked: failed, with the reason
    const s = await packageMaps.linkMaps({ waves, pkgDir: work, server: { baseUrl: 'http://127.0.0.1:9', username: 'admin', password: 'admin' } });
    record('when AEM cannot be reached the status says so', s.state === 'failed' && /Guides state/.test(s.message), s.message);

    fs.rmSync(src, { recursive: true, force: true });
    fs.rmSync(work, { recursive: true, force: true });
    const passed = results.filter(Boolean).length;
    say(`\n${passed}/${results.length} passed`);
    process.exit(passed === results.length ? 0 : 1);
})().catch((e) => { console.error(e); process.exit(1); });
