// Upload order for DITA maps (lib/dita-order.js and the waves in upload-service.js). A parent map uploaded with
// its submaps got GUIDs that no file had: Guides processed it before the submaps. Maps must go after the files
// they link to, and only once Guides has processed those files.
//   node tools/test-dita-order.js
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');
const { uploadWaves } = require('../lib/dita-order');

const results = [];
// upload-service.js hides console.log lines that are not its own (library chatter), so results go to stdout directly
const say = (line) => process.stdout.write(line + '\n');
const record = (name, ok, detail) => { results.push(ok); say(`[${ok ? 'PASS' : 'FAIL'}] ${name}: ${detail}`); };

function tree(files) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dita-order-'));
    for (const [rel, text] of Object.entries(files)) {
        fs.mkdirSync(path.dirname(path.join(root, rel)), { recursive: true });
        fs.writeFileSync(path.join(root, rel), text);
    }
    return root;
}

const MAP = (...refs) => `<?xml version="1.0"?><!DOCTYPE map PUBLIC "-//OASIS//DTD DITA Map//EN" "map.dtd"><map>${refs.join('')}</map>`;
const BOOK = {
    'kubernetes-master.ditamap': MAP('<mapref href="maps/gateway.ditamap"/>', '<mapref href="maps/redis%20cache.ditamap#x"/>', '<topicref href="topics/t1.dita"/>',
        '<topicref href="maps/keys.ditamap" format="ditamap"/>', '<topicref href="https://example.com/x.ditamap" scope="external"/>'),
    'maps/gateway.ditamap': MAP('<topicref href="../topics/t1.dita"/>', '<mapref href="inner/deep.ditamap"/>'),
    'maps/redis cache.ditamap': MAP('<topicref href="../topics/t2.dita"/>'),
    'maps/keys.ditamap': MAP('<keydef keys="k" href="../topics/t2.dita"/>'),
    'maps/inner/deep.ditamap': MAP('<topicref href="../../topics/t2.dita"/>'),
    'maps/cycle-a.ditamap': MAP('<mapref href="cycle-b.ditamap"/>'),
    'maps/cycle-b.ditamap': MAP('<mapref href="cycle-a.ditamap"/>'),
    'topics/t1.dita': '<topic id="t1"><title>t1</title></topic>',
    'topics/t2.dita': '<topic id="t2"><title>t2</title></topic>',
    'images/a.png': 'png',
};

async function unit() {
    const root = tree(BOOK);
    const items = Object.keys(BOOK).map((rel) => ({ filePath: path.join(root, rel), rel }));
    const waves = (await uploadWaves(items)).map((w) => w.map((i) => i.rel).sort());
    const pos = (rel) => waves.findIndex((w) => w.includes(rel));
    record('files that are not maps go first, in one wave', JSON.stringify(waves[0]) === JSON.stringify(['images/a.png', 'topics/t1.dita', 'topics/t2.dita']), JSON.stringify(waves));
    record('a submap goes before the map that links to it (mapref, topicref format=ditamap, encoded name, #fragment)',
        pos('maps/inner/deep.ditamap') < pos('maps/gateway.ditamap') && pos('maps/gateway.ditamap') < pos('kubernetes-master.ditamap')
        && pos('maps/redis cache.ditamap') < pos('kubernetes-master.ditamap') && pos('maps/keys.ditamap') < pos('kubernetes-master.ditamap'), JSON.stringify(waves));
    record('the top map is alone in the last wave', JSON.stringify(waves[waves.length - 1]) === '["kubernetes-master.ditamap"]', JSON.stringify(waves));
    record('maps that link to each other are still uploaded, once each', pos('maps/cycle-a.ditamap') > 0 && pos('maps/cycle-b.ditamap') > 0
        && waves.flat().length === items.length && new Set(waves.flat()).size === items.length, JSON.stringify(waves));

    const plain = ['a.dita', 'b.xml', 'c.png'].map((rel) => ({ filePath: path.join(root, rel), rel }));
    const one = await uploadWaves(plain);
    record('an upload without maps is one wave in the order given', one.length === 1 && one[0].map((i) => i.rel).join() === 'a.dita,b.xml,c.png', JSON.stringify(one));
    record('nothing to upload is no wave', (await uploadWaves([])).length === 0, '[]');
    const unreadable = await uploadWaves([{ filePath: path.join(root, 'gone.ditamap'), rel: 'gone.ditamap' }, { filePath: path.join(root, 'topics/t1.dita'), rel: 't1' }]);
    record('a map that cannot be read is uploaded after the other files', unreadable.length === 2 && unreadable[1][0].rel === 'gone.ditamap', JSON.stringify(unreadable));
    fs.rmSync(root, { recursive: true, force: true });
}

async function withMockAem(port, env, fn) {
    const proc = spawn(process.execPath, [path.join(__dirname, '..', 'test', 'mock-aem.js'), String(port)], { env: { ...process.env, ...env }, stdio: 'ignore' });
    try {
        for (let i = 0; ; i++) {
            try { await fetch(`http://127.0.0.1:${port}/__completed`); break; } catch (e) { if (i > 50) throw e; await new Promise((r) => setTimeout(r, 100)); }
        }
        return await fn(`http://127.0.0.1:${port}`);
    } finally {
        proc.kill('SIGKILL');
    }
}

async function upload(aem, localDir, folder) {
    // runUpload reads its timings from the environment when upload-service.js loads
    const { runUpload } = require('../upload-service');
    const { promise } = runUpload({ baseUrl: aem, username: 'admin', password: 'admin', damPath: folder, localPaths: [localDir], maxConcurrent: 4, uploadPipelines: 4 }, () => {});
    return promise;
}

async function endToEnd() {
    process.env.GUIDES_WAIT_POLL_MS = '200';
    process.env.GUIDES_WAIT_GRACE_MS = '1500';
    process.env.LOG_LEVEL = 'error';
    const root = tree(BOOK);
    const local = path.join(root);
    const DELAY = 700;
    await withMockAem(4691, { MOCK_GUIDES_DELAY_MS: String(DELAY) }, async (aem) => {
        const started = Date.now();
        const r = await upload(aem, local, 'content/dam/book');
        const t = await (await fetch(`${aem}/__timeline`)).json();
        const base = '/content/dam/book/' + path.basename(local);
        const at = (rel) => t[`${base}/${rel}`] || {};
        record('the upload with maps finishes without errors', r.success && r.totalFiles === Object.keys(BOOK).length, JSON.stringify({ success: r.success, files: r.totalFiles, errors: r.errors }));
        const top = at('kubernetes-master.ditamap').completed;
        const kids = ['maps/gateway.ditamap', 'maps/redis cache.ditamap', 'maps/keys.ditamap', 'maps/inner/deep.ditamap', 'topics/t1.dita', 'topics/t2.dita'];
        const late = kids.filter((k) => !(at(k).processed && at(k).processed <= top));
        record('the top map reaches AEM only after Guides processed every file and submap it links to', top && late.length === 0, late.length ? `not processed before it: ${late.join(', ')}` : `${Date.now() - started} ms`);
        record('a submap reaches AEM only after Guides processed its own submap',
            at('maps/inner/deep.ditamap').processed <= at('maps/gateway.ditamap').completed, JSON.stringify([at('maps/inner/deep.ditamap'), at('maps/gateway.ditamap')]));
    });
    await withMockAem(4692, { MOCK_GUIDES: 'off' }, async (aem) => {
        const started = Date.now();
        const r = await upload(aem, local, 'content/dam/noguides');
        const secs = (Date.now() - started) / 1000;
        record('without Guides processing the upload waits once (grace time) and then finishes', r.success && r.totalFiles === Object.keys(BOOK).length && secs < 8, `${secs.toFixed(1)} s, success ${r.success}`);
    });
    fs.rmSync(root, { recursive: true, force: true });
}

(async () => {
    await unit();
    await endToEnd();
    const passed = results.filter(Boolean).length;
    say(`\n${passed}/${results.length} passed`);
    process.exit(passed === results.length ? 0 : 1);
})().catch((e) => { console.error(e); process.exit(1); });
