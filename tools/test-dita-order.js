// Upload order for DITA links (lib/dita-order.js and the waves in upload-service.js). A parent map uploaded with
// its submaps got GUIDs that no file had: Guides processed it before the submaps. The same race hits a topic that
// xrefs or conrefs another topic, or shows an image. Every file must go after the files it links to, once Guides
// has processed them; files that link to each other are sent a second time at the end.
//   node tools/test-dita-order.js
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');
const { planUpload } = require('../lib/dita-order');

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
const TOPIC = (id, ...body) => `<topic id="${id}"><title>${id}</title><body>${body.join('')}</body></topic>`;
const BOOK = {
    'kubernetes-master.ditamap': MAP('<mapref href="maps/gateway.ditamap"/>', '<mapref href="maps/redis%20cache.ditamap#x"/>', '<topicref href="topics/t1.dita"/>',
        '<topicref href="maps/keys.ditamap" format="ditamap"/>', '<topicref href="https://example.com/x.ditamap" scope="external"/>'),
    'maps/gateway.ditamap': MAP('<topicref href="../topics/t1.dita"/>', '<mapref href="inner/deep.ditamap"/>'),
    'maps/redis cache.ditamap': MAP('<topicref href="../topics/t2.dita"/>'),
    'maps/keys.ditamap': MAP('<keydef keys="k" href="../topics/t2.dita"/>'),
    'maps/inner/deep.ditamap': MAP('<topicref href="../../topics/t2.dita"/>'),
    // t1 conrefs t2, t2 shows an image, t3 and t4 xref each other
    'topics/t1.dita': TOPIC('t1', '<p conref="t2.dita#t2/p1"/>'),
    'topics/t2.dita': TOPIC('t2', '<p id="p1">x</p><image href="../images/a%20b.png"/>'),
    'topics/t3.dita': TOPIC('t3', '<xref href="t4.dita"/>'),
    'topics/t4.dita': TOPIC('t4', '<xref href="t3.dita#t3"/>', '<xref href="#t4/local"/>'),
    'topics/plain.dita': TOPIC('plain'),
    'images/a b.png': 'png',
};

async function unit() {
    const root = tree(BOOK);
    const items = Object.keys(BOOK).map((rel) => ({ filePath: path.join(root, rel), rel }));
    const plan = await planUpload(items);
    const waves = plan.waves.map((w) => w.map((i) => i.rel).sort());
    const pos = (rel) => waves.findIndex((w) => w.includes(rel));
    record('files that link to nothing in the upload go first', ['images/a b.png', 'topics/plain.dita'].every((f) => pos(f) === 0) && pos('topics/t2.dita') > 0, JSON.stringify(waves));
    record('a topic goes after the topic it conrefs, which goes after its image', pos('images/a b.png') < pos('topics/t2.dita') && pos('topics/t2.dita') < pos('topics/t1.dita'), JSON.stringify(waves));
    record('a submap goes before the map that links to it (mapref, topicref format=ditamap, encoded name, #fragment)',
        pos('maps/inner/deep.ditamap') < pos('maps/gateway.ditamap') && pos('maps/gateway.ditamap') < pos('kubernetes-master.ditamap')
        && pos('maps/redis cache.ditamap') < pos('kubernetes-master.ditamap') && pos('maps/keys.ditamap') < pos('kubernetes-master.ditamap'), JSON.stringify(waves));
    record('the top map is alone in the last wave', JSON.stringify(waves[waves.length - 1]) === '["kubernetes-master.ditamap"]', JSON.stringify(waves));
    const again = plan.again.map((i) => i.rel).sort();
    record('topics that xref each other share a wave and are sent again; nothing else is', pos('topics/t3.dita') === pos('topics/t4.dita')
        && JSON.stringify(again) === '["topics/t3.dita","topics/t4.dita"]', JSON.stringify(again));
    record('every file is in exactly one wave', waves.flat().length === items.length && new Set(waves.flat()).size === items.length, `${waves.flat().length} of ${items.length}`);
    record('files with links are listed (the ones a package install links again)', plan.linking.size === 9 && ![...plan.linking].some((i) => /plain|png/.test(i.rel)), [...plan.linking].map((i) => i.rel).join(','));

    // A chain deeper than the wave limit: the rest shares the last wave and is sent again
    const chain = {};
    for (let i = 0; i < 6; i++) chain[`c/c${i}.dita`] = TOPIC(`c${i}`, i ? `<xref href="c${i - 1}.dita"/>` : '');
    const croot = tree(chain);
    const cplan = await planUpload(Object.keys(chain).map((rel) => ({ filePath: path.join(croot, rel), rel })), undefined, { maxWaves: 3 });
    record('a chain deeper than the limit uses the limit, and the files that could not be ordered are sent again', cplan.waves.length === 3
        && JSON.stringify(cplan.again.map((i) => i.rel).sort()) === '["c/c3.dita","c/c4.dita","c/c5.dita"]', JSON.stringify({ waves: cplan.waves.map((w) => w.map((i) => i.rel)), again: cplan.again.map((i) => i.rel) }));

    const plain = ['a.dita', 'b.xml', 'c.png'].map((rel) => ({ filePath: path.join(root, rel), rel }));
    const one = await planUpload(plain);
    record('an upload without links is one wave in the order given, nothing sent again', one.waves.length === 1 && one.waves[0].map((i) => i.rel).join() === 'a.dita,b.xml,c.png' && !one.again.length, JSON.stringify(one.waves));
    record('nothing to upload is no wave', (await planUpload([])).waves.length === 0, '[]');
    // 3,000 topics in one xref chain: no stack overflow, done quickly
    const long = {};
    for (let i = 0; i < 3000; i++) long[`l/t${i}.dita`] = TOPIC(`t${i}`, i ? `<xref href="t${i - 1}.dita"/>` : '');
    const lroot = tree(long);
    const t0 = Date.now();
    const lplan = await planUpload(Object.keys(long).map((rel) => ({ filePath: path.join(lroot, rel), rel })));
    record('a 3,000-topic chain is planned without running out of stack', lplan.waves.length === 10 && lplan.again.length === 2990, `${Date.now() - t0} ms, ${lplan.waves.length} waves, ${lplan.again.length} again`);
    [root, croot, lroot].forEach((d) => fs.rmSync(d, { recursive: true, force: true }));
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

async function upload(aem, localDir, folder, events) {
    // runUpload reads its timings from the environment when upload-service.js loads
    const { runUpload } = require('../upload-service');
    const { promise } = runUpload({ baseUrl: aem, username: 'admin', password: 'admin', damPath: folder, localPaths: [localDir], maxConcurrent: 4, uploadPipelines: 4 },
        (e) => { if (events) events[e] = (events[e] || 0) + 1; });
    return promise;
}

async function endToEnd() {
    process.env.GUIDES_WAIT_POLL_MS = '200';
    process.env.GUIDES_WAIT_GRACE_MS = '1500';
    process.env.LOG_LEVEL = 'error';
    const local = tree(BOOK);
    const n = Object.keys(BOOK).length;
    await withMockAem(4691, { MOCK_GUIDES_DELAY_MS: '600', MOCK_GUIDES: 'all' }, async (aem) => {
        const started = Date.now();
        const events = {};
        const r = await upload(aem, local, 'content/dam/book', events);
        const t = await (await fetch(`${aem}/__timeline`)).json();
        const completed = await (await fetch(`${aem}/__completed`)).json();
        const base = '/content/dam/book/' + path.basename(local);
        const at = (rel) => t[`${base}/${rel}`] || {};
        record('the upload finishes without errors, every file counted once', r.success && r.totalFiles === n && events.fileend === n, JSON.stringify({ success: r.success, files: r.totalFiles, fileend: events.fileend, errors: r.errors }));
        const before = (a, b) => at(a).processed && at(a).processed <= (at(b).firstCompleted || at(b).completed);
        const late = [['images/a b.png', 'topics/t2.dita'], ['topics/t2.dita', 'topics/t1.dita'], ['topics/t1.dita', 'maps/gateway.ditamap'],
            ['maps/inner/deep.ditamap', 'maps/gateway.ditamap'], ['maps/gateway.ditamap', 'kubernetes-master.ditamap'], ['maps/redis cache.ditamap', 'kubernetes-master.ditamap']]
            .filter(([a, b]) => !before(a, b));
        record('each file reaches AEM only after Guides processed the files it links to', late.length === 0, late.length ? `not processed before: ${JSON.stringify(late)}` : `${Date.now() - started} ms`);
        const sends = (rel) => completed.filter((p) => p === `${base}/${rel}`).length;
        record('topics that xref each other are sent twice, the second time after both were processed',
            sends('topics/t3.dita') === 2 && sends('topics/t4.dita') === 2 && sends('topics/t1.dita') === 1 && sends('kubernetes-master.ditamap') === 1
            && Math.min(at('topics/t3.dita').completed, at('topics/t4.dita').completed) >= Math.max(at('topics/t3.dita').firstProcessed, at('topics/t4.dita').firstProcessed),
            JSON.stringify({ t3: at('topics/t3.dita'), t4: at('topics/t4.dita') }));
    });
    // Guides that processes only DITA files (path-based): the image is not waited for beyond the grace time
    await withMockAem(4693, { MOCK_GUIDES_DELAY_MS: '300' }, async (aem) => {
        const started = Date.now();
        const r = await upload(aem, local, 'content/dam/pathbased');
        const secs = (Date.now() - started) / 1000;
        record('when Guides gives images no state, they are waited for only during the grace time', r.success && r.totalFiles === n && secs < 15, `${secs.toFixed(1)} s, success ${r.success}`);
    });
    await withMockAem(4692, { MOCK_GUIDES: 'off' }, async (aem) => {
        const started = Date.now();
        const r = await upload(aem, local, 'content/dam/noguides');
        const secs = (Date.now() - started) / 1000;
        const completed = await (await fetch(`${aem}/__completed`)).json();
        record('without Guides processing the upload waits once (grace time), sends nothing twice and finishes', r.success && r.totalFiles === n && completed.length === n && secs < 8, `${secs.toFixed(1)} s, ${completed.length} sent, success ${r.success}`);
    });
    fs.rmSync(local, { recursive: true, force: true });
}

(async () => {
    await unit();
    await endToEnd();
    const passed = results.filter(Boolean).length;
    say(`\n${passed}/${results.length} passed`);
    process.exit(passed === results.length ? 0 : 1);
})().catch((e) => { console.error(e); process.exit(1); });
