// Adaptive copy speed against simulated source servers (lib/copy-from-aem.js). Short windows via COPY_ADAPT_WINDOW_MS.
process.env.COPY_ADAPT_WINDOW_MS = '300';
const http = require('http');
const os = require('os');
const path = require('path');
const fs = require('fs');
const { copyFolder } = require('../lib/copy-from-aem');

const results = [];
const record = (name, ok, detail) => { results.push(ok); console.log(`[${ok ? 'PASS' : 'FAIL'}] ${name}: ${detail}`); };
const FILES = 600;

/** A source with FILES assets; each download takes delayMs; above `limit` downloads at once it answers 429. */
function source({ delayMs, limit = Infinity }) {
    let inflight = 0;
    let most = 0;
    let refused = 0;
    const server = http.createServer((req, res) => {
        if (req.url.startsWith('/bin/querybuilder.json')) {
            const hits = Array.from({ length: FILES }, (_, i) => ({ 'jcr:path': `/content/dam/src/f${i}.png`, 'jcr:content': { metadata: { 'dam:size': 4 } } }));
            res.setHeader('Content-Type', 'application/json');
            return res.end(JSON.stringify({ hits }));
        }
        inflight++;
        most = Math.max(most, inflight);
        if (inflight > limit) {
            inflight--;
            refused++;
            res.statusCode = 429;
            return res.end();
        }
        setTimeout(() => { inflight--; res.setHeader('Content-Length', '4'); res.end('abcd'); }, delayMs);
    });
    return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve({
        server, url: `http://127.0.0.1:${server.address().port}`, stats: () => ({ most, refused }),
    })));
}

async function run(src, opts) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'copy-adaptive-'));
    const seen = [];
    try {
        const sum = await copyFolder({ baseUrl: src.url, username: 'a', password: 'b', folder: '/content/dam/src', stagingDir: dir,
            ...opts, onFile: async () => {}, onConcurrency: (c) => seen.push(c.concurrency) });
        return { sum, seen };
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
}

(async () => {
    // A source that keeps up: Auto grows from 8 to 16 and never above
    let src = await source({ delayMs: 40 });
    let r = await run(src, { concurrency: 8, maxConcurrency: 16 });
    record('Auto on a source that keeps up: 8 -> 16, never above 16, every file copied',
        r.seen.includes(16) && Math.max(...r.seen) === 16 && src.stats().most <= 16 && r.sum.files === FILES && r.sum.failed.length === 0,
        `steps=${r.seen.join(',')} most at once=${src.stats().most} files=${r.sum.files}`);
    src.server.close();

    // A fixed choice is a ceiling: Normal never passes 8
    src = await source({ delayMs: 40 });
    r = await run(src, { concurrency: 8, maxConcurrency: 8 });
    record('Normal: never more than 8 at once', src.stats().most <= 8 && r.sum.files === FILES, `most at once=${src.stats().most} steps=${r.seen.join(',') || 'none'}`);
    src.server.close();

    // A source that refuses above 10 at once: Auto backs off after the 429s, and no file is lost (retried)
    src = await source({ delayMs: 40, limit: 10 });
    r = await run(src, { concurrency: 8, maxConcurrency: 16 });
    const st = src.stats();
    const backedOff = r.seen.some((c, i) => i > 0 && c < r.seen[i - 1]);
    record('429s from the source: Auto slows down and every file still arrives', st.refused > 0 && backedOff && r.sum.files === FILES && r.sum.failed.length === 0,
        `refused=${st.refused} steps=${r.seen.join(',')} files=${r.sum.files} failed=${r.sum.failed.length}`);
    src.server.close();

    console.log(`\n${results.filter(Boolean).length}/${results.length} passed`);
    process.exit(results.every(Boolean) ? 0 : 1);
})();
