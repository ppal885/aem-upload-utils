// Upload and copy edge cases against the running preview (3930) and mock AEM (4620): a retried chunk is not
// counted twice, the file limit counts files (not folders), kept names with # still upload, and a copied DITA file
// that is not UTF-8 is copied byte for byte instead of being rewritten.
const crypto = require('crypto');
const BASE = 'http://127.0.0.1:3930';
const AEM = 'http://127.0.0.1:4620';
const results = [];
const record = (name, ok, detail) => { results.push(ok); console.log(`[${ok ? 'PASS' : 'FAIL'}] ${name}: ${detail}`); };
const H = { 'Content-Type': 'application/json', Origin: BASE };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
const target = { baseUrl: AEM, username: 'admin', password: 'admin' };
const RUN = Date.now().toString(36);

let cookie = '';
async function call(path, method = 'GET', body, form) {
    const r = await fetch(BASE + path, { method, headers: form ? { Origin: BASE, Cookie: cookie } : { ...H, Cookie: cookie }, body: form || (body ? JSON.stringify(body) : undefined) });
    return { status: r.status, body: await r.json().catch(() => ({})) };
}
async function waitFor(fn, done, ms = 300, tries = 200) {
    let v;
    for (let i = 0; i < tries; i++) { v = await fn(); if (done(v)) return v; await sleep(ms); }
    return v;
}
function chunk(files, index) {
    const fd = new FormData();
    Object.entries(files).forEach(([rel, data]) => fd.append('files', new Blob([data]), rel.split('/').pop()));
    fd.append('_relativePaths', JSON.stringify(Object.keys(files)));
    if (index !== undefined) { fd.append('chunkIndex', String(index)); fd.append('totalChunks', '2'); }
    return fd;
}
/** init -> chunks -> finalize -> wait; returns the final upload status. */
async function upload(damPath, chunks, extra = {}) {
    const total = chunks.reduce((n, c) => n + Object.values(c.files).reduce((m, d) => m + Buffer.byteLength(d), 0), 0);
    const init = await call('/api/upload/browser/init', 'POST', { ...target, damPath, totalBytes: total, fileCount: 10, ...extra });
    const answers = [];
    for (const c of chunks) answers.push(await call(`/api/upload/browser/chunk/${init.body.sessionId}`, 'POST', null, chunk(c.files, c.index)));
    const fin = await call('/api/upload/browser/finalize', 'POST', { sessionId: init.body.sessionId });
    if (!fin.body.uploadId) return { answers, fin };
    const st = await waitFor(() => call(`/api/upload/status/${fin.body.uploadId}`), (r) => r.body.done);
    return { answers, fin, st: st.body };
}

(async () => {
    const login = await fetch(BASE + '/api/auth/login', { method: 'POST', headers: H, body: JSON.stringify({ email: 'admin@test.local', password: 'admin-pass-123' }) });
    cookie = (login.headers.get('set-cookie') || '').split(';')[0];

    // A chunk sent again (its first answer was lost) is stored once
    const c0 = { 'r/a.dita': '<topic id="a"/>', 'r/b.dita': '<topic id="b"/>' };
    const dup = await upload(`content/dam/gaps-${RUN}`, [{ files: c0, index: 0 }, { files: c0, index: 0 }, { files: { 'r/c.dita': '<topic id="c"/>' }, index: 1 }]);
    record('a retried chunk is not counted twice', dup.answers[1].body.duplicate === true && dup.answers[2].body.received === 3 && dup.st && dup.st.completedFiles === 3,
        `answers=${dup.answers.map((a) => a.body.received).join(',')} completed=${dup.st && dup.st.completedFiles}`);

    // 3 files in 4 folders with a 3-file limit: folders do not count
    const lim = await upload(`content/dam/gaps-lim-${RUN}`, [{ files: { 'top/a/x/1.dita': '<t/>', 'top/b/2.dita': '<t/>', 'top/c/3.dita': '<t/>' } }], { maxUploadFiles: 3 });
    record('the file limit counts files, not folders', lim.st && lim.st.completedFiles === 3 && !lim.st.failedFiles, JSON.stringify(lim.st || lim.fin.body).slice(0, 200));

    // Keep names exactly with # in a file and a folder name: uploads, and lands where the preview says
    const keep = await upload(`content/dam/gaps-keep-${RUN}`, [{ files: { 'top/f #2/d.dita': '<t id="d"/>', 'top/c #1.dita': '<t id="c"/>' } }], { keepNodeNames: true });
    const landed = await (await fetch(AEM + '/__completed')).json();
    const want = [`/content/dam/gaps-keep-${RUN}/top/f -2/d.dita`, `/content/dam/gaps-keep-${RUN}/top/c -1.dita`];
    record('kept names with # upload (as -) instead of failing or landing in a wrong folder', keep.st && keep.st.completedFiles === 2 && !keep.st.failedFiles && want.every((p) => landed.includes(p)),
        `completed=${keep.st && keep.st.completedFiles} failed=${keep.st && keep.st.failedFiles} landed=${landed.filter((p) => p.includes('gaps-keep')).join(' ')}`);

    // Copy: a windows-1252 map is copied unchanged, a UTF-8 map has its GUID link turned into a path
    const src = `content/dam/gaps-src-${RUN}`;
    await upload(src, [{ files: { 'enc/t.dita': '<topic id="t"><title>T</title></topic>' } }]);
    const q = await (await fetch(`${AEM}/bin/querybuilder.json?path=/${src}&type=dam:Asset&p.limit=-1`, { headers: { Authorization: 'Basic ' + Buffer.from('admin:admin').toString('base64') } })).json();
    const guid = (((q.hits || []).find((h) => h['jcr:path'].endsWith('/t.dita')) || {})['jcr:content'] || {}).fmUuid;
    const latin = Buffer.concat([Buffer.from('<?xml version="1.0" encoding="windows-1252"?>\n<map><title>Caf'), Buffer.from([0xe9]), Buffer.from(`</title><topicref href="${guid}"/></map>\n`)]);
    const utf8 = `<?xml version="1.0" encoding="UTF-8"?>\n<map><title>Café</title><topicref href="${guid}"/></map>\n`;
    const form = new FormData();
    form.append('files', new Blob([latin]), 'latin.ditamap'); form.append('files', new Blob([utf8]), 'utf8.ditamap');
    form.append('_relativePaths', JSON.stringify(['enc/latin.ditamap', 'enc/utf8.ditamap']));
    const init = await call('/api/upload/browser/init', 'POST', { ...target, damPath: src, totalBytes: 500, fileCount: 2 });
    await call(`/api/upload/browser/chunk/${init.body.sessionId}`, 'POST', null, form);
    const fin = await call('/api/upload/browser/finalize', 'POST', { sessionId: init.body.sessionId });
    await waitFor(() => call(`/api/upload/status/${fin.body.uploadId}`), (r) => r.body.done);

    const dst = `content/dam/gaps-dst-${RUN}`;
    const cinit = await call('/api/upload/browser/init', 'POST', { ...target, damPath: dst });
    await call(`/api/copy/${cinit.body.sessionId}/start`, 'POST', { sourceUrl: AEM, folder: `/${src}/enc`, sourceUsername: 'admin', sourcePassword: 'admin' });
    const copy = (await waitFor(() => call(`/api/copy/${cinit.body.sessionId}`), (r) => r.status !== 200 || ['done', 'failed'].includes(r.body.copy.state))).body.copy || {};
    const cfin = await call('/api/upload/browser/finalize', 'POST', { sessionId: cinit.body.sessionId });
    await waitFor(() => call(`/api/upload/status/${cfin.body.uploadId}`), (r) => r.body.done);
    const got = async (name) => Buffer.from(await (await fetch(`${AEM}/__asset/${dst}/enc/${name}`)).arrayBuffer());
    const gotLatin = await got('latin.ditamap');
    const gotUtf8 = (await got('utf8.ditamap')).toString('utf8');
    record('a copied DITA file that is not UTF-8 arrives byte for byte and is reported',
        !!guid && sha(gotLatin) === sha(latin) && (copy.dita.unreadable || []).some((p) => p.endsWith('/latin.ditamap')),
        `guid=${guid} same=${sha(gotLatin) === sha(latin)} unreadable=${JSON.stringify(copy.dita && copy.dita.unreadable)}`);
    record('...while the UTF-8 one has its GUID link turned into a path, accents intact', gotUtf8.includes('href="t.dita"') && gotUtf8.includes('Café'), gotUtf8.replace(/\n/g, ' ').slice(0, 160));

    console.log(`\n${results.filter(Boolean).length}/${results.length} passed`);
    process.exit(results.every(Boolean) ? 0 : 1);
})().catch((e) => { console.error(e); process.exit(1); });
