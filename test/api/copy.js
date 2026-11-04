// Copy from AEM checks against the running preview (3930) and mock AEM (4620, the only COPY_TARGETS server):
// allowed targets (https, or http on a private IP for on-prem), a source signed in with an access token,
// copy -> upload, copy -> package -> install / download, and a failed copy that still says why.
// Run with COPY_ALLOW_DOWNLOAD=true too: the download check follows the app's setting.
const crypto = require('crypto');
const BASE = 'http://127.0.0.1:3930';
const AEM = 'http://127.0.0.1:4620';
const TOKEN = 'eyJhbGciOiJub25lIn0.eyJzdWIiOiJhZG1pbiJ9.mock';
const results = [];
const record = (name, ok, detail) => { results.push(ok); console.log(`[${ok ? 'PASS' : 'FAIL'}] ${name}: ${detail}`); };
const H = { 'Content-Type': 'application/json', Origin: BASE };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');

let cookie = '';
async function call(path, method = 'GET', body, form) {
    const r = await fetch(BASE + path, { method, headers: form ? { Origin: BASE, Cookie: cookie } : { ...H, Cookie: cookie }, body: form || (body ? JSON.stringify(body) : undefined) });
    const ct = r.headers.get('content-type') || '';
    return { status: r.status, body: ct.includes('json') ? await r.json() : Buffer.from(await r.arrayBuffer()) };
}
async function waitFor(fn, done, ms = 300, tries = 200) {
    let v;
    for (let i = 0; i < tries; i++) { v = await fn(); if (done(v)) return v; await sleep(ms); }
    return v;
}

const SRC = '/content/dam/copysrc' + Date.now().toString(36);
const files = {
    'guide.ditamap': '<map><topicref href="intro.dita"/><topicref href="sub/detail.dita"/></map>\n',
    'intro.dita': '<topic id="intro"><title>Intro</title></topic>\n',
    'sub/detail.dita': '<topic id="detail"><title>Detail é</title></topic>\n',
};
const target = { baseUrl: AEM, username: 'admin', password: 'admin' };

async function uploadSource() {
    const name = SRC.split('/').pop();
    const init = await call('/api/upload/browser/init', 'POST', { ...target, damPath: 'content/dam', totalBytes: 500, fileCount: 3 });
    const fd = new FormData();
    const rel = Object.keys(files).map((k) => `${name}/${k}`);
    Object.keys(files).forEach((k) => fd.append('files', new Blob([files[k]]), k.split('/').pop()));
    fd.append('_relativePaths', JSON.stringify(rel)); fd.append('chunkIndex', '0'); fd.append('totalChunks', '1');
    await call(`/api/upload/browser/chunk/${init.body.sessionId}`, 'POST', null, fd);
    const fin = await call('/api/upload/browser/finalize', 'POST', { sessionId: init.body.sessionId });
    return waitFor(() => call(`/api/upload/status/${fin.body.uploadId}`), (r) => r.body.done);
}

async function startCopy(sessionId, source) {
    const start = await call(`/api/copy/${sessionId}/start`, 'POST', { sourceUrl: AEM, folder: SRC, concurrency: 'auto', ...source });
    if (start.status !== 202) return start;
    return waitFor(() => call(`/api/copy/${sessionId}`), (r) => r.status !== 200 || ['done', 'failed', 'cancelled'].includes(r.body.copy.state));
}

(async () => {
    const login = await fetch(BASE + '/api/auth/login', { method: 'POST', headers: H, body: JSON.stringify({ email: 'admin@test.local', password: 'admin-pass-123' }) });
    cookie = (login.headers.get('set-cookie') || '').split(';')[0];

    // Allowed targets: on-prem over plain http is fine on a private IP, not on a public name
    const before = (await call('/api/admin/copy-targets')).body.targets;
    const onPrem = await call('/api/admin/copy-targets', 'POST', { targets: [...before, 'http://10.42.35.172:4502', 'https://author-p1-e2.adobeaemcloud.com'] });
    record('an on-prem http://10.x target and an AEM Cloud https target can be added', onPrem.status === 200 && onPrem.body.targets.includes('http://10.42.35.172:4502')
        && onPrem.body.targets.includes('https://author-p1-e2.adobeaemcloud.com'), JSON.stringify(onPrem.body));
    const pub = await call('/api/admin/copy-targets', 'POST', { targets: [...before, 'http://aem.example.com:4502'] });
    record('plain http to a public name is refused', pub.status === 400 && /use https/.test(pub.body.error), `${pub.status} ${pub.body.error}`);
    await call('/api/admin/copy-targets', 'POST', { targets: before });

    const up = await uploadSource();
    record('source folder uploaded', up.body.completedFiles === 3 && !up.body.failedFiles, JSON.stringify(up.body).slice(0, 200));

    // Copy -> upload, the source signed in with an access token (AEM Cloud)
    const badTok = await call('/api/upload/browser/init', 'POST', { ...target, damPath: 'content/dam/copydst' });
    const refused = await call(`/api/copy/${badTok.body.sessionId}/start`, 'POST', { sourceUrl: AEM, folder: SRC, sourceUsername: 'token', sourcePassword: TOKEN.replace('mock', 'nope') });
    record('a wrong access token is refused', refused.status === 400 && /Source AEM/.test(refused.body.error || ''), `${refused.status} ${refused.body.error}`);
    await call(`/api/upload/browser/cancel/${badTok.body.sessionId}`, 'POST');

    const init = await call('/api/upload/browser/init', 'POST', { ...target, damPath: 'content/dam/copydst' });
    const copy = await startCopy(init.body.sessionId, { sourceUsername: 'token', sourcePassword: TOKEN });
    record('copy with an access token as the source password', copy.status === 200 && copy.body.copy.state === 'done' && copy.body.copy.done === 3,
        JSON.stringify(copy.body.copy || copy.body).slice(0, 300));
    const fin = await call('/api/upload/browser/finalize', 'POST', { sessionId: init.body.sessionId });
    const done = await waitFor(() => call(`/api/upload/status/${fin.body.uploadId}`), (r) => r.body.done);
    const landed = `/content/dam/copydst/${SRC.split('/').pop()}`;
    const same = await Promise.all(Object.keys(files).map(async (k) => {
        const r = await fetch(`${AEM}/__asset${landed}/${k}`);
        return r.ok && sha(Buffer.from(await r.arrayBuffer())) === sha(Buffer.from(files[k]));
    }));
    record('copy -> upload: every file lands under the target folder unchanged', done.body.completedFiles === 3 && same.every(Boolean), `completed=${done.body.completedFiles} same=${same}`);

    // Copy -> package -> install on the allowed server, and download as COPY_ALLOW_DOWNLOAD says
    const opts = (await call('/api/copy/options')).body;
    const pk = await call('/api/packages/init', 'POST', { damPath: 'content/dam/pkgdst', packageName: 'copy-test', version: '1.0', group: 'guides-upload-copy' });
    const pcopy = await startCopy(pk.body.sessionId, { sourceUsername: 'admin', sourcePassword: 'admin' });
    record('copy -> package: copy done', pcopy.body.copy && pcopy.body.copy.state === 'done' && pcopy.body.copy.done === 3, JSON.stringify(pcopy.body.copy || pcopy.body).slice(0, 200));
    const build = await call(`/api/packages/build/${pk.body.sessionId}`, 'POST');
    const pkg = (await waitFor(() => call(`/api/packages/${build.body.package.id}`), (r) => r.body.package.state !== 'building')).body.package;
    record('copy -> package: package built and marked as copied content', pkg.state === 'ready' && pkg.files === 3 && !!pkg.copiedFrom, JSON.stringify(pkg).slice(0, 200));
    const dl = await call(`/api/packages/${pkg.id}/download`);
    record(`copy -> package: download ${opts.packageDownload ? 'allowed (COPY_ALLOW_DOWNLOAD=true)' : 'refused (COPY_ALLOW_DOWNLOAD off)'}`,
        opts.packageDownload ? dl.status === 200 && dl.body.length === pkg.zipBytes && dl.body.slice(0, 2).toString() === 'PK' : dl.status === 403,
        `status=${dl.status} bytes=${dl.body.length} zipBytes=${pkg.zipBytes}`);
    const inst = await call(`/api/packages/${pkg.id}/install`, 'POST', target);
    const installed = inst.status === 202 ? await waitFor(() => call(`/api/packages/${pkg.id}`), (r) => (r.body.package.installs || []).length > 0) : inst;
    const last = installed.body.package && installed.body.package.installs[0];
    record('copy -> package: installs on the allowed server', inst.status < 300 && last && last.ok, `${inst.status} ${JSON.stringify(last || inst.body).slice(0, 200)}`);

    // A copy that fails still says why (it used to answer "No copy for this session")
    const fi = await call('/api/upload/browser/init', 'POST', { ...target, damPath: 'content/dam/copydst2' });
    const fstart = await call(`/api/copy/${fi.body.sessionId}/start`, 'POST', { sourceUrl: AEM, folder: '/content/dam/qbfail', sourceUsername: 'admin', sourcePassword: 'admin' });
    const failed = await waitFor(() => call(`/api/copy/${fi.body.sessionId}`), (r) => r.status !== 200 || ['failed', 'done'].includes(r.body.copy.state));
    record('a failed copy keeps its error for the page', fstart.status === 202 && failed.status === 200 && failed.body.copy.state === 'failed' && /HTTP 500/.test(failed.body.copy.error),
        `${failed.status} ${JSON.stringify(failed.body.copy || failed.body).slice(0, 200)}`);

    console.log(`\n${results.filter(Boolean).length}/${results.length} passed`);
    process.exit(results.every(Boolean) ? 0 : 1);
})().catch((e) => { console.error(e); process.exit(1); });
