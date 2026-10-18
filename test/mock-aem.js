// Standalone mock AEM for UI tests. Basic auth admin:admin. Slow completeUpload so progress can be sampled.
//   node test/mock-aem.js [port] [seed.json]   seed: { assets: { path: { text, guid?, staleSha1? } }, profiles: [...] }
// Keeps the bytes of every asset (uploaded or seeded) and serves them back like a copy source; counts writes.
const http = require('http');
const fs = require('fs');
const path = require('path');
const PORT = parseInt(process.argv[2] || '4620', 10);
const SEED = process.argv[3] ? JSON.parse(fs.readFileSync(process.argv[3], 'utf8')) : {};
const DEFAULT_PROFILE = { id: 'b1aad0a7-9079-e56c-1ed8-6fcababe8166', name: 'Default Profile', paths: ['/content/dam'],
    schema: { catalogs: ['/libs/fmdita/dita_resources/DITA-1.3/dtd/catalog.xml'], errors: '' },
    ditaot: { zip: '/libs/fmdita/dita_resources/DITA-OT.zip', catalogs: [], errors: '' }, errors: '' };
let profiles = SEED.profiles || [DEFAULT_PROFILE];
const writes = [];
const USERS = { admin: 'Admin User', qeuser: 'QE User' };
const userFor = (h) => { const m = Object.keys(USERS).find((u) => h === 'Basic ' + Buffer.from(u + ':' + u).toString('base64')); return m || null; };
const folders = new Set(['/content/dam']);
const delay = (ms) => new Promise((r) => setTimeout(r, ms));
const completedPaths = [];
const titles = {};
const installs = [];
const crypto = require('crypto');
const blobSha = new Map();
const assetSha = new Map();
const blobData = new Map();
const assetData = new Map();
const assetGuid = new Map();
const assetGuides = new Map();
const assetDam = new Map();
const reprocessed = [];
const DITA_FILE = /\.(dita|ditamap|xml|bookmap)$/i;
const sha1 = (b) => crypto.createHash('sha1').update(b).digest('hex');
for (const [p, a] of Object.entries(SEED.assets || {})) {
    const buf = Buffer.from(a.text != null ? a.text : a.base64 || '', a.text != null ? 'utf8' : 'base64');
    assetData.set(p, buf);
    assetSha.set(p, a.staleSha1 ? sha1(Buffer.from('older version')) : sha1(buf));
    if (a.guid) assetGuid.set(p, a.guid);
    if (a.guidesState) assetGuides.set(p, a.guidesState);
    if (a.damState) assetDam.set(p, a.damState);
    for (let d = path.posix.dirname(p); d.startsWith('/content/dam/'); d = path.posix.dirname(d)) folders.add(d);
}

const server = http.createServer((req, res) => {
    let body = '';
    const raw = [];
    req.on('data', (c) => { body += c; raw.push(c); });
    req.on('end', async () => {
        const p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
        if (p === '/__installs') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify(installs)); }
        if (p === '/__folders') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify(titles)); }
        if (p === '/__completed') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify(completedPaths)); }
        if (p === '/__reprocessed') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify(reprocessed)); }
        if (p === '/__writes') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify(writes)); }
        if (p === '/__profiles' && req.method === 'POST') { profiles = JSON.parse(body); res.writeHead(200); return res.end('{}'); }
        if (p.startsWith('/__asset/')) {
            const a = assetData.get(p.slice('/__asset'.length));
            res.writeHead(a ? 200 : 404); return res.end(a || '');
        }
        if (req.method !== 'GET' && !p.startsWith('/blob/')) writes.push(`${req.method} ${p}`);
        const who = userFor(req.headers.authorization);
        if (req.method !== 'PUT' && !who) { res.writeHead(401); return res.end(); }
        if (p === '/libs/granite/security/currentuser.json') {
            res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ authorizableId: who, name: USERS[who] }));
        }
        if (req.method === 'POST' && p.startsWith('/api/assets/')) {
            const f = '/content/dam/' + p.slice('/api/assets/'.length);
            if (folders.has(f)) { res.writeHead(409); return res.end(); }
            if (!folders.has(path.posix.dirname(f))) { res.writeHead(404); return res.end(); }
            try { titles[f] = JSON.parse(body).properties['jcr:title']; } catch (_) { titles[f] = null; }
            folders.add(f); res.writeHead(201); return res.end('{}');
        }
        if (req.method === 'POST' && p.endsWith('.initiateUpload.json')) {
            const folder = p.replace('.initiateUpload.json', '');
            if (!folders.has(folder)) { res.writeHead(404); return res.end(); }
            const names = new URLSearchParams(body).getAll('fileName');
            res.writeHead(201, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ completeURI: `${folder}.completeUpload.json`, folderPath: folder,
                files: names.map((n) => ({ fileName: n, mimeType: 'application/xml', uploadToken: 't',
                    uploadURIs: [`http://127.0.0.1:${PORT}/blob/${encodeURIComponent(folder + '/' + n)}`], minPartSize: 1, maxPartSize: 1e8 })) }));
        }
        if (req.method === 'GET' && p.endsWith('/jcr:content/renditions/original')) {
            const a = assetData.get(p.slice(0, -'/jcr:content/renditions/original'.length));
            if (!a) { res.writeHead(404); return res.end(); }
            const range = /^bytes=(\d+)-(\d+)$/.exec(req.headers.range || '');
            if (range) {
                const start = Number(range[1]);
                const end = Math.min(Number(range[2]), a.length - 1);
                res.writeHead(206, { 'Content-Type': 'application/octet-stream', 'Content-Range': `bytes ${start}-${end}/${a.length}`, 'Content-Length': end - start + 1 });
                return res.end(a.subarray(start, end + 1));
            }
            res.writeHead(200, { 'Content-Type': 'application/octet-stream', 'Content-Length': a.length }); return res.end(a);
        }
        if (req.method === 'GET' && p.endsWith('/jcr:content.json') && assetData.has(p.slice(0, -'/jcr:content.json'.length))) {
            res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end('{"jcr:primaryType":"dam:AssetContent"}');
        }
        if (req.method === 'GET' && p === '/bin/fmdita/profiles') {
            res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify(profiles));
        }
        if (req.method === 'POST' && p === '/bin/fmdita/profiles') {
            const f = new URLSearchParams(body);
            if (f.get('operation') !== 'updateprofile' || f.get('mode') !== 'create') { res.writeHead(432); return res.end('{}'); }
            const prof = JSON.parse(f.get('data'));
            // real Guides fails to save a non-default profile without its (empty) Node.js context
            if (!prof.nodejs || !prof.schema || !prof.ditaot) { res.writeHead(432); return res.end('{}'); }
            prof.paths.forEach((x) => profiles.forEach((o) => { o.paths = (o.paths || []).filter((y) => y !== x); }));
            profiles.push({ ...prof, errors: '', schema: { ...prof.schema, errors: '' }, ditaot: { ...prof.ditaot, errors: '' } });
            res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify(profiles));
        }
        if (req.method === 'GET' && p === '/bin/xsd2json') {
            // 200 when the DITA profile of the file (longest path) has a catalog with this public id, like Guides' SchemaServlet
            const q = new URL(req.url, 'http://x').searchParams;
            const file = q.get('file') || '';
            let best = null;
            for (const pr of profiles) for (const r of pr.paths || []) if ((file === r || file.startsWith(r + '/')) && (!best || r.length > best.r.length)) best = { pr, r };
            const ids = new Set();
            for (const c of (best ? best.pr.schema.catalogs : [])) {
                const text = assetData.has(c) ? assetData.get(c).toString('utf8') : '';
                for (const m of text.matchAll(/publicId="([^"]+)"/g)) ids.add(m[1]);
            }
            const ok = /^-\/\/OASIS\/\//.test(q.get('publicId') || '') || ids.has(q.get('publicId'));
            res.writeHead(ok ? 200 : 400, { 'Content-Type': 'application/json' }); return res.end(ok ? '{}' : '{"code":"DTD_PATH_NOT_FOUND"}');
        }
        if (req.method === 'POST' && p === '/bin/asynccommand') {
            const folder = new URLSearchParams(body).get('folder');
            reprocessed.push(folder);
            for (const k of assetData.keys()) if (k.startsWith(folder + '/')) assetDam.set(k, 'processed');
            res.writeHead(200, { 'Content-Type': 'text/html' }); return res.end('<div id="Message">Job(Id:mock) created successfully.</div>');
        }
        if (req.method === 'GET' && p.endsWith('.json') && (folders.has(p.slice(0, -5)) || assetData.has(p.slice(0, -5)))) {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ 'jcr:primaryType': assetData.has(p.slice(0, -5)) ? 'dam:Asset' : 'sling:Folder' }));
        }
        if (req.method === 'PUT') {
            if (p.startsWith('/blob/')) {
                blobSha.set(p.slice(6), crypto.createHash('sha1').update(Buffer.concat(raw)).digest('hex'));
                blobData.set(p.slice(6), Buffer.concat(raw));
            }
            res.writeHead(201); return res.end();
        }
        if (p === '/bin/querybuilder.json') {
            const q = new URL(req.url, 'http://x').searchParams;
            const root = q.get('path') || '/';
            if (root.includes('qbfail')) { res.writeHead(500); return res.end(); }
            const want = q.get('property') === 'jcr:content/fmUuid' ? q.get('property.value') : null;
            const hits = [...assetSha.entries()].filter(([k]) => k.startsWith(root + '/'))
                .filter(([k]) => !want || assetGuid.get(k) === want)
                .map(([k, sha]) => ({ 'jcr:path': k, 'jcr:content': { ...(assetGuid.has(k) ? { fmUuid: assetGuid.get(k) } : {}),
                    ...(assetGuides.has(k) ? { 'guides:assetState': assetGuides.get(k) } : {}), ...(assetDam.has(k) ? { 'dam:assetState': assetDam.get(k) } : {}),
                    metadata: { ...(sha ? { 'dam:sha1': sha } : {}), ...(assetData.has(k) ? { 'dam:size': assetData.get(k).length } : {}) } } }));
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ success: true, results: hits.length, total: hits.length, hits }));
        }
        if (req.method === 'POST' && p.endsWith('.completeUpload.json')) {
            await delay(150);
            if (/^bad/.test(new URLSearchParams(body).get('fileName') || '')) { res.writeHead(405); return res.end(); }
            const done = p.replace('.completeUpload.json', '') + '/' + new URLSearchParams(body).get('fileName');
            completedPaths.push(done);
            assetSha.set(done, blobSha.get(done) || null);
            if (blobData.has(done)) assetData.set(done, blobData.get(done));
            // an upload is processed by AEM and by Guides (DITA files get a GUID)
            assetDam.set(done, 'processed');
            if (DITA_FILE.test(done)) { assetGuides.set(done, 'SUCCESS'); if (!assetGuid.has(done)) assetGuid.set(done, 'GUID-' + crypto.randomUUID()); }
            res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end('{}');
        }
        if (req.method === 'POST' && p === '/crx/packmgr/service.jsp') {
            const name = (/filename="([^"]+)"/.exec(body) || [])[1] || '';
            installs.push({ who, name, bytes: body.length, install: body.includes('name="install"') });
            res.writeHead(200, { 'Content-Type': 'text/xml' });
            return res.end(`<crx version="1.0"><response><data><log>Installing content
Package installed</log></data><status code="200">ok</status></response></crx>`);
        }
        res.writeHead(500); res.end();
    });
});
server.listen(PORT, '127.0.0.1', () => console.log('mock AEM on', PORT));
