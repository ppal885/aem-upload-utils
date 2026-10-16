// Standalone mock AEM for UI tests. Basic auth admin:admin. Slow completeUpload so progress can be sampled.
const http = require('http');
const path = require('path');
const PORT = parseInt(process.argv[2] || '4620', 10);
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

const server = http.createServer((req, res) => {
    let body = '';
    const raw = [];
    req.on('data', (c) => { body += c; raw.push(c); });
    req.on('end', async () => {
        const p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
        if (p === '/__installs') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify(installs)); }
        if (p === '/__folders') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify(titles)); }
        if (p === '/__completed') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify(completedPaths)); }
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
        if (req.method === 'PUT') {
            if (p.startsWith('/blob/')) blobSha.set(p.slice(6), crypto.createHash('sha1').update(Buffer.concat(raw)).digest('hex'));
            res.writeHead(201); return res.end();
        }
        if (p === '/bin/querybuilder.json') {
            const q = new URL(req.url, 'http://x').searchParams;
            const root = q.get('path') || '/';
            if (root.includes('qbfail')) { res.writeHead(500); return res.end(); }
            const hits = [...assetSha.entries()].filter(([k]) => k.startsWith(root + '/'))
                .map(([k, sha]) => ({ 'jcr:path': k, 'jcr:content': { metadata: sha ? { 'dam:sha1': sha } : {} } }));
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ success: true, results: hits.length, total: hits.length, hits }));
        }
        if (req.method === 'POST' && p.endsWith('.completeUpload.json')) {
            await delay(150);
            if (/^bad/.test(new URLSearchParams(body).get('fileName') || '')) { res.writeHead(405); return res.end(); }
            const done = p.replace('.completeUpload.json', '') + '/' + new URLSearchParams(body).get('fileName');
            completedPaths.push(done);
            assetSha.set(done, blobSha.get(done) || null);
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
