#!/usr/bin/env node
'use strict';

// Plain-Node tests for package-builder.js.
//   node tools/test-package-builder.js              functional tests (+ tar / python validation)
//   node tools/test-package-builder.js --scale      also the 60,000-file scale test
//   node tools/test-package-builder.js --scale-only [--files 60000] [--bytes 209715200] [--reuse]
//   node tools/test-package-builder.js --zip64       also the ZIP64 test (4.5 GiB streamed entry, slow)
// Work dir: $PKG_TEST_DIR or os.tmpdir()/pkg-builder-test (deleted at the end unless --keep).

const assert = require('node:assert/strict');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const zlib = require('node:zlib');
const { execFileSync, spawnSync } = require('node:child_process');
const {
    buildPackage, toPlatformName, cleanFolderName, cleanAssetName, docViewString,
} = require('../package-builder');
const { crc32 } = require('../lib/zip-writer');

const argv = process.argv.slice(2);
const flag = (n) => argv.includes(n);
const opt = (n, d) => { const i = argv.indexOf(n); return i >= 0 ? argv[i + 1] : d; };
const WORK = process.env.PKG_TEST_DIR || path.join(os.tmpdir(), 'pkg-builder-test');

let passed = 0;
let failed = 0;
async function test(name, fn) {
    try {
        await fn();
        passed++;
        console.log(`PASS  ${name}`);
    } catch (e) {
        failed++;
        console.log(`FAIL  ${name}\n      ${e.stack.split('\n').slice(0, 4).join('\n      ')}`);
    }
}

// ---- independent zip reader (central directory, ZIP64 aware, verifies CRC) ----------------------
function readZip(file) {
    const buf = fs.readFileSync(file);
    let eocd = -1;
    for (let i = buf.length - 22; i >= Math.max(0, buf.length - 65557); i--) {
        if (buf.readUInt32LE(i) === 0x06054b50) { eocd = i; break; }
    }
    if (eocd < 0) throw new Error('EOCD not found');
    let count = buf.readUInt16LE(eocd + 10);
    let cdOff = buf.readUInt32LE(eocd + 16);
    if (count === 0xFFFF || cdOff === 0xFFFFFFFF) {
        const loc = eocd - 20;
        assert.equal(buf.readUInt32LE(loc), 0x07064b50, 'zip64 locator');
        const z = Number(buf.readBigUInt64LE(loc + 8));
        assert.equal(buf.readUInt32LE(z), 0x06064b50, 'zip64 eocd');
        count = Number(buf.readBigUInt64LE(z + 32));
        cdOff = Number(buf.readBigUInt64LE(z + 48));
    }
    const entries = new Map();
    let p = cdOff;
    for (let i = 0; i < count; i++) {
        assert.equal(buf.readUInt32LE(p), 0x02014b50, 'central header sig');
        const flags = buf.readUInt16LE(p + 8);
        const method = buf.readUInt16LE(p + 10);
        const crc = buf.readUInt32LE(p + 16);
        let csize = buf.readUInt32LE(p + 20);
        let usize = buf.readUInt32LE(p + 24);
        const nlen = buf.readUInt16LE(p + 28);
        const xlen = buf.readUInt16LE(p + 30);
        const clen = buf.readUInt16LE(p + 32);
        let lo = buf.readUInt32LE(p + 42);
        const name = buf.toString('utf8', p + 46, p + 46 + nlen);
        let x = p + 46 + nlen;
        const xend = x + xlen;
        while (x < xend) {
            const id = buf.readUInt16LE(x);
            const sz = buf.readUInt16LE(x + 2);
            if (id === 1) {
                let q = x + 4;
                if (usize === 0xFFFFFFFF) { usize = Number(buf.readBigUInt64LE(q)); q += 8; }
                if (csize === 0xFFFFFFFF) { csize = Number(buf.readBigUInt64LE(q)); q += 8; }
                if (lo === 0xFFFFFFFF) { lo = Number(buf.readBigUInt64LE(q)); q += 8; }
            }
            x += 4 + sz;
        }
        assert.ok(flags & 0x0800, `utf8 flag on ${name}`);
        assert.ok(!entries.has(name), `duplicate zip entry ${name}`);
        entries.set(name, { name, method, crc, csize, usize, lo, flags });
        p = xend + clen;
    }
    const data = (name) => {
        const e = entries.get(name);
        if (!e) throw new Error(`missing entry ${name}`);
        assert.equal(buf.readUInt32LE(e.lo), 0x04034b50, 'local header sig');
        const nlen = buf.readUInt16LE(e.lo + 26);
        const xlen = buf.readUInt16LE(e.lo + 28);
        const start = e.lo + 30 + nlen + xlen;
        const raw = buf.subarray(start, start + e.csize);
        const out = e.method === 8 ? zlib.inflateRawSync(raw) : Buffer.from(raw);
        assert.equal(out.length, e.usize, `size ${name}`);
        assert.equal(crc32(out), e.crc, `crc ${name}`);
        return out;
    };
    return { entries, data, text: (n) => data(n).toString('utf8') };
}

function sysTar() {
    const win = path.join(process.env.SystemRoot || 'C:\\Windows', 'System32', 'tar.exe');
    if (process.platform === 'win32' && fs.existsSync(win)) return win;
    // GNU tar cannot read zip files; libarchive's bsdtar (what Windows and macOS ship as tar) can
    if (process.platform !== 'linux') return 'tar';
    if (spawnSync('bsdtar', ['--version']).status === 0) return 'bsdtar';
    throw new Error('bsdtar not found: install libarchive-tools (apt) to read zip files with tar');
}

function pythonCmd() {
    for (const c of ['python', 'python3', 'py']) {
        const r = spawnSync(c, ['--version'], { encoding: 'utf8' });
        if (r.status === 0) return c;
    }
    return null;
}

async function write(p, content) {
    await fsp.mkdir(path.dirname(p), { recursive: true });
    await fsp.writeFile(p, content);
}

const DITA = '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE topic PUBLIC "-//OASIS//DTD DITA Topic//EN" "topic.dtd">\n'
    + '<topic id="t1"><title>Hello &amp; welcome</title><body><p>x</p></body></topic>\n';
const MAP = '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE map PUBLIC "-//OASIS//DTD DITA Map//EN" "map.dtd">\n'
    + '<map><title>M</title><topicref href="topics/intro.dita"/></map>\n';
const PNG = Buffer.from('89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000d4944415478da63f8cfc0f01f0005000201a5d3a7e60000000049454e44ae426082', 'hex');

async function makeFixture(base) {
    const src = path.join(base, 'My Docs');
    await write(path.join(src, 'guide.ditamap'), MAP);
    await write(path.join(src, 'topics', 'intro.dita'), DITA);
    await write(path.join(src, 'topics', 'topic #1 & {x}.dita'), DITA);
    await write(path.join(src, 'images', 'logo.png'), PNG);
    await write(path.join(src, 'images', 'photo.JPG'), Buffer.from([0xff, 0xd8, 0xff, 0xd9]));
    await write(path.join(src, 'Folder #2 & {y}.v1', 'data.bin'), Buffer.alloc(1000, 7));
    await write(path.join(src, 'Folder #2 & {y}.v1', '_a_b.txt'), 'underscore');
    await write(path.join(src, 'Folder #2 & {y}.v1', 'a#b.txt'), 'first');
    await write(path.join(src, 'Folder #2 & {y}.v1', 'a&b.txt'), 'second');
    await fsp.mkdir(path.join(src, 'empty dir'), { recursive: true });
    await write(path.join(src, '.hidden'), 'h');
    await write(path.join(src, 'Thumbs.db'), 'thumbs');
    await write(path.join(src, 'topics', '~$lock.dita'), 'lock');
    await write(path.join(src, 'topics', 'ehthumbs.db'), 'tmp');
    await write(path.join(src, 'desktop.ini'), 'ini');
    await write(path.join(src, '.git', 'config'), 'git');
    // larger than the 8 MB buffer limit -> streamed (deflate + data descriptor) path
    await write(path.join(src, 'big', 'large.xml'), Buffer.alloc(9 * 1024 * 1024, 'a'));
    return src;
}

const FOLDER_XML = (title) => '<?xml version="1.0" encoding="UTF-8"?>\n'
    + '<jcr:root xmlns:sling="http://sling.apache.org/jcr/sling/1.0" xmlns:jcr="http://www.jcp.org/jcr/1.0" '
    + 'xmlns:nt="http://www.jcp.org/jcr/nt/1.0"\n'
    + '    jcr:primaryType="sling:OrderedFolder">\n'
    + '    <jcr:content\n'
    + '        jcr:primaryType="nt:unstructured"\n'
    + `        jcr:title="${title}"/>\n`
    + '</jcr:root>\n';

async function functionalTests() {
    const base = path.join(WORK, 'func');
    await fsp.rm(base, { recursive: true, force: true });
    const src = await makeFixture(base);
    const out = path.join(base, 'pkg.zip');
    const progress = [];

    let res;
    await test('buildPackage (top folder) completes', async () => {
        res = await buildPackage({
            sourceDir: src,
            damRoot: '/content/dam/en/',
            packageName: 'my-docs',
            group: 'qe/uploads',
            version: '1.2',
            description: 'Desc <with> "xml" & stuff',
            createdBy: 'tester',
            outFile: out,
            onProgress: (p) => progress.push(p),
        });
        assert.equal(res.outFile, out);
        assert.equal(res.filterRoot, '/content/dam/en/My-Docs');
        assert.equal(res.files, 9);
        assert.equal(res.folders, 6); // My-Docs, big, empty-dir, Folder--2----y--v1, images, topics
        assert.ok(progress.length >= 1);
        const last = progress[progress.length - 1];
        assert.equal(last.files, 9);
        assert.equal(last.totalFiles, 10); // a&b.txt is counted, then skipped as a cleaned-name collision
        assert.equal(res.bytes, last.bytes);
    });

    const z = readZip(out);
    const R = 'jcr_root/content/dam/en/My-Docs';
    const F2 = `${R}/Folder--2----y--v1`;

    await test('exact entry list', async () => {
        const asset = (p) => [`${p}/`, `${p}/.content.xml`, `${p}/_jcr_content/`, `${p}/_jcr_content/renditions/`,
            `${p}/_jcr_content/renditions/original`, `${p}/_jcr_content/renditions/original.dir/`,
            `${p}/_jcr_content/renditions/original.dir/.content.xml`];
        const folder = (p) => [`${p}/`, `${p}/.content.xml`];
        const expected = [
            'META-INF/', 'META-INF/vault/', 'META-INF/vault/filter.xml', 'META-INF/vault/properties.xml',
            'jcr_root/', 'jcr_root/content/', 'jcr_root/content/dam/', ...folder('jcr_root/content/dam/en'),
            ...folder(R),
            ...folder(F2), ...asset(`${F2}/__a_b.txt`), ...asset(`${F2}/a-b.txt`), ...asset(`${F2}/data.bin`),
            ...folder(`${R}/big`), ...asset(`${R}/big/large.xml`),
            ...folder(`${R}/empty-dir`),
            ...asset(`${R}/guide.ditamap`),
            ...folder(`${R}/images`), ...asset(`${R}/images/logo.png`), ...asset(`${R}/images/photo.JPG`),
            ...folder(`${R}/topics`), ...asset(`${R}/topics/intro.dita`), ...asset(`${R}/topics/topic -1 - -x-.dita`),
        ].sort();
        assert.deepEqual([...z.entries.keys()].sort(), expected);
    });

    await test('filter.xml', async () => {
        assert.equal(z.text('META-INF/vault/filter.xml'), '<?xml version="1.0" encoding="UTF-8"?>\n'
            + '<workspaceFilter version="1.0">\n'
            + '    <filter root="/content/dam/en/My-Docs" mode="update"/>\n'
            + '</workspaceFilter>\n');
    });

    await test('properties.xml', async () => {
        const t = z.text('META-INF/vault/properties.xml');
        assert.match(t, /^<\?xml version="1.0" encoding="UTF-8" standalone="no"\?>\n<!DOCTYPE properties SYSTEM "http:\/\/java.sun.com\/dtd\/properties.dtd">/);
        for (const [k, v] of [['name', 'my-docs'], ['group', 'qe/uploads'], ['version', '1.2'],
            ['description', 'Desc &lt;with&gt; &quot;xml&quot; &amp; stuff'], ['createdBy', 'tester'],
            ['buildCount', '1'], ['requiresRoot', 'false'], ['allowIndexDefinitions', 'false'],
            ['packageType', 'content']]) {
            assert.ok(t.includes(`<entry key="${k}">${v}</entry>`), `property ${k}`);
        }
        assert.match(t, /<entry key="created">\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}[+-]\d\d:\d\d<\/entry>/);
    });

    await test('folder .content.xml (title = original name, escaped)', async () => {
        assert.equal(z.text(`${R}/.content.xml`), FOLDER_XML('My Docs'));
        assert.equal(z.text(`${F2}/.content.xml`), FOLDER_XML('Folder #2 &amp; {y}.v1'));
        assert.equal(z.text('jcr_root/content/dam/en/.content.xml'), FOLDER_XML('en'));
    });

    await test('asset .content.xml + original.dir + mime types', async () => {
        const cases = [
            [`${R}/topics/intro.dita`, 'application/xml'],
            [`${R}/guide.ditamap`, 'application/xml'],
            [`${R}/images/logo.png`, 'image/png'],
            [`${R}/images/photo.JPG`, 'image/jpeg'],
            [`${F2}/data.bin`, 'application/octet-stream'],
            [`${R}/big/large.xml`, 'application/xml'],
        ];
        for (const [p, mime] of cases) {
            const a = z.text(`${p}/.content.xml`);
            assert.match(a, /^<\?xml version="1.0" encoding="UTF-8"\?>\n<jcr:root xmlns:dc="http:\/\/purl.org\/dc\/elements\/1.1\/" xmlns:jcr="http:\/\/www.jcp.org\/jcr\/1.0" xmlns:dam="http:\/\/www.day.com\/dam\/1.0" xmlns:nt="http:\/\/www.jcp.org\/jcr\/nt\/1.0"\n    jcr:primaryType="dam:Asset">/);
            assert.ok(a.includes('jcr:primaryType="dam:AssetContent"'));
            assert.ok(a.includes(`<metadata\n            dc:format="${mime}"\n            jcr:primaryType="nt:unstructured"/>`), `${p} dc:format`);
            assert.ok(a.includes('<renditions jcr:primaryType="nt:folder"/>'));
            assert.match(a, /jcr:lastModified="\{Date\}\d{4}-\d\d-\d\dT[\d:.]+[+-]\d\d:\d\d"/);
            const o = z.text(`${p}/_jcr_content/renditions/original.dir/.content.xml`);
            assert.ok(o.includes('jcr:primaryType="nt:file"'));
            assert.ok(o.includes(`jcr:mimeType="${mime}"`), `${p} jcr:mimeType`);
            assert.ok(o.includes('jcr:primaryType="nt:resource"'));
        }
    });

    await test('binaries are byte-identical (buffered, stored and streamed paths)', async () => {
        const orig = (p) => z.data(`${p}/_jcr_content/renditions/original`);
        assert.equal(orig(`${R}/topics/intro.dita`).toString(), DITA);
        assert.deepEqual(orig(`${R}/images/logo.png`), PNG);
        assert.equal(z.entries.get(`${R}/images/logo.png/_jcr_content/renditions/original`).method, 0, 'png stored');
        const big = z.entries.get(`${R}/big/large.xml/_jcr_content/renditions/original`);
        assert.ok(big.flags & 0x8, 'large file uses data descriptor');
        assert.equal(big.method, 8);
        assert.equal(orig(`${R}/big/large.xml`).length, 9 * 1024 * 1024);
        assert.equal(orig(`${F2}/a-b.txt`).toString(), 'first', 'first of colliding names wins');
    });

    await test('skipped temp/hidden files and collisions reported', async () => {
        const names = z.entries.keys();
        for (const n of names) assert.ok(!/Thumbs|\.hidden|desktop|~\$lock|save\.tmp|\.git/.test(n), `leaked ${n}`);
        const reasons = res.skipped.map((s) => `${path.basename(s.path)}|${s.reason.split(':')[0]}`);
        for (const n of ['.hidden', 'Thumbs.db', 'desktop.ini', '.git', '~$lock.dita', 'ehthumbs.db']) {
            assert.ok(reasons.some((r) => r.startsWith(`${n}|system file`)), `skip ${n}`);
        }
        assert.ok(reasons.some((r) => r.startsWith('a&b.txt|name collision')));
    });

    await test('name cleaning + FileVault platform escaping units', async () => {
        assert.equal(cleanFolderName('My Folder #1 & {x}.v2;+?^"%'), 'My-Folder--1----x--v2------');
        assert.equal(cleanFolderName('a:b[c]|d*e\\f'), 'a-b-c--d-e-f');
        assert.equal(cleanAssetName('topic #1 & {x}?%.dita'), 'topic -1 - -x---.dita');
        assert.equal(cleanAssetName('a:b[1]|*.png'), 'a-b-1---.png');
        assert.equal(cleanAssetName('a.b.c.dita'), 'a.b.c.dita');
        assert.equal(toPlatformName('jcr:content'), '_jcr_content');
        assert.equal(toPlatformName('_a_b'), '__a_b');
        assert.equal(toPlatformName('_ab'), '_ab');
        assert.equal(toPlatformName('a_b'), 'a_b');
        assert.equal(toPlatformName('a:b:c'), '_a_b%3ac');
        assert.equal(toPlatformName(':x'), '%3ax');
        assert.equal(toPlatformName('q"<>|?*%\\'), 'q%22%3c%3e%7c%3f*%25%5c');
        assert.equal(docViewString('{Date}x'), '\\{Date}x');
        assert.equal(docViewString('[a]'), '\\[a]');
        assert.equal(docViewString('a\\b&<"'), 'a\\\\b&amp;&lt;&quot;');
    });

    await test('skipTopFolder: contents go directly under damRoot, one filter per child', async () => {
        const out2 = path.join(base, 'pkg-skip.zip');
        const r = await buildPackage({ sourceDir: src, damRoot: '/content/dam/en', skipTopFolder: true, outFile: out2 });
        assert.deepEqual(r.filterRoots, [
            '/content/dam/en/Folder--2----y--v1', '/content/dam/en/big', '/content/dam/en/empty-dir',
            '/content/dam/en/guide.ditamap', '/content/dam/en/images', '/content/dam/en/topics',
        ]);
        const z2 = readZip(out2);
        const keys = [...z2.entries.keys()];
        assert.ok(keys.includes('jcr_root/content/dam/en/guide.ditamap/.content.xml'));
        assert.ok(keys.includes('jcr_root/content/dam/en/topics/intro.dita/_jcr_content/renditions/original'));
        assert.ok(!keys.some((k) => k.includes('My-Docs')));
        assert.equal(r.files, 9);
        assert.equal(r.folders, 5);
        const f = z2.text('META-INF/vault/filter.xml');
        assert.equal((f.match(/<filter /g) || []).length, 6);
        assert.ok(f.includes('<filter root="/content/dam/en/guide.ditamap" mode="update"/>'));
    });

    await test('input validation', async () => {
        const o = path.join(base, 'bad.zip');
        await assert.rejects(buildPackage({ sourceDir: src, damRoot: '/content/en', outFile: o }), /under \/content\/dam/);
        await assert.rejects(buildPackage({ sourceDir: src, damRoot: '/content/dam/../x', outFile: o }), /"\.\."/);
        await assert.rejects(buildPackage({ sourceDir: src, damRoot: '/content/dam/', outFile: o }), /under \/content\/dam/);
        await assert.rejects(buildPackage({ sourceDir: src, damRoot: '/content/dam/a', outFile: o, packageName: 'a/b' }), /name/);
        await assert.rejects(buildPackage({ sourceDir: path.join(base, 'nope'), damRoot: '/content/dam/a', outFile: o }), /ENOENT/);
    });

    await test('standard tools read the zip (tar -tf / tar -xf / python zipfile.testzip)', async () => {
        const tar = sysTar();
        const list = execFileSync(tar, ['-tf', out], { encoding: 'utf8', maxBuffer: 1 << 26 });
        assert.ok(list.includes('META-INF/vault/filter.xml'));
        assert.ok(list.includes('topic -1 - -x-.dita/_jcr_content/renditions/original'));
        const x = path.join(base, 'extracted');
        await fsp.mkdir(x, { recursive: true });
        execFileSync(tar, ['-xf', out, '-C', x]);
        const p = path.join(x, 'jcr_root/content/dam/en/My-Docs/topics/intro.dita/_jcr_content/renditions/original');
        assert.equal(fs.readFileSync(p, 'utf8'), DITA);
        console.log(`      tar: ${tar}`);
        const py = pythonCmd();
        if (!py) { console.log('      python not found - skipped zipfile check'); return; }
        const r = spawnSync(py, ['-I', '-c', 'import sys,zipfile; z=zipfile.ZipFile(sys.argv[1]); print(z.testzip(), len(z.namelist()))', out], { encoding: 'utf8' });
        assert.equal(r.status, 0, r.stderr);
        assert.match(r.stdout.trim(), /^None \d+$/, `python testzip: ${r.stdout} ${r.stderr}`);
        console.log(`      python testzip: ${r.stdout.trim()}`);
    });

    await test('CLI tools/build-package.js', async () => {
        const o = path.join(base, 'cli.zip');
        const r = spawnSync(process.execPath, [path.join(__dirname, 'build-package.js'), '--source', src,
            '--dam', '/content/dam/en', '--name', 'cli-pkg', '--group', 'g', '--version', '2.0',
            '--skip-top-folder', 'true', '--out', o], { encoding: 'utf8' });
        assert.equal(r.status, 0, r.stderr + r.stdout);
        assert.match(r.stdout, /files=9 folders=5/);
        const t = readZip(o).text('META-INF/vault/properties.xml');
        assert.ok(t.includes('<entry key="name">cli-pkg</entry>') && t.includes('<entry key="version">2.0</entry>'));
        const bad = spawnSync(process.execPath, [path.join(__dirname, 'build-package.js'), '--source', src], { encoding: 'utf8' });
        assert.equal(bad.status, 2);
    });
}

// ZIP64 paths without a 4 GB source file: a >65535-entry archive and one streamed 4.5 GiB entry of
// zeros (deflates to a few MB on disk). Verified with the own reader, tar and python.
async function zip64Test() {
    const { Readable } = require('node:stream');
    const { ZipWriter } = require('../lib/zip-writer');
    const base = path.join(WORK, 'zip64');
    await fsp.rm(base, { recursive: true, force: true });
    await fsp.mkdir(base, { recursive: true });
    const out = path.join(base, 'z64.zip');
    const size = 4.5 * 1024 * 1024 * 1024;
    const chunk = Buffer.alloc(4 * 1024 * 1024);
    const zeros = Readable.from((function* gen() {
        for (let left = size; left > 0; left -= chunk.length) yield left >= chunk.length ? chunk : chunk.subarray(0, left);
    })());
    const t0 = Date.now();
    await test('zip64: 70,000 entries + one streamed 4.5 GiB entry', async () => {
        const z = new ZipWriter(out, { level: 1 });
        await z.addBuffer('first.txt', Buffer.from('first'), new Date());
        const r = await z.addStream('huge.bin', zeros, new Date(), size);
        assert.equal(r.usize, size);
        for (let i = 0; i < 70000; i++) await z.addBuffer(`many/${i}.txt`, Buffer.from(String(i)), new Date());
        const res = await z.finish();
        assert.equal(res.entries, 70002);
        assert.ok(res.zip64);
        const zr = readZip(out);
        assert.equal(zr.entries.size, 70002);
        assert.equal(zr.entries.get('huge.bin').usize, size);
        assert.equal(zr.text('many/69999.txt'), '69999');
        assert.equal(zr.text('first.txt'), 'first');
        const list = execFileSync(sysTar(), ['-tvf', out], { maxBuffer: 1 << 28 }).toString('utf8');
        assert.ok(/\b4831838208\b.*huge\.bin/.test(list), 'tar lists 4.5 GiB size');
        const py = pythonCmd();
        if (py) {
            const pr = spawnSync(py, ['-I', '-c', 'import sys,zipfile; z=zipfile.ZipFile(sys.argv[1]); i=z.getinfo("huge.bin"); print(z.testzip(), len(z.namelist()), i.file_size)', out], { encoding: 'utf8', maxBuffer: 1 << 20 });
            assert.equal(pr.status, 0, pr.stderr);
            assert.equal(pr.stdout.trim(), `None 70002 ${size}`);
        }
        console.log(`      zip ${(fs.statSync(out).size / 1048576).toFixed(1)} MB, ${((Date.now() - t0) / 1000).toFixed(1)}s incl. verification`);
    });
}

async function scaleTest() {
    const nFiles = Number(opt('--files', 60000));
    const totalBytes = Number(opt('--bytes', 200 * 1024 * 1024));
    const per = Math.floor(totalBytes / nFiles);
    const base = path.join(WORK, 'scale');
    const src = path.join(base, 'scale-src');
    const out = path.join(base, 'scale.zip');
    const reuse = flag('--reuse') && fs.existsSync(src);
    if (!reuse) await fsp.rm(base, { recursive: true, force: true });
    const t0 = Date.now();
    const filler = Buffer.alloc(per);
    for (let i = 0; i < per; i++) filler[i] = 32 + ((i * 7919) % 95);
    const perDir = 1000;
    for (let d = 0; !reuse && d * perDir < nFiles; d++) {
        const dir = path.join(src, `chapter ${d}`, `sub-${d % 7}`);
        await fsp.mkdir(dir, { recursive: true });
        const jobs = [];
        for (let i = d * perDir; i < Math.min(nFiles, (d + 1) * perDir); i++) {
            const ext = i % 10 === 0 ? '.ditamap' : (i % 10 === 1 ? '.png' : '.dita');
            jobs.push(fsp.writeFile(path.join(dir, `topic_${i}${ext}`), filler));
        }
        await Promise.all(jobs);
    }
    if (reuse) console.log(`      reusing existing source tree ${src}`);
    else console.log(`      generated ${nFiles} files x ${per} B in ${((Date.now() - t0) / 1000).toFixed(1)}s`);

    if (global.gc) global.gc();
    const rssStart = process.memoryUsage().rss;
    let peakRss = rssStart;
    let peakHeap = 0;
    const sampler = setInterval(() => {
        const m = process.memoryUsage();
        peakRss = Math.max(peakRss, m.rss);
        peakHeap = Math.max(peakHeap, m.heapUsed);
    }, 50);
    let progressCalls = 0;
    const t1 = Date.now();
    let res;
    await test(`scale: ${nFiles} files / ${(nFiles * per / 1048576).toFixed(0)} MB`, async () => {
        res = await buildPackage({
            sourceDir: src, damRoot: '/content/dam/scale', outFile: out,
            onProgress: (p) => { progressCalls++; peakRss = Math.max(peakRss, process.memoryUsage().rss); assert.ok(p.files <= p.totalFiles); },
        });
        assert.equal(res.files, nFiles);
        assert.equal(res.bytes, nFiles * per);
    });
    clearInterval(sampler);
    const secs = (Date.now() - t1) / 1000;
    if (!res) return;
    console.log(`      build: ${secs.toFixed(1)}s, zip ${(res.zipBytes / 1048576).toFixed(1)} MB, entries ${res.entries}, `
        + `zip64=${res.zip64}, progress callbacks ${progressCalls}`);
    console.log(`      memory: rss start ${(rssStart / 1048576).toFixed(0)} MB, peak rss ${(peakRss / 1048576).toFixed(0)} MB, `
        + `peak heapUsed ${(peakHeap / 1048576).toFixed(0)} MB`);

    await test('scale: zip readable (own reader spot-check, tar -tf count, python testzip)', async () => {
        const z = readZip(out);
        assert.equal(z.entries.size, res.entries);
        const sample = 'jcr_root/content/dam/scale/scale-src/chapter-59/sub-3/topic_59999.dita/_jcr_content/renditions/original';
        if (nFiles === 60000) assert.deepEqual(z.data(sample), filler);
        const tar = sysTar();
        const t2 = Date.now();
        const list = execFileSync(tar, ['-tf', out], { maxBuffer: 1 << 30 }).toString('utf8').split('\n').filter(Boolean);
        assert.equal(list.length, res.entries);
        console.log(`      tar -tf: ${list.length} entries in ${((Date.now() - t2) / 1000).toFixed(1)}s`);
        const py = pythonCmd();
        if (py) {
            const t3 = Date.now();
            const r = spawnSync(py, ['-I', '-c', 'import sys,zipfile; z=zipfile.ZipFile(sys.argv[1]); print(z.testzip(), len(z.namelist()))', out], { encoding: 'utf8' });
            assert.equal(r.status, 0, r.stderr);
            assert.equal(r.stdout.trim(), `None ${res.entries}`);
            console.log(`      python testzip: OK (${((Date.now() - t3) / 1000).toFixed(1)}s)`);
        }
    });
}

(async () => {
    await fsp.mkdir(WORK, { recursive: true });
    if (!flag('--scale-only')) await functionalTests();
    if (flag('--scale') || flag('--scale-only')) await scaleTest();
    if (flag('--zip64')) await zip64Test();
    if (!flag('--keep')) await fsp.rm(WORK, { recursive: true, force: true }).catch(() => {});
    console.log(`\n${passed} passed, ${failed} failed`);
    process.exit(failed ? 1 : 0);
})();
