// Checks for the naming and link rules in public/aem-names.js and the ZIP64 decision in lib/zip-writer.js.
const AemNames = require('../public/aem-names');
const { deflateBound } = require('../lib/zip-writer');

const results = [];
const record = (name, ok, detail) => { results.push(ok); console.log(`[${ok ? 'PASS' : 'FAIL'}] ${name}: ${detail}`); };

// Kept names on a direct upload lose only what initiateUpload refuses (files) or the folder URL cannot carry
const a = AemNames.keepUploadAssetName('c #1 {x}&y?%.dita');
record('kept file name on a direct upload: # { } & ? % become -', a === 'c -1 -x--y--.dita', a);
const f = AemNames.keepUploadFolderName('f #2?. a');
record('kept folder name on a direct upload: # and ? become -, dots and spaces stay', f === 'f -2-. a', f);
const k = AemNames.keepNodeName('c #1.dita');
record('a package keeps the name exactly', k === 'c #1.dita', k);
const up = AemNames.planUpload(['top/f #2/c #1.dita'], 'content/dam/x', 0, true, true).landed[0];
const pkg = AemNames.planUpload(['top/f #2/c #1.dita'], 'content/dam/x', 0, true, false).landed[0];
record('the preview shows the direct-upload name, and the exact name for a package',
    up === '/content/dam/x/top/f -2/c -1.dita' && pkg === '/content/dam/x/top/f #2/c #1.dita', `${up} | ${pkg}`);

// Links in the selection
const rel = ['1k/g/a.ditamap', '1k/g/b.ditamap', '1k/g/b.dita', '1k/g/sub/c.dita', '1k/GUID-22222222-2222-4222-8222-222222222222.dita', '1k/g/img one.png'];
const r = AemNames.ditaLinkProblems([
    { relPath: '1k/g/a.ditamap', text: '<map><topicref href="GUID-11111111-1111-4111-8111-111111111111.dita"/><topicref href="b.dita#b"/>'
        + '<topicref href="https://example.com" scope="external"/><topicref keyref="k"/></map>' },
    { relPath: '1k/g/b.ditamap', text: '<map><topicref href="sub/c.dita"/><topicref href="../GUID-22222222-2222-4222-8222-222222222222.dita"/>'
        + "<topicref href='gone.dita' format='dita'/><topicref href=\"x.pdf\" format=\"pdf\"/></map>" },
    { relPath: '1k/g/sub/c.dita', text: '<topic id="c"><p conref="../b.dita#b/p1"/><xref href="#c/x"/><image href="../img%20one.png"/><image href="../../../x.png"/></topic>' },
], rel);
record('a GUID link that is not in the selection is reported apart', r.guidCount === 1 && r.guid[0].ref.startsWith('GUID-1111') && r.guidFiles === 1, JSON.stringify(r.guid));
record('a missing file and a path above the selection are reported', r.missingCount === 2 && r.missing.map((x) => x.ref).join(',') === 'gone.dita,../../../x.png', JSON.stringify(r.missing));
record('links that resolve, external links, other formats, key refs and same-file fragments pass', r.links === 8, `links=${r.links}`);
const clean = AemNames.ditaLinkProblems([{ relPath: 'm.ditamap', text: '<map><topicref href="t.dita"/></map>' }], ['m.ditamap', 't.dita']);
record('a selection whose links all resolve has no problems', !clean.guidCount && !clean.missingCount && clean.links === 1, JSON.stringify(clean));
record('.dita, .ditamap and .bookmap files are read for links, not .xml', AemNames.isDitaLinkFile('a.DITA') && AemNames.isDitaLinkFile('b.bookmap') && !AemNames.isDitaLinkFile('catalog.xml'), 'ok');

// ZIP64: announced whenever deflate output could reach 4 GiB
const U32 = 0xFFFFFFFF;
const MB = 1048576;
record('a 4090 MB file cannot pass 4 GiB after deflate: no ZIP64 needed', deflateBound(4090 * MB) < U32, String(deflateBound(4090 * MB)));
record('a 4095.5 MB file could: ZIP64 announced (the old threshold missed it)', deflateBound(4095.5 * MB) >= U32 && 4095.5 * MB < 0xFFFF0000, String(deflateBound(4095.5 * MB)));

console.log(`\n${results.filter(Boolean).length}/${results.length} passed`);
process.exit(results.every(Boolean) ? 0 : 1);
