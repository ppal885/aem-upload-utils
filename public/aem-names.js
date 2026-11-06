// Naming rules shared by the server and the page, so the preview shows exactly what AEM will get.
// The replacement rules match @adobe/aem-upload 2.0.3 (and the AEM Assets UI): invalid characters become "-".
(function (root, factory) {
    if (typeof module === 'object' && module.exports) module.exports = factory();
    else root.AemNames = factory();
}(typeof self !== 'undefined' ? self : this, function () {
    const INVALID_ANY = /[/:[\]|*\\]/g;
    const INVALID_FOLDER = /[.%;#+?^{}\s"&]/g;
    const INVALID_ASSET = /[#%{}?&]/g;
    const MAX_NAME_BYTES = 255;

    // Only operating-system clutter is skipped; names such as TestFile1.dita or ~notes are real content
    const SYSTEM_NAMES = [
        /^\./,
        /^~\$/,
        /^thumbs\.db$/i,
        /^desktop\.ini$/i,
        /^ehthumbs(_vista)?\.db$/i,
        /^Adobe Bridge Cache\.bct?$/i,
        /^__MACOSX$/,
    ];

    function isSystemName(name) {
        return SYSTEM_NAMES.some((re) => re.test(name || ''));
    }

    function isSystemPath(relPath) {
        return String(relPath || '').split('/').some(isSystemName);
    }

    function folderNodeName(name) {
        return String(name).normalize('NFC').replace(INVALID_FOLDER, '-').replace(INVALID_ANY, '-');
    }

    // "Keep names exactly" (and copies from another AEM): only what JCR can never store is replaced
    function keepNodeName(name) {
        return String(name).normalize('NFC').replace(INVALID_ANY, '-');
    }

    // A direct upload cannot carry every name a package can: AEM's initiateUpload refuses these in file names,
    // and @adobe/aem-upload puts folder names in the URL with encodeURI, which leaves # and ? as they are
    // (the folder would be created as "f " for "f #2"). Kept names lose these characters on a direct upload only.
    const UPLOAD_REFUSED_ASSET = /[{}&?#%]/g;
    const UPLOAD_REFUSED_FOLDER = /[?#]/g;

    function keepUploadFolderName(name) {
        return keepNodeName(name).replace(UPLOAD_REFUSED_FOLDER, '-');
    }

    function keepUploadAssetName(name) {
        return keepNodeName(name).replace(UPLOAD_REFUSED_ASSET, '-');
    }

    // Same split as Node's path.parse: the extension is kept as it is, only the base name is cleaned
    function assetNodeName(name) {
        const n = String(name).normalize('NFC');
        const dot = n.lastIndexOf('.');
        const base = dot > 0 ? n.slice(0, dot) : n;
        const ext = dot > 0 ? n.slice(dot) : '';
        return base.replace(INVALID_ASSET, '-').replace(INVALID_ANY, '-') + ext;
    }

    function byteLength(s) {
        return typeof TextEncoder !== 'undefined' ? new TextEncoder().encode(s).length : Buffer.byteLength(s);
    }

    // Accepts what people paste: "/content/dam/en/", "content\\dam\\en", "https://host/assets.html/content/dam/en"
    function normalizeDamPath(input) {
        let p = String(input || '').trim();
        const inUrl = /\/content\/dam(\/|$).*/.exec(p);
        if (/^https?:\/\//i.test(p) && inUrl) p = inUrl[0];
        p = p.replace(/\\/g, '/').split('/').map((s) => s.trim()).filter(Boolean).join('/');
        try { p = decodeURIComponent(p); } catch (_) { /* keep as typed */ }
        p = p.normalize('NFC');
        if (!p) return { path: '', error: 'Enter the DAM folder, for example content/dam/my-project' };
        if (p !== 'content/dam' && !p.startsWith('content/dam/')) {
            return { path: p, error: 'The DAM folder must be inside /content/dam' };
        }
        const segs = p.split('/');
        if (segs.some((s) => s === '.' || s === '..')) return { path: p, error: 'The DAM folder cannot contain "." or ".." parts' };
        const bad = /[#?%[\]|*:"<>]/.exec(p);
        if (bad) return { path: p, error: 'The DAM folder cannot contain the character ' + bad[0] };
        return { path: p, error: '' };
    }

    function commonFolders(relPaths) {
        let common = null;
        for (const rel of relPaths) {
            const dirs = String(rel).split('/').slice(0, -1);
            if (common === null) { common = dirs; continue; }
            let i = 0;
            while (i < common.length && i < dirs.length && common[i] === dirs[i]) i++;
            common = common.slice(0, i);
            if (!common.length) break;
        }
        return common || [];
    }

    function stripLevels(relPath, levels) {
        if (!levels) return relPath;
        return String(relPath).split('/').slice(levels).join('/');
    }

    // Where each file lands in AEM, which names change, which clash and which are too long.
    // directUpload: kept names follow the direct upload limits (a package keeps them exactly).
    function planUpload(relPaths, damPath, levels, keepNames, directUpload) {
        const dam = '/' + String(damPath || '').replace(/^\/+|\/+$/g, '');
        const damTail = dam.split('/').pop();
        let paths = relPaths.map((p) => stripLevels(String(p).normalize('NFC'), levels || 0));
        const tops = new Set(paths.map((p) => p.split('/')[0]));
        let unwrapped = '';
        // the server does not create ".../en/en" when the only top folder has the DAM folder's name
        if (tops.size === 1 && paths.every((p) => p.includes('/'))) {
            const only = [...tops][0];
            if (only === damTail) {
                unwrapped = only;
                paths = paths.map((p) => p.slice(only.length + 1));
            }
        }
        const renamed = new Map();
        const targets = new Map();
        const clashes = [];
        const tooLong = [];
        const landed = paths.map((p, i) => {
            const segs = p.split('/');
            const out = segs.map((s, j) => {
                const isFile = j === segs.length - 1;
                const node = keepNames
                    ? (directUpload ? (isFile ? keepUploadAssetName(s) : keepUploadFolderName(s)) : keepNodeName(s))
                    : isFile ? assetNodeName(s) : folderNodeName(s);
                if (node !== s) {
                    const key = segs.slice(0, j + 1).join('/');
                    if (!renamed.has(key)) renamed.set(key, { from: s, to: node, folder: !isFile });
                }
                if (byteLength(s) > MAX_NAME_BYTES && tooLong.length < 20) tooLong.push(relPaths[i]);
                return node;
            });
            const target = dam + '/' + out.join('/');
            if (targets.has(target)) {
                if (clashes.length < 20) clashes.push({ a: targets.get(target), b: relPaths[i], target });
            } else {
                targets.set(target, relPaths[i]);
            }
            return target;
        });
        return { landed, renamed: [...renamed.values()], clashes, tooLong, unwrapped };
    }

    // ─── DTD catalogs in the selection ───
    // A catalog.xml points at DTD files by relative path; if a folder or file named in that path is renamed on
    // upload, AEM Guides cannot find the DTD and the topics using it do not open.
    const CATALOG_NS = 'urn:oasis:names:tc:entity:xmlns:xml:catalog';
    const CATALOG_REF = /<(?:\w+:)?(public|system|uri|nextCatalog|delegatePublic|delegateSystem|delegateURI|rewriteSystem|rewriteURI|systemSuffix|uriSuffix)\b[^>]*?\s(uri|catalog|rewritePrefix)\s*=\s*["']([^"']+)["']/g;

    function isCatalogFile(name, text) {
        return /\.xml$/i.test(name) && /<(?:\w+:)?catalog\b/.test(text) && text.includes(CATALOG_NS);
    }

    /**
     * Relative catalog references that leave the catalog's own folder ("../x.dtd" from wd-dtd/catalog.xml):
     * AEM Guides copies only the folder of each catalog in a DITA profile, so such files are not found
     * unless that folder also holds a catalog of the same profile.
     */
    function catalogEscapes(text) {
        const out = [];
        for (const m of String(text).matchAll(CATALOG_REF)) {
            const ref = m[3].trim();
            if (!ref || /^[a-z][a-z0-9+.-]*:/i.test(ref) || ref.startsWith('/') || ref.startsWith('#')) continue;
            let depth = 0;
            let escapes = false;
            for (const seg of ref.split('#')[0].split('/')) {
                if (!seg || seg === '.') continue;
                depth += seg === '..' ? -1 : 1;
                if (depth < 0) { escapes = true; break; }
            }
            if (escapes && !out.includes(ref)) out.push(ref);
        }
        return out;
    }

    // ─── DITA-OT zips ───
    // Guides unzips the DITA-OT zip and expects everything under a top folder named exactly "DITA-OT"
    // (DitaOtContext DITA_OT_FOLDER), with DITA-OT/plugins/org.dita.base/catalog-dita.xml for Integrate Catalogs.
    // listZipEntries reads only the central directory: read(start, end) resolves to the bytes [start, end).
    async function listZipEntries(size, read) {
        const tailLen = Math.min(size, 65536 + 22);
        const tail = await read(size - tailLen, size);
        const dv = new DataView(tail.buffer, tail.byteOffset, tail.byteLength);
        let eocd = -1;
        for (let i = tail.length - 22; i >= 0; i--) if (dv.getUint32(i, true) === 0x06054b50) { eocd = i; break; }
        if (eocd < 0) throw new Error('not a zip file');
        let cdSize = dv.getUint32(eocd + 12, true);
        let cdOffset = dv.getUint32(eocd + 16, true);
        if (cdOffset === 0xffffffff || cdSize === 0xffffffff) {
            const loc = eocd - 20;
            if (loc < 0 || dv.getUint32(loc, true) !== 0x07064b50) throw new Error('ZIP64 record not found');
            const z64 = Number(dv.getBigUint64(loc + 8, true));
            const rec = await read(z64, z64 + 56);
            const rv = new DataView(rec.buffer, rec.byteOffset, rec.byteLength);
            cdSize = Number(rv.getBigUint64(40, true));
            cdOffset = Number(rv.getBigUint64(48, true));
        }
        const cd = await read(cdOffset, cdOffset + cdSize);
        const cv = new DataView(cd.buffer, cd.byteOffset, cd.byteLength);
        const names = [];
        const dec = new TextDecoder('utf-8');
        for (let p = 0; p + 46 <= cd.length && cv.getUint32(p, true) === 0x02014b50;) {
            const n = cv.getUint16(p + 28, true);
            const e = cv.getUint16(p + 30, true);
            const c = cv.getUint16(p + 32, true);
            names.push(dec.decode(cd.subarray(p + 46, p + 46 + n)));
            p += 46 + n + e + c;
        }
        return names;
    }

    /** { ok, top, catalog, problems: [string], warnings: [string] } for the entry names of a DITA-OT zip. */
    function checkDitaOtZip(names) {
        const problems = [];
        const warnings = [];
        const tops = [...new Set(names.map((n) => n.replace(/^\/+/, '').split('/')[0]).filter(Boolean))];
        if (names.some((n) => n.startsWith('/') || n.split('/').includes('..'))) problems.push('The zip has entries with ".." or absolute paths; Guides would write them outside its DITA-OT folder.');
        const hasTop = names.some((n) => n.startsWith('DITA-OT/'));
        if (!hasTop) {
            const near = tops.find((t) => t.toLowerCase().replace(/[^a-z]/g, '').startsWith('ditaot'));
            problems.push(near
                ? `The top folder in the zip is "${near}"; AEM Guides needs it named exactly "DITA-OT" (rename the folder and zip it again).`
                : `AEM Guides needs a top folder named exactly "DITA-OT" in the zip; found ${tops.slice(0, 3).map((t) => `"${t}"`).join(', ') || 'no folder'}.`);
        }
        const catalog = names.includes('DITA-OT/plugins/org.dita.base/catalog-dita.xml') ? 'DITA-OT/plugins/org.dita.base/catalog-dita.xml'
            : names.includes('DITA-OT/catalog-dita.xml') ? 'DITA-OT/catalog-dita.xml' : null;
        if (hasTop && !catalog) warnings.push('No DITA-OT/plugins/org.dita.base/catalog-dita.xml in the zip: "Integrate Catalogs" in the DITA profile would have no effect.');
        return { ok: problems.length === 0, tops, catalog, problems, warnings };
    }

    /**
     * catalogs: [{ relPath, text }] (relPath as in the selection). Returns the references that break:
     * { catalog, ref, name, to } when a name in the reference would be renamed, { catalog, ref, missing: true }
     * when the referenced file is not in the selection.
     */
    function catalogProblems(catalogs, relPaths, keepNames) {
        const have = new Set((relPaths || []).map((p) => String(p).normalize('NFC')));
        const out = [];
        const seen = new Set();
        for (const c of catalogs || []) {
            const dir = String(c.relPath).normalize('NFC').split('/').slice(0, -1);
            for (const m of String(c.text).matchAll(CATALOG_REF)) {
                const ref = m[3].trim();
                if (!ref || /^[a-z][a-z0-9+.-]*:/i.test(ref) || ref.startsWith('/') || ref.startsWith('#')) continue;
                let decoded = ref.split('#')[0];
                try { decoded = decodeURIComponent(decoded); } catch (_) { /* keep as written */ }
                const own = decoded.split('/').filter((s) => s && s !== '.');
                const folderRef = m[2] === 'rewritePrefix' || decoded.endsWith('/');
                for (let j = 0; j < own.length; j++) {
                    const s = own[j].normalize('NFC');
                    if (s === '..') continue;
                    const isFile = !folderRef && j === own.length - 1;
                    const node = keepNames ? keepNodeName(s) : isFile ? assetNodeName(s) : folderNodeName(s);
                    const key = `${c.relPath}|${ref}|r`;
                    if (node !== s && !seen.has(key)) {
                        seen.add(key);
                        out.push({ catalog: c.relPath, ref, name: s, to: node, folder: !isFile });
                    }
                }
                if (catalogEscapes(`<uri uri="${ref.replace(/"/g, '')}"/>`).length) {
                    const key = `${c.relPath}|${ref}|o`;
                    if (!seen.has(key)) { seen.add(key); out.push({ catalog: c.relPath, ref, outside: true }); }
                }
                if (!folderRef) {
                    const parts = [...dir];
                    for (const s of decoded.split('/')) {
                        if (!s || s === '.') continue;
                        if (s === '..') parts.pop(); else parts.push(s.normalize('NFC'));
                    }
                    const key = `${c.relPath}|${ref}|m`;
                    if (!have.has(parts.join('/')) && !seen.has(key)) {
                        seen.add(key);
                        out.push({ catalog: c.relPath, ref, missing: true });
                    }
                }
            }
        }
        return out;
    }

    // ─── DITA links in the selection ───
    // hrefs, conrefs and conrefends that point at files which are not in the selection break in AEM. GUID file names
    // (href="GUID-….dita") come from another UUID-based Guides server: GUIDs are given out per server.
    const DITA_LINK_FILE = /\.(dita|ditamap|bookmap)$/i;
    const TAG = /<[A-Za-z_][\w:.-]*\s[^>]*>/g;
    const LINK_ATTR = /\s(href|conref|conrefend|scope|format)\s*=\s*(["'])([^"']*)\2/g;
    const GUID_NAME = /^GUID-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(\.[A-Za-z0-9]+)?$/i;
    const NON_FILE_FORMAT = /^(html|pdf|txt|zip|htm|url|external)$/i;

    function isDitaLinkFile(name) {
        return DITA_LINK_FILE.test(name || '');
    }

    function resolveRel(fromRel, ref) {
        const parts = String(fromRel).split('/').slice(0, -1);
        for (const seg of ref.split('/')) {
            if (!seg || seg === '.') continue;
            if (seg === '..') { if (!parts.length) return null; parts.pop(); } else parts.push(seg);
        }
        return parts.join('/');
    }

    /**
     * files: [{ relPath, text }] (relPath as in the selection), relPaths: every path in the selection.
     * Returns { links, missing: [{ from, ref }], guid: [{ from, ref }], missingCount, guidCount, files }: links to
     * files not in the selection, the GUID ones apart. External links, key references and same-file
     * fragments are not checked; examples are capped at 20 each.
     */
    function ditaLinkProblems(files, relPaths) {
        const have = new Set((relPaths || []).map((p) => String(p).normalize('NFC')));
        const out = { links: 0, missing: [], guid: [], missingCount: 0, guidCount: 0, files: 0 };
        const guidFiles = new Set();
        for (const f of files || []) {
            out.files++;
            const from = String(f.relPath).normalize('NFC');
            let fileHasGuid = false;
            for (const tag of String(f.text).matchAll(TAG)) {
                const attrs = {};
                for (const a of tag[0].matchAll(LINK_ATTR)) attrs[a[1]] = a[3].trim();
                if (attrs.scope === 'external' || attrs.scope === 'peer' || (attrs.format && NON_FILE_FORMAT.test(attrs.format))) continue;
                for (const kind of ['href', 'conref', 'conrefend']) {
                    const raw = attrs[kind];
                    if (!raw || raw.startsWith('#') || raw.startsWith('/') || /^[a-z][a-z0-9+.-]*:/i.test(raw)) continue;
                    let ref = raw.split('#')[0];
                    try { ref = decodeURIComponent(ref); } catch (_) { /* keep as written */ }
                    if (!ref) continue;
                    out.links++;
                    const target = resolveRel(from, ref.normalize('NFC'));
                    if (target !== null && have.has(target)) continue;
                    const isGuid = GUID_NAME.test(ref.split('/').pop());
                    if (isGuid) { out.guidCount++; fileHasGuid = true; if (out.guid.length < 20) out.guid.push({ from, ref: raw }); }
                    else { out.missingCount++; if (out.missing.length < 20) out.missing.push({ from, ref: raw }); }
                }
            }
            if (fileHasGuid) guidFiles.add(from);
        }
        out.guidFiles = guidFiles.size;
        return out;
    }

    return {
        isDitaLinkFile, ditaLinkProblems,
        isSystemName, isSystemPath, folderNodeName, assetNodeName, keepNodeName, keepUploadFolderName, keepUploadAssetName, normalizeDamPath,
        commonFolders, stripLevels, planUpload, isCatalogFile, catalogProblems, catalogEscapes, listZipEntries, checkDitaOtZip, MAX_NAME_BYTES,
    };
}));
