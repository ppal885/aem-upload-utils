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

    // Where each file lands in AEM, which names change, which clash and which are too long
    function planUpload(relPaths, damPath, levels) {
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
                const node = isFile ? assetNodeName(s) : folderNodeName(s);
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

    return {
        isSystemName, isSystemPath, folderNodeName, assetNodeName, normalizeDamPath,
        commonFolders, stripLevels, planUpload, MAX_NAME_BYTES,
    };
}));
