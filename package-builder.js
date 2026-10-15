'use strict';

/**
 * Builds an AEM CRX / FileVault content package (.zip) from a local folder, laid out the way
 * CRX Package Manager exports DAM assets, so the package can be uploaded + installed instead of
 * uploading assets one by one.
 *
 *   const { buildPackage } = require('./package-builder');
 *   await buildPackage({ sourceDir, damRoot: '/content/dam/en', outFile: 'x.zip' });
 *
 * Streaming: the source tree is walked depth-first and every file is written straight into the
 * zip (small files are read whole, large files are streamed); the zip's central directory is
 * spilled to a sidecar file (see lib/zip-writer.js). Memory does not grow with the file count
 * beyond one directory listing at a time and a per-directory name set.
 */

const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const { ZipWriter } = require('./lib/zip-writer');

// --- Node names and skipped files: the same rules as a direct upload (public/aem-names.js) ---------
const AemNames = require('./public/aem-names');

const cleanFolderName = AemNames.folderNodeName;
const cleanAssetName = AemNames.assetNodeName;
const isTempName = AemNames.isSystemName;

// --- FileVault PlatformNameFormat.getPlatformName (repository name -> archive path segment) -----
// "jcr:content" -> "_jcr_content"; a leading "_" with another "_" gets doubled ("_a_b" -> "__a_b");
// a second ":" and \ < > " / | ? % become %XX (lower-case hex). Output checked against FileVault 3.2.0
// PlatformNameFormat.getPlatformName ("*" is kept as is there; cleaned names never contain it anyway).
const PLATFORM_ESCAPES = {
    '\\': '%5c', '<': '%3c', '>': '%3e', '"': '%22', '/': '%2f', '|': '%7c', '?': '%3f', '%': '%25',
};

function toPlatformName(repoName) {
    let buf = '_';
    let escapeColon = false;
    let useUnderscore = false;
    let numUnderscore = 0;
    for (let i = 0; i < repoName.length; i++) {
        const c = repoName[i];
        if (c === ':') {
            if (!escapeColon && i > 0) {
                escapeColon = true;
                useUnderscore = true;
                numUnderscore = 2;
                buf += '_';
            } else {
                buf += '%3a';
            }
        } else if (c === '_') {
            if (i === 0) useUnderscore = true;
            numUnderscore++;
            buf += c;
        } else if (PLATFORM_ESCAPES[c]) {
            buf += PLATFORM_ESCAPES[c];
        } else {
            buf += c;
        }
    }
    return useUnderscore && numUnderscore > 1 ? buf : buf.slice(1);
}

// --- XML helpers -------------------------------------------------------------------------------
// eslint-disable-next-line no-control-regex
const XML_INVALID_CHARS = /[\u0000-\u0008\u000B\u000C\u000E-\u001F￾￿]/;

function xmlEscape(value) {
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;')
        .replace(/\t/g, '&#x9;')
        .replace(/\n/g, '&#xa;')
        .replace(/\r/g, '&#xd;');
}

// FileVault DocView single-value string escaping: "\" -> "\\", and a leading "{" or "[" is escaped
// so it is not parsed as a {Type} hint or a multi-value array. Then XML attribute escaping.
function docViewString(value) {
    let v = String(value).replace(/\\/g, '\\\\');
    if (v.startsWith('{') || v.startsWith('[')) v = `\\${v}`;
    return xmlEscape(v);
}

function isoWithOffset(date) {
    const pad = (n, w = 2) => String(n).padStart(w, '0');
    const off = -date.getTimezoneOffset();
    const sign = off >= 0 ? '+' : '-';
    const abs = Math.abs(off);
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`
        + `T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(date.getMilliseconds(), 3)}`
        + `${sign}${pad(Math.floor(abs / 60))}:${pad(abs % 60)}`;
}

const XML_DECL = '<?xml version="1.0" encoding="UTF-8"?>\n';
const NS = {
    jcr: 'http://www.jcp.org/jcr/1.0',
    nt: 'http://www.jcp.org/jcr/nt/1.0',
    sling: 'http://sling.apache.org/jcr/sling/1.0',
    dam: 'http://www.day.com/dam/1.0',
    dc: 'http://purl.org/dc/elements/1.1/',
};
function nsDecl(prefixes) {
    return prefixes.map((p) => `xmlns:${p}="${NS[p]}"`).join(' ');
}

// DAM folders: sling:OrderedFolder. That is what the Assets HTTP API (POST class=assetFolder, used by
// @adobe/aem-upload to create folders) and the Assets UI "Create Folder" (orderable default) create,
// so packaged folders match uploaded folders. Title lives on jcr:content like the Assets API sets it.
function folderXml(title) {
    return `${XML_DECL}<jcr:root ${nsDecl(['sling', 'jcr', 'nt'])}
    jcr:primaryType="sling:OrderedFolder">
    <jcr:content
        jcr:primaryType="nt:unstructured"
        jcr:title="${docViewString(title)}"/>
</jcr:root>
`;
}

function assetXml(mimeType, lastModified) {
    return `${XML_DECL}<jcr:root ${nsDecl(['dc', 'jcr', 'dam', 'nt'])}
    jcr:primaryType="dam:Asset">
    <jcr:content
        jcr:lastModified="{Date}${lastModified}"
        jcr:primaryType="dam:AssetContent">
        <metadata
            dc:format="${docViewString(mimeType)}"
            jcr:primaryType="nt:unstructured"/>
        <renditions jcr:primaryType="nt:folder"/>
    </jcr:content>
</jcr:root>
`;
}

function originalDirXml(mimeType, lastModified) {
    return `${XML_DECL}<jcr:root ${nsDecl(['jcr', 'nt'])}
    jcr:primaryType="nt:file">
    <jcr:content
        jcr:lastModified="{Date}${lastModified}"
        jcr:mimeType="${docViewString(mimeType)}"
        jcr:primaryType="nt:resource"/>
</jcr:root>
`;
}

// --- MIME types --------------------------------------------------------------------------------
const MIME_TYPES = {
    '.dita': 'application/xml',
    '.ditamap': 'application/xml',
    '.ditaval': 'application/xml',
    '.bookmap': 'application/xml',
    '.xml': 'application/xml',
    '.xsl': 'application/xml',
    '.xslt': 'application/xml',
    '.dtd': 'application/xml-dtd',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.bmp': 'image/bmp',
    '.svg': 'image/svg+xml',
    '.tif': 'image/tiff',
    '.tiff': 'image/tiff',
    '.webp': 'image/webp',
    '.ico': 'image/x-icon',
    '.eps': 'application/postscript',
    '.psd': 'image/vnd.adobe.photoshop',
    '.pdf': 'application/pdf',
    '.html': 'text/html',
    '.htm': 'text/html',
    '.css': 'text/css',
    '.js': 'application/javascript',
    '.json': 'application/json',
    '.txt': 'text/plain',
    '.csv': 'text/csv',
    '.md': 'text/markdown',
    '.zip': 'application/zip',
    '.mp4': 'video/mp4',
    '.mp3': 'audio/mpeg',
    '.doc': 'application/msword',
    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.xls': 'application/vnd.ms-excel',
    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.ppt': 'application/vnd.ms-powerpoint',
    '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
};
const DEFAULT_MIME = 'application/octet-stream';

function mimeTypeFor(fileName) {
    return MIME_TYPES[path.extname(fileName).toLowerCase()] || DEFAULT_MIME;
}

// Already-compressed formats are STORED (deflate would only cost CPU).
const STORE_EXTENSIONS = new Set(['.png', '.jpg', '.jpeg', '.gif', '.webp', '.zip', '.mp4', '.mp3',
    '.docx', '.xlsx', '.pptx', '.gz', '.7z', '.pdf']);

// Files up to this size are read whole (CRC known before the header); bigger ones are streamed.
const BUFFER_LIMIT = 8 * 1024 * 1024;

// Parallel file opens/reads ahead of the (sequential) zip writer, and the largest file read during
// prefetch (bigger ones are read/streamed when their turn comes). Worst-case prefetch memory per
// directory level: PREFETCH * PREFETCH_READ_LIMIT.
const PREFETCH = 16;
const PREFETCH_READ_LIMIT = 1024 * 1024;

/** 'dir' | 'file' | 'other' | 'error' for a Dirent, following symlinks. */
async function entryKind(dir, dirent) {
    if (dirent.isDirectory()) return 'dir';
    if (dirent.isFile()) return 'file';
    if (!dirent.isSymbolicLink()) return 'other';
    try {
        const st = await fsp.stat(path.join(dir, dirent.name));
        if (st.isDirectory()) return 'dir';
        return st.isFile() ? 'file' : 'other';
    } catch (_) {
        return 'error';
    }
}

// --- validation --------------------------------------------------------------------------------
function validateDamRoot(damRoot) {
    if (typeof damRoot !== 'string') throw new Error('damRoot is required');
    const root = damRoot.replace(/\/+$/, '');
    if (!root.startsWith('/content/dam/') || root.length <= '/content/dam/'.length) {
        throw new Error(`damRoot must be a path under /content/dam/ (got "${damRoot}")`);
    }
    const segs = root.split('/').slice(1);
    for (const s of segs) {
        if (!s || s === '.' || s === '..') throw new Error(`damRoot contains an empty, "." or ".." segment: "${damRoot}"`);
        if (/[\\:[\]|*]/.test(s) || XML_INVALID_CHARS.test(s)) {
            throw new Error(`damRoot segment "${s}" contains characters that are invalid in a JCR name`);
        }
    }
    return root;
}

function validatePackageField(label, value, { allowSlash = false } = {}) {
    const v = String(value);
    if (!v.trim()) throw new Error(`${label} must not be empty`);
    if (XML_INVALID_CHARS.test(v)) throw new Error(`${label} contains control characters`);
    const bad = allowSlash ? /[\\:*?"<>|]|\.\./ : /[/\\:*?"<>|]/;
    if (bad.test(v)) throw new Error(`${label} "${v}" contains characters not allowed in a package ${label}`);
    return v;
}

// --- main --------------------------------------------------------------------------------------
/**
 * @returns {Promise<{outFile:string, files:number, folders:number, bytes:number, filterRoot:string|string[],
 *   filterRoots:string[], zipBytes:number, entries:number, skipped:Array<{path:string, reason:string}>}>}
 *   bytes = total size of packaged asset binaries; zipBytes = size of the written .zip.
 */
async function buildPackage({
    sourceDir,
    damRoot,
    packageName,
    group = 'aem-upload-utils',
    version = '1.0',
    description = '',
    createdBy = 'aem-upload-utils',
    skipTopFolder = false,
    outFile,
    onProgress,
} = {}) {
    if (!sourceDir) throw new Error('sourceDir is required');
    if (!outFile) throw new Error('outFile is required');
    const root = validateDamRoot(damRoot);
    const srcAbs = path.resolve(sourceDir);
    const srcStat = await fsp.stat(srcAbs);
    if (!srcStat.isDirectory()) throw new Error(`sourceDir is not a directory: ${srcAbs}`);

    const topRaw = path.basename(srcAbs);
    const topName = cleanFolderName(topRaw);
    const name = validatePackageField('name', packageName || topName || 'dam-package');
    const grp = validatePackageField('group', group, { allowSlash: true });
    const ver = validatePackageField('version', version);
    if (!skipTopFolder && (!topName || topName === '.' || topName === '..')) {
        throw new Error(`cannot derive a folder name from sourceDir "${srcAbs}"; use skipTopFolder`);
    }

    const skipped = [];

    // Pass 1: count files for progress reporting. Dirent types avoid a stat per file (stat is only
    // needed to resolve symlinks, which are followed like @adobe/aem-upload's fs.stat does).
    let totalFiles = 0;
    const count = async (dir) => {
        let entries;
        try { entries = await fsp.readdir(dir, { withFileTypes: true }); } catch (_) { return; }
        for (const d of entries) {
            if (isTempName(d.name)) continue;
            // eslint-disable-next-line no-await-in-loop
            const kind = await entryKind(dir, d);
            // eslint-disable-next-line no-await-in-loop
            if (kind === 'dir') await count(path.join(dir, d.name));
            else if (kind === 'file') totalFiles++;
        }
    };
    await count(srcAbs);

    // Top-level JCR names that this package will own (for filter.xml).
    const rootSegs = root.split('/').slice(1); // ['content','dam','en']
    let filterRoots;
    if (!skipTopFolder) {
        filterRoots = [`${root}/${topName}`];
    } else {
        filterRoots = [];
        const seen = new Set();
        const children = (await fsp.readdir(srcAbs)).sort();
        for (const n of children) {
            if (isTempName(n)) continue;
            let st;
            try { st = await fsp.stat(path.join(srcAbs, n)); } catch (_) { continue; }
            let jcrName;
            if (st.isDirectory()) jcrName = cleanFolderName(n);
            else if (st.isFile()) jcrName = cleanAssetName(n);
            else continue;
            if (!jcrName || seen.has(jcrName)) continue;
            if (st.isFile() && jcrName.endsWith('.dir')) continue;
            seen.add(jcrName);
            filterRoots.push(`${root}/${jcrName}`);
        }
        if (filterRoots.length === 0) throw new Error(`sourceDir has nothing to package: ${srcAbs}`);
    }

    const now = new Date();
    const zip = new ZipWriter(outFile);
    let files = 0;
    let folders = 0;
    let bytes = 0;
    let lastProgress = 0;
    const progress = (force) => {
        if (typeof onProgress !== 'function') return;
        const t = Date.now();
        if (!force && t - lastProgress < 250) return;
        lastProgress = t;
        try { onProgress({ files, totalFiles, bytes }); } catch (_) { /* caller bug, ignore */ }
    };

    const metaInf = async () => {
        await zip.addDirectory('META-INF/', now);
        await zip.addDirectory('META-INF/vault/', now);
        // Filter strategy (see report): one <filter> per top-level node this package creates, each with
        // mode="update". "update" adds new nodes and overwrites nodes contained in the package but never
        // removes existing repository nodes that are absent from the package. That mirrors what the upload
        // tool does (create/overwrite, never delete) and guarantees installing cannot delete unrelated
        // content under damRoot - or under the packaged folder itself. The default mode "replace" would
        // wipe every existing asset below each filter root that is not in the zip, and a root of damRoot
        // itself would wipe all siblings; both are rejected. Trade-off: renditions/metadata already on an
        // existing asset that the package does not carry are kept as they are.
        const filterXml = `${XML_DECL}<workspaceFilter version="1.0">\n${
            filterRoots.map((r) => `    <filter root="${xmlEscape(r)}" mode="update"/>\n`).join('')
        }</workspaceFilter>\n`;
        await zip.addBuffer('META-INF/vault/filter.xml', Buffer.from(filterXml, 'utf8'), now);
        const props = [
            ['name', name],
            ['group', grp],
            ['version', ver],
            ['description', description || ''],
            ['createdBy', createdBy || ''],
            ['created', isoWithOffset(now)],
            ['buildCount', '1'],
            ['requiresRoot', 'false'],
            ['allowIndexDefinitions', 'false'],
            ['packageType', 'content'],
            ['packageFormatVersion', '2'],
        ];
        const propsXml = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n'
            + '<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">\n'
            + '<properties>\n<comment>FileVault Package Properties</comment>\n'
            + props.map(([k, v]) => `<entry key="${xmlEscape(k)}">${xmlEscape(v)}</entry>\n`).join('')
            + '</properties>\n';
        await zip.addBuffer('META-INF/vault/properties.xml', Buffer.from(propsXml, 'utf8'), now);
    };

    const addFolder = async (zipDir, title, mtime) => {
        await zip.addDirectory(`${zipDir}/`, mtime);
        await zip.addBuffer(`${zipDir}/.content.xml`, Buffer.from(folderXml(title), 'utf8'), mtime);
    };

    const addAsset = async (absPath, loaded, zipDir, rawName) => {
        const { st } = loaded;
        const mime = mimeTypeFor(rawName);
        const mtime = st.mtime;
        const lm = isoWithOffset(mtime);
        await zip.addDirectory(`${zipDir}/`, mtime);
        await zip.addBuffer(`${zipDir}/.content.xml`, Buffer.from(assetXml(mime, lm), 'utf8'), mtime);
        await zip.addDirectory(`${zipDir}/_jcr_content/`, mtime);
        await zip.addDirectory(`${zipDir}/_jcr_content/renditions/`, mtime);
        const orig = `${zipDir}/_jcr_content/renditions/original`;
        if (loaded.data || st.size <= BUFFER_LIMIT) {
            const data = loaded.data || await fsp.readFile(absPath);
            await zip.addBuffer(orig, data, mtime, STORE_EXTENSIONS.has(path.extname(rawName).toLowerCase()));
            bytes += data.length;
        } else {
            const { usize } = await zip.addStream(orig, fs.createReadStream(absPath, { highWaterMark: 1 << 20 }),
                mtime, st.size);
            bytes += usize;
        }
        await zip.addDirectory(`${orig}.dir/`, mtime);
        await zip.addBuffer(`${orig}.dir/.content.xml`, Buffer.from(originalDirXml(mime, lm), 'utf8'), mtime);
        files++;
        progress(false);
    };

    // stat + (small) read of one file through a single handle; errors are returned, not thrown.
    const loadFile = async (abs) => {
        let fh;
        try {
            fh = await fsp.open(abs, 'r');
            const st = await fh.stat();
            const data = st.size <= PREFETCH_READ_LIMIT ? await fh.readFile() : null;
            return { st, data };
        } catch (error) {
            return { error };
        } finally {
            if (fh) await fh.close().catch(() => {});
        }
    };

    // Walks one local directory whose JCR node is at zipDir (already written). Output order is strictly
    // sequential, but up to PREFETCH upcoming files of the directory are opened/read in parallel: on
    // Windows (antivirus scan on open) per-file latency, not CPU, dominates large trees.
    const walk = async (dir, zipDir, jcrPath) => {
        let dirents;
        try {
            dirents = await fsp.readdir(dir, { withFileTypes: true });
        } catch (e) {
            skipped.push({ path: dir, reason: `unreadable directory: ${e.message}` });
            return;
        }
        dirents.sort((x, y) => (x.name < y.name ? -1 : (x.name > y.name ? 1 : 0)));
        const used = new Set();
        const items = [];
        for (const d of dirents) {
            const n = d.name;
            const abs = path.join(dir, n);
            if (isTempName(n)) {
                skipped.push({ path: abs, reason: 'system file (skipped like a direct upload)' });
                continue;
            }
            // eslint-disable-next-line no-await-in-loop
            const kind = await entryKind(dir, d);
            if (kind === 'error') {
                skipped.push({ path: abs, reason: 'stat failed (broken link or no access)' });
                continue;
            }
            if (kind !== 'dir' && kind !== 'file') continue;
            const isDir = kind === 'dir';
            const jcrName = isDir ? cleanFolderName(n) : cleanAssetName(n);
            if (!jcrName || jcrName === '.' || jcrName === '..' || XML_INVALID_CHARS.test(jcrName)) {
                skipped.push({ path: abs, reason: `cannot map to a valid JCR name ("${jcrName}")` });
                continue;
            }
            // FileVault reads "<x>.dir/" as the extended serialization of sibling node "<x>"; an asset node
            // literally named "*.dir" would be misread on install. Folder names cannot end in ".dir" (the
            // folder cleaning turns "." into "-").
            if (!isDir && jcrName.endsWith('.dir')) {
                skipped.push({ path: abs, reason: 'asset name ending in ".dir" collides with FileVault .dir syntax' });
                continue;
            }
            if (used.has(jcrName)) {
                skipped.push({ path: abs, reason: `name collision after cleaning: "${jcrName}" already used in ${jcrPath}` });
                continue;
            }
            used.add(jcrName);
            items.push({ n, abs, isDir, jcrName });
        }

        const pending = new Map();
        let next = 0;
        const fill = () => {
            while (pending.size < PREFETCH && next < items.length) {
                const it = items[next];
                if (!it.isDir) pending.set(next, loadFile(it.abs));
                next++;
            }
        };
        for (let i = 0; i < items.length; i++) {
            fill();
            const it = items[i];
            const childZip = `${zipDir}/${toPlatformName(it.jcrName)}`;
            if (it.isDir) {
                // eslint-disable-next-line no-await-in-loop
                await addFolder(childZip, it.n, now);
                folders++;
                // eslint-disable-next-line no-await-in-loop
                await walk(it.abs, childZip, `${jcrPath}/${it.jcrName}`);
            } else {
                // eslint-disable-next-line no-await-in-loop
                const loaded = await pending.get(i);
                pending.delete(i);
                if (loaded.error) {
                    skipped.push({ path: it.abs, reason: `read failed: ${loaded.error.message}` });
                    continue;
                }
                // eslint-disable-next-line no-await-in-loop
                await addAsset(it.abs, loaded, childZip, it.n);
            }
        }
    };

    try {
        await metaInf();
        await zip.addDirectory('jcr_root/', now);
        // Ancestors: /content and /content/dam always exist and are left out. Intermediate DAM folders
        // below /content/dam (e.g. /content/dam/en) are outside the filter, so FileVault never modifies
        // them when they exist; if they are missing they are created from this .content.xml as
        // sling:OrderedFolder (instead of FileVault's default nt:folder, which DAM does not expect).
        let zipDir = 'jcr_root';
        for (let i = 0; i < rootSegs.length; i++) {
            zipDir += `/${toPlatformName(rootSegs[i])}`;
            if (i < 2) await zip.addDirectory(`${zipDir}/`, now);
            else await addFolder(zipDir, rootSegs[i], now);
        }
        if (skipTopFolder) {
            await walk(srcAbs, zipDir, root);
        } else {
            const topZip = `${zipDir}/${toPlatformName(topName)}`;
            await addFolder(topZip, topRaw, now);
            folders++;
            await walk(srcAbs, topZip, `${root}/${topName}`);
        }
        const res = await zip.finish();
        progress(true);
        return {
            outFile,
            files,
            folders,
            bytes,
            filterRoot: filterRoots.length === 1 ? filterRoots[0] : filterRoots,
            filterRoots,
            zipBytes: res.bytes,
            entries: res.entries,
            zip64: res.zip64,
            skipped,
        };
    } catch (e) {
        await zip.abort();
        throw e;
    }
}

module.exports = {
    buildPackage,
    // exported for tests / reuse
    cleanFolderName,
    cleanAssetName,
    isTempName,
    toPlatformName,
    mimeTypeFor,
    docViewString,
};
