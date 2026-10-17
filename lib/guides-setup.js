/**
 * Setup check for content copied between AEM Guides servers. Guides finds DTDs and DITA-OT through a DITA profile
 * (/var/dxml/profiles, read with GET /bin/fmdita/profiles?operation=getallprofiles) chosen by the longest matching
 * path, and the profile stores absolute paths: the content opens and publishes on the target only when a profile
 * there covers the same folder with catalogs that resolve the DOCTYPEs the files use. Only GET requests are sent.
 */
const LIBS = '/libs/';
const OASIS = /^-\/\/OASIS\/\//;
const MAX_CATALOG_DEPTH = 4;
const TIMEOUT_MS = 60 * 1000;

function headers(username, password) {
    return { Authorization: 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64') };
}

async function getProfiles(server) {
    const r = await fetch(`${server.baseUrl}/bin/fmdita/profiles?operation=getallprofiles`, {
        headers: headers(server.username, server.password), signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (!r.ok) throw new Error(`DITA profiles could not be read (HTTP ${r.status})`);
    const list = await r.json();
    return Array.isArray(list) ? list : [];
}

/** The profile Guides uses for path: the one whose paths[] has the longest prefix match. */
function profileFor(profiles, path) {
    let best = null;
    let bestLen = -1;
    for (const p of profiles) {
        for (const root of p.paths || []) {
            const r = String(root).replace(/\/+$/, '');
            if ((path === r || path.startsWith(r + '/')) && r.length > bestLen) { best = p; bestLen = r.length; }
        }
    }
    return best;
}

async function fetchText(server, path) {
    const url = path.startsWith('/content/dam/') ? `${path}/jcr:content/renditions/original` : path;
    const r = await fetch(server.baseUrl + url.split('/').map(encodeURIComponent).join('/'), {
        headers: headers(server.username, server.password), signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    return r.ok ? r.text() : null;
}

/** Public ids a catalog (and its nextCatalog chain) resolves: Set, plus whether every catalog could be read. */
async function catalogPublicIds(server, catalogPath, seen = new Set(), depth = 0) {
    const ids = new Set();
    if (seen.has(catalogPath) || depth > MAX_CATALOG_DEPTH) return { ids, readable: true };
    seen.add(catalogPath);
    const text = await fetchText(server, catalogPath);
    if (text == null) return { ids, readable: false };
    let readable = true;
    for (const m of text.matchAll(/<public\s[^>]*publicId\s*=\s*["']([^"']+)["']/g)) ids.add(m[1].trim());
    const dir = catalogPath.slice(0, catalogPath.lastIndexOf('/'));
    for (const m of text.matchAll(/<nextCatalog\s[^>]*catalog\s*=\s*["']([^"']+)["']/g)) {
        if (/^[a-z]+:/i.test(m[1])) continue;
        const next = normalizePath(m[1].startsWith('/') ? m[1] : `${dir}/${m[1]}`);
        const sub = await catalogPublicIds(server, next, seen, depth + 1);
        sub.ids.forEach((id) => ids.add(id));
        readable = readable && sub.readable;
    }
    return { ids, readable };
}

function normalizePath(p) {
    const out = [];
    for (const s of p.split('/')) {
        if (s === '..') out.pop(); else if (s && s !== '.') out.push(s);
    }
    return '/' + out.join('/');
}

async function exists(server, path) {
    const r = await fetch(`${server.baseUrl}${path.split('/').map(encodeURIComponent).join('/')}.json`, {
        headers: headers(server.username, server.password), signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    return r.ok;
}

function where(path, folder) {
    if (path.startsWith(LIBS)) return 'built-in';
    if (path === folder || path.startsWith(folder + '/')) return 'copied';
    return 'outside';
}

function summarizeProfile(p) {
    if (!p) return null;
    return {
        id: p.id, name: p.name, paths: p.paths || [],
        catalogs: (p.schema && p.schema.catalogs) || [],
        ditaotZip: (p.ditaot && p.ditaot.zip) || null,
        ditaotCatalogs: (p.ditaot && p.ditaot.catalogs) || [],
        errors: [p.errors, p.schema && p.schema.errors, p.ditaot && p.ditaot.errors].filter(Boolean),
    };
}

/**
 * source/target: { baseUrl, username, password }; folder: the copied source folder; targetFolder: where it lands;
 * doctypes: { publicId: fileCount } from the copied files; outside: [{ path, links }] links leaving the folder.
 * Returns { ready, steps: [string], source, target, doctypes: [...] }.
 */
async function checkSetup({ source, target, folder, targetFolder, doctypes = {}, outside = [], previous = null }) {
    const steps = [];
    const result = { source: null, target: null, doctypes: [], steps, ready: true };
    if (previous) {
        // "Check again" after the target was set up: the source side does not change (and its password is gone)
        result.source = previous.source;
        result.doctypes = previous.doctypes;
        result.sourceError = previous.sourceError;
    } else {
        await checkSource(result, source, folder, doctypes);
    }
    return finish(result, { target, folder, targetFolder, outside });
}

async function checkSource(result, source, folder, doctypes) {
    let srcProfile = null;
    try {
        srcProfile = profileFor(await getProfiles(source), folder);
    } catch (e) {
        result.sourceError = e.message;
    }
    const sp = summarizeProfile(srcProfile);
    result.source = sp && {
        ...sp,
        custom: sp.id !== 'b1aad0a7-9079-e56c-1ed8-6fcababe8166',
        items: [
            ...sp.catalogs.map((path) => ({ kind: 'DTD catalog', path, where: where(path, folder) })),
            ...(sp.ditaotZip ? [{ kind: 'DITA-OT', path: sp.ditaotZip, where: where(sp.ditaotZip, folder) }] : []),
            ...sp.ditaotCatalogs.map((path) => ({ kind: 'DITA-OT catalog', path, where: where(path, folder) })),
        ],
    };

    // Which catalog resolves each DOCTYPE the copied files use
    const sourceIds = new Map();
    for (const c of sp ? sp.catalogs : []) {
        if (c.startsWith(LIBS)) continue;
        const { ids } = await catalogPublicIds(source, c);
        ids.forEach((id) => { if (!sourceIds.has(id)) sourceIds.set(id, c); });
    }
    for (const [publicId, files] of Object.entries(doctypes).sort((a, b) => b[1] - a[1])) {
        result.doctypes.push({ publicId, files, standard: OASIS.test(publicId), catalog: OASIS.test(publicId) ? 'built-in' : sourceIds.get(publicId) || null });
    }
}

async function finish(result, { target, folder, targetFolder, outside }) {
    const { steps } = result;
    const sp = result.source;
    const custom = result.doctypes.filter((d) => !d.standard);

    const toCopy = result.source ? result.source.items.filter((i) => i.where === 'outside') : [];
    toCopy.forEach((i) => steps.push(`Copy ${i.path.replace(/\/[^/]+\.(xml|zip)$/i, '')} too: the ${i.kind} of this content lives there, outside ${folder}.`));
    if (outside.length) {
        const roots = [...new Set(outside.map((o) => o.path.split('/').slice(0, 4).join('/')))];
        steps.push(`${outside.length} file(s) outside ${folder} are linked from the copied content (for example ${outside[0].path}). Copy ${roots.join(', ')} to the same place too, or those links stay broken.`);
    }
    if (custom.some((d) => !d.catalog)) {
        result.ready = false;
        steps.push(`No DTD catalog on the source resolves ${custom.filter((d) => !d.catalog).map((d) => `"${d.publicId}"`).join(', ')}; these files will not open on the target either until a catalog for them is set up.`);
    }

    if (target) {
        let tp = null;
        try {
            tp = summarizeProfile(profileFor(await getProfiles(target), targetFolder));
        } catch (e) {
            result.targetError = e.message;
        }
        result.target = tp && { ...tp };
        if (tp) {
            const targetIds = new Set();
            const missingCatalogs = [];
            for (const c of tp.catalogs) {
                if (c.startsWith(LIBS)) continue;
                const { ids, readable } = await catalogPublicIds(target, c);
                if (!readable) missingCatalogs.push(c);
                ids.forEach((id) => targetIds.add(id));
            }
            result.target.missingCatalogs = missingCatalogs;
            const unresolved = custom.filter((d) => !targetIds.has(d.publicId));
            result.target.unresolvedDoctypes = unresolved.map((d) => d.publicId);
            if (unresolved.length) {
                result.ready = false;
                const catalogs = sp ? sp.catalogs.filter((c) => !c.startsWith(LIBS)) : [];
                steps.push(`On the target, the DITA profile for ${targetFolder} is "${tp.name}" and it cannot resolve ${unresolved.map((d) => `"${d.publicId}"`).join(', ')}. `
                    + `Before uploading the topics: copy the DTD folder, then in Tools > Guides > DITA Profiles create or edit a profile whose paths include ${targetFolder}`
                    + (catalogs.length ? ` and whose DTD catalogs include ${catalogs.join(', ')}` : '') + ', and save it.');
            }
            if (missingCatalogs.length) steps.push(`The target profile "${tp.name}" lists catalogs that do not exist there: ${missingCatalogs.join(', ')}.`);
            if (sp && sp.ditaotZip && tp.ditaotZip !== sp.ditaotZip) {
                steps.push(`Publishing: the source uses DITA-OT ${sp.ditaotZip}, the target profile uses ${tp.ditaotZip || 'none'}. Set the same DITA-OT zip in the target profile (and re-save it after uploading the zip) for the same output.`);
            }
            if (sp && sp.ditaotZip && !sp.ditaotZip.startsWith(LIBS) && !(await exists(target, sp.ditaotZip))) {
                steps.push(`The DITA-OT zip ${sp.ditaotZip} is not on the target yet.`);
            }
        }
    }
    if (custom.length || (sp && result.source.custom)) {
        steps.push('Output presets stored on maps are not copied; set them up again on the target if you publish from there.');
    }
    return result;
}

module.exports = { checkSetup, profileFor, catalogPublicIds };
