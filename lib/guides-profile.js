/**
 * Creates a new AEM Guides DITA profile (Tools > Guides > DITA Profiles) for a folder, the way "Create Profile" does
 * in AEM: the target's Global/Default profile is the starting point, with the content folder in Assigned Paths, the
 * uploaded catalog.xml in the schema catalogs and, optionally, a custom DITA-OT zip. It never edits or deletes an
 * existing profile. planProfile only reads; createProfile re-plans, writes with POST /bin/fmdita/profiles and then
 * reads the profiles back to confirm it was saved without errors.
 */
const crypto = require('crypto');
const { profileFor } = require('./guides-setup');
const AemNames = require('../public/aem-names');

const DEFAULT_ID = 'b1aad0a7-9079-e56c-1ed8-6fcababe8166';
const NAME = /^[A-Za-z0-9][A-Za-z0-9 ._()-]{0,79}$/;
const TIMEOUT_MS = 2 * 60 * 1000;

const auth = (s) => ({ Authorization: 'Basic ' + Buffer.from(`${s.username}:${s.password}`).toString('base64') });
const enc = (p) => p.split('/').map(encodeURIComponent).join('/');

async function readProfiles(server) {
    const r = await fetch(`${server.baseUrl}/bin/fmdita/profiles?operation=getallprofiles`, { headers: auth(server), signal: AbortSignal.timeout(TIMEOUT_MS) });
    if (r.status === 401) throw new Error('AEM did not accept the username or password.');
    if (!r.ok) throw new Error(`DITA profiles could not be read (HTTP ${r.status})`);
    const list = await r.json();
    return Array.isArray(list) ? list : [];
}

async function exists(server, path) {
    const r = await fetch(`${server.baseUrl}${enc(path)}.json`, { headers: auth(server), signal: AbortSignal.timeout(TIMEOUT_MS) });
    return r.ok;
}

const cleanPath = (p) => '/' + String(p || '').trim().replace(/\\/g, '/').replace(/^\/+|\/+$/g, '');
const ORIGINAL = '/jcr:content/renditions/original';
const dirOf = (p) => p.slice(0, p.lastIndexOf('/'));
const inside = (p, dir) => p === dir || p.startsWith(dir + '/');

async function nodeJson(server, path) {
    const r = await fetch(`${server.baseUrl}${enc(path)}.json`, { headers: auth(server), signal: AbortSignal.timeout(TIMEOUT_MS) });
    return r.ok ? r.json() : null;
}

// The binary of a DAM asset is its original rendition; an nt:file (like /libs/.../DITA-OT.zip) is read directly
async function binaryPath(server, path) {
    if (path.endsWith(ORIGINAL)) return { node: path, asset: path.slice(0, -ORIGINAL.length) };
    const content = await nodeJson(server, `${path}/jcr:content`);
    if (!content) return null;
    return content['jcr:primaryType'] === 'dam:AssetContent' ? { node: path + ORIGINAL, asset: path } : { node: path, asset: null };
}

async function readRange(server, path, start, end) {
    const r = await fetch(`${server.baseUrl}${enc(path)}`, { headers: { ...auth(server), Range: `bytes=${start}-${end - 1}` }, signal: AbortSignal.timeout(TIMEOUT_MS) });
    if (r.status !== 206) { try { await r.body?.cancel(); } catch (_) { /* closed */ } throw new Error(`no range support (HTTP ${r.status})`); }
    return { bytes: new Uint8Array(await r.arrayBuffer()), total: Number((/\/(\d+)$/.exec(r.headers.get('content-range') || '') || [])[1]) };
}

/** Entry names of a zip in AEM, reading only its table of contents (HTTP Range); null when the server cannot do ranges. */
async function remoteZipEntries(server, path) {
    let total;
    try {
        ({ total } = await readRange(server, path, 0, 1));
    } catch (_) { return null; }
    if (!total) return null;
    return AemNames.listZipEntries(total, async (start, end) => (await readRange(server, path, start, end)).bytes);
}

async function readText(server, path) {
    const b = await binaryPath(server, path);
    if (!b) return null;
    const r = await fetch(`${server.baseUrl}${enc(b.node)}`, { headers: auth(server), signal: AbortSignal.timeout(TIMEOUT_MS) });
    return r.ok ? r.text() : null;
}

/** What creating the profile would do; { ok, problems, notes, profile }. */
async function planProfile(server, { name, paths, catalogs, ditaotZip }) {
    const problems = [];
    const notes = [];
    const n = String(name || '').trim();
    const wantPaths = [...new Set((paths || []).map(cleanPath).filter((p) => p !== '/'))];
    const wantCatalogs = [...new Set((catalogs || []).map(cleanPath).filter((p) => p !== '/'))];
    const zip = ditaotZip ? cleanPath(ditaotZip) : '';
    if (!NAME.test(n)) problems.push('Profile name: letters, numbers, space, dot, dash, underscore or brackets.');
    if (!wantPaths.length) problems.push('Enter the folder the topics go to (Assigned Paths).');
    if (wantPaths.some((p) => !/^\/content\/dam\/[^/]/.test(p))) problems.push('Assigned Paths must be folders inside /content/dam.');
    if (!wantCatalogs.length) problems.push('Enter the catalog.xml of the DTD folder.');
    if (wantCatalogs.some((c) => !/\.xml$/i.test(c))) problems.push('A catalog is an .xml file, for example /content/dam/my-dtd/catalog.xml.');
    if (problems.length) return { ok: false, problems, notes };

    const profiles = await readProfiles(server);
    const base = profiles.find((p) => p.id === DEFAULT_ID) || profiles.find((p) => (p.paths || []).includes('/content/dam'));
    if (!base) return { ok: false, problems: ['The Global/Default DITA profile was not found on this server.'], notes };
    if (profiles.some((p) => String(p.name).toLowerCase() === n.toLowerCase())) problems.push(`A DITA profile named "${n}" already exists. Choose another name, or edit that profile in AEM.`);

    // Catalogs: Guides copies each catalog's folder (with subfolders) to disk and resolves the catalog there
    const custom = wantCatalogs.filter((c) => !c.startsWith('/libs/'));
    for (const c of wantCatalogs) {
        const node = await nodeJson(server, c);
        if (!node) { problems.push(`${c} is not on this server yet. Upload the DTD folder first.`); continue; }
        if (!['dam:Asset', 'nt:file'].includes(node['jcr:primaryType'])) problems.push(`${c} is a folder; choose the catalog file itself (for example ${c}/catalog.xml).`);
    }
    for (let i = 0; i < custom.length; i++) {
        for (let j = i + 1; j < custom.length; j++) {
            const a = dirOf(custom[i]);
            const b = dirOf(custom[j]);
            if (inside(a, b) || inside(b, a)) problems.push(`${custom[i]} and ${custom[j]} are in the same or nested folders; Guides copies each catalog's folder and the second copy fails. Keep one catalog per folder (link others with nextCatalog).`);
        }
    }
    for (const c of custom) {
        const text = await readText(server, c).catch(() => null);
        if (text == null) continue;
        for (const ref of AemNames.catalogEscapes(text)) {
            const parts = dirOf(c).split('/');
            for (const seg of ref.split('#')[0].split('/')) { if (seg === '..') parts.pop(); else if (seg && seg !== '.') parts.push(seg); }
            const target = parts.join('/');
            if (!custom.some((o) => inside(target, dirOf(o)))) {
                problems.push(`${c} refers to "${ref}" (${target}), outside its folder. Guides copies only the catalog's folder, so that file would not be found: move it into ${dirOf(c)} or add the catalog of its folder to this profile.`);
            }
        }
    }

    // DITA-OT zip: Guides reads <path>/jcr:content/jcr:data, which only an nt:file has; a DAM asset needs its original rendition
    let zipPath = zip;
    if (zip) {
        const b = await binaryPath(server, zip);
        if (!b) problems.push(`The DITA-OT zip ${zip} is not on this server yet.`);
        else {
            if (b.asset && !zip.endsWith(ORIGINAL)) {
                zipPath = b.node;
                notes.push(`DITA-OT zip: ${zip} is a DAM asset, so the profile uses ${zipPath} (Guides cannot read the zip from the asset path itself).`);
            }
            const names = await remoteZipEntries(server, b.node).catch(() => null);
            if (!names) notes.push('The contents of the DITA-OT zip could not be checked here; make sure its top folder is named exactly "DITA-OT".');
            else {
                const z = AemNames.checkDitaOtZip(names);
                problems.push(...z.problems.map((x) => 'DITA-OT zip: ' + x));
                notes.push(...z.warnings.map((x) => 'DITA-OT zip: ' + x));
            }
        }
    }
    for (const p of wantPaths) if (!(await exists(server, p))) notes.push(`${p} does not exist yet; that is fine, upload the topics there after creating the profile.`);

    for (const p of wantPaths) {
        const owner = profiles.find((x) => (x.paths || []).map((y) => String(y).replace(/\/+$/, '')).includes(p));
        if (owner) notes.push(`${p} is in profile "${owner.name}" now and moves to the new profile (a folder belongs to one profile only).`);
        else {
            const current = profileFor(profiles, p);
            notes.push(`${p} uses "${current ? current.name : 'no profile'}" now; after this it uses "${n}".`);
        }
    }

    const builtIn = ((base.schema && base.schema.catalogs) || []).filter((c) => !wantCatalogs.includes(c));
    const profile = JSON.parse(JSON.stringify(base));
    Object.assign(profile, { id: crypto.randomUUID(), name: n, paths: wantPaths, reuse: false, errors: '' });
    profile.schema = { ...(profile.schema || {}), catalogs: [...builtIn, ...wantCatalogs], errors: '', isBuilt: false };
    delete profile.schema.folder;
    profile.ditaot = { ...(profile.ditaot || {}), errors: '', isBuilt: false };
    delete profile.ditaot.extractPath;
    if (zipPath) Object.assign(profile.ditaot, { zip: zipPath, overwrite: true });
    // non-default profiles carry an empty Node.js context (Guides fails to save one without it, HTTP 432)
    profile.nodejs = { timeout: 0, errors: '', dependencyCount: -1, isBuilt: false };
    delete profile.lastModified;
    return { ok: problems.length === 0, problems, notes, profile: { id: profile.id, name: n, paths: wantPaths, catalogs: profile.schema.catalogs, ditaotZip: profile.ditaot.zip }, full: profile };
}

/** Creates the profile after planning again; resolves to { ok, profile, errors }. */
async function createProfile(server, input) {
    const plan = await planProfile(server, input);
    if (!plan.ok) return { ok: false, problems: plan.problems, notes: plan.notes };
    const r = await fetch(`${server.baseUrl}/bin/fmdita/profiles`, {
        method: 'POST', headers: auth(server), signal: AbortSignal.timeout(TIMEOUT_MS),
        body: new URLSearchParams({ operation: 'updateprofile', mode: 'create', data: JSON.stringify(plan.full) }),
    });
    if (!r.ok) return { ok: false, problems: [`AEM did not save the profile (HTTP ${r.status}).`], notes: plan.notes };
    const saved = (await readProfiles(server)).find((p) => p.id === plan.full.id);
    if (!saved) return { ok: false, problems: ['AEM answered, but the profile is not in the list afterwards.'], notes: plan.notes };
    const errors = [saved.errors, saved.schema && saved.schema.errors, saved.ditaot && saved.ditaot.errors].filter(Boolean);
    return { ok: errors.length === 0, profile: plan.profile, errors, notes: plan.notes };
}

module.exports = { planProfile, createProfile };
