/**
 * Upload order for DITA maps. On a UUID-based Guides server, Guides rewrites each map's hrefs to the GUIDs
 * (fmUuid) of the files they point at when it processes the map. A map processed before the files it links to
 * have their GUIDs gets GUIDs that no asset ends up with: a parent map uploaded together with its submaps
 * pointed at 10 GUIDs that did not exist anywhere in the folder.
 *
 * uploadWaves splits an upload into waves: every file that is not a map first, then the maps that link to no
 * other map of the upload, then the maps that link to those, and so on up to the top map. The caller uploads
 * one wave at a time and lets Guides finish the previous one before starting the next.
 */
const fsp = require('fs').promises;
const path = require('path');
const AemNames = require('../public/aem-names');

const MAP_FILE = /\.(ditamap|bookmap)$/i;
const MAX_MAP_BYTES = 20 * 1024 * 1024;
const READ_WIDTH = 16;

const keyOf = (p) => path.resolve(String(p)).normalize('NFC');

function isMapFile(name) {
    return MAP_FILE.test(String(name || ''));
}

/** Local paths of the other maps of `mapPaths` that the map at `file` links to (href / conref). */
async function linkedMaps(file, byKey) {
    let text;
    try {
        const st = await fsp.stat(file);
        if (st.size > MAX_MAP_BYTES) return [];
        text = await fsp.readFile(file, 'utf8');
    } catch (_) {
        return [];
    }
    const self = keyOf(file);
    const dir = path.dirname(file);
    const out = new Set();
    for (const { ref } of AemNames.ditaFileRefs(text)) {
        if (!isMapFile(ref)) continue;
        const target = keyOf(path.resolve(dir, ref));
        if (target !== self && byKey.has(target)) out.add(target);
    }
    return [...out];
}

/**
 * items: the files of one upload; localPathOf(item) gives each one's local path.
 * Resolves to [[item, ...], ...]: the waves in upload order, none empty. Without maps (or with maps that link
 * to no other map) the order inside a wave is the order given, and an upload without maps is one wave.
 */
async function uploadWaves(items, localPathOf = (i) => i.filePath) {
    const maps = items.filter((i) => isMapFile(localPathOf(i)));
    if (!maps.length) return items.length ? [items] : [];
    const others = items.filter((i) => !isMapFile(localPathOf(i)));

    const byKey = new Map(maps.map((m) => [keyOf(localPathOf(m)), m]));
    const deps = new Map();
    const keys = [...byKey.keys()];
    let next = 0;
    await Promise.all(Array.from({ length: Math.min(READ_WIDTH, keys.length) }, async () => {
        while (next < keys.length) {
            const k = keys[next++];
            deps.set(k, await linkedMaps(localPathOf(byKey.get(k)), byKey));
        }
    }));

    // Level of a map = 1 + the highest level of the maps it links to (0 for none). A cycle (a.ditamap links to
    // b.ditamap and back) cannot be ordered: the link that closes it is not counted.
    const level = new Map();
    const visiting = new Set();
    const levelOf = (k) => {
        if (level.has(k)) return level.get(k);
        if (visiting.has(k)) return -1;
        visiting.add(k);
        let l = 0;
        for (const d of deps.get(k) || []) l = Math.max(l, levelOf(d) + 1);
        visiting.delete(k);
        level.set(k, l);
        return l;
    };
    const mapWaves = [];
    for (const m of maps) {
        const l = levelOf(keyOf(localPathOf(m)));
        (mapWaves[l] = mapWaves[l] || []).push(m);
    }
    return [others, ...mapWaves].filter((w) => w && w.length);
}

module.exports = { uploadWaves, isMapFile };
