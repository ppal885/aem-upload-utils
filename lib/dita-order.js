/**
 * Upload order for DITA content. On a UUID-based Guides server, Guides rewrites each file's links (href, conref,
 * conrefend: maps to submaps and topics, topics to topics and images) to the GUIDs (fmUuid) of the files they point
 * at when it processes the file. A file processed before the files it links to have their GUIDs gets GUIDs that no
 * asset ends up with: a parent map uploaded together with its submaps pointed at 10 GUIDs that did not exist
 * anywhere in the folder.
 *
 * planUpload orders an upload by those links: files that link to nothing in the upload first (images, topics
 * without links), then each file one wave after the last of the files it links to, so maps come last. The caller
 * uploads one wave at a time and lets Guides finish the previous one before starting the next.
 *
 * Some links cannot be ordered: files that link to each other (a.dita and b.dita xref each other) and chains
 * deeper than maxWaves. Those files are listed in `again`: once everything is processed the caller sends them a
 * second time; then every file they link to has its GUID. A second upload updates the asset in place, so the
 * asset keeps its own GUID.
 */
const fsp = require('fs').promises;
const path = require('path');
const AemNames = require('../public/aem-names');

const MAP_FILE = /\.(ditamap|bookmap)$/i;
// Files whose links Guides rewrites
const LINKING_FILE = /\.(dita|ditamap|bookmap|xml)$/i;
const MAX_LINKING_BYTES = 20 * 1024 * 1024;
const READ_WIDTH = 16;
const MAX_WAVES = Math.max(2, parseInt(process.env.DITA_MAX_WAVES, 10) || 10);

const keyOf = (p) => path.resolve(String(p)).normalize('NFC');

function isMapFile(name) {
    return MAP_FILE.test(String(name || ''));
}

/** Keys (keyOf) of the files of the upload that the file at `file` links to. */
async function linksOf(file, byKey) {
    let text;
    try {
        const st = await fsp.stat(file);
        if (st.size > MAX_LINKING_BYTES) return [];
        text = await fsp.readFile(file, 'utf8');
    } catch (_) {
        return [];
    }
    const self = keyOf(file);
    const dir = path.dirname(file);
    const out = new Set();
    for (const { ref } of AemNames.ditaFileRefs(text)) {
        const target = keyOf(path.resolve(dir, ref));
        if (target !== self && byKey.has(target)) out.add(target);
    }
    return [...out];
}

/**
 * Strongly connected components of the link graph (iterative Tarjan, so long chains do not overflow the stack),
 * in the order Tarjan finds them: every component comes after the components it links to.
 */
function components(keys, deps) {
    const index = new Map();
    const low = new Map();
    const onStack = new Set();
    const stack = [];
    const out = [];
    let counter = 0;
    for (const root of keys) {
        if (index.has(root)) continue;
        const work = [[root, 0]];
        index.set(root, counter); low.set(root, counter); counter++;
        stack.push(root); onStack.add(root);
        while (work.length) {
            const frame = work[work.length - 1];
            const [v, i] = frame;
            const ds = deps.get(v) || [];
            if (i < ds.length) {
                frame[1]++;
                const w = ds[i];
                if (!index.has(w)) {
                    index.set(w, counter); low.set(w, counter); counter++;
                    stack.push(w); onStack.add(w);
                    work.push([w, 0]);
                } else if (onStack.has(w)) {
                    low.set(v, Math.min(low.get(v), index.get(w)));
                }
                continue;
            }
            work.pop();
            if (work.length) {
                const parent = work[work.length - 1][0];
                low.set(parent, Math.min(low.get(parent), low.get(v)));
            }
            if (low.get(v) === index.get(v)) {
                const comp = [];
                let w;
                do { w = stack.pop(); onStack.delete(w); comp.push(w); } while (w !== v);
                out.push(comp);
            }
        }
    }
    return out;
}

/**
 * items: the files of one upload; localPathOf(item) gives each one's local path.
 * Resolves to { waves: [[item, ...], ...], again: [item, ...], linking: Set(item), linked: Set(item) }: the waves
 * in upload order (none empty; inside a wave the order given), the files to send a second time, the files that
 * link to another file of the upload and the files something links to. An upload without links is one wave.
 */
async function planUpload(items, localPathOf = (i) => i.filePath, { maxWaves = MAX_WAVES } = {}) {
    if (!items.length) return { waves: [], again: [], linking: new Set(), linked: new Set() };
    const byKey = new Map();
    for (const it of items) byKey.set(keyOf(localPathOf(it)), it);
    const keys = [...byKey.keys()];
    const deps = new Map();
    const readers = keys.filter((k) => LINKING_FILE.test(k));
    let next = 0;
    await Promise.all(Array.from({ length: Math.min(READ_WIDTH, readers.length) }, async () => {
        while (next < readers.length) {
            const k = readers[next++];
            const d = await linksOf(localPathOf(byKey.get(k)), byKey);
            if (d.length) deps.set(k, d);
        }
    }));
    if (!deps.size) return { waves: [items], again: [], linking: new Set(), linked: new Set() };

    // Wave of a file = one more than the highest wave of the files it links to; files linking to each other share
    // a wave. Deeper than maxWaves goes into the last wave.
    const wave = new Map();
    for (const comp of components(keys, deps)) {
        const inComp = new Set(comp);
        let w = 0;
        for (const k of comp) for (const d of deps.get(k) || []) if (!inComp.has(d)) w = Math.max(w, wave.get(d) + 1);
        w = Math.min(w, maxWaves - 1);
        for (const k of comp) wave.set(k, w);
    }
    const waves = [];
    const again = [];
    const linking = new Set();
    const linkedKeys = new Set([...deps.values()].flat());
    const linked = new Set();
    for (const it of items) {
        const k = keyOf(localPathOf(it));
        const w = wave.get(k);
        (waves[w] = waves[w] || []).push(it);
        const ds = deps.get(k) || [];
        if (ds.length) linking.add(it);
        if (linkedKeys.has(k)) linked.add(it);
        // A file is processed after what it links to only when that is in an earlier wave
        if (ds.some((d) => wave.get(d) >= w)) again.push(it);
    }
    return { waves: waves.filter((x) => x && x.length), again, linking, linked };
}

module.exports = { planUpload, isMapFile, LINKING_FILE };
