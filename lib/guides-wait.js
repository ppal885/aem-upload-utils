/**
 * Waits until AEM Guides has processed DITA files (guides:assetState set and not PROCESSING). Used before maps
 * are sent to AEM: a map Guides processes before the files it links to have their GUIDs gets GUIDs no file has
 * (see lib/dita-order.js). Never throws: the outcome says how the wait ended.
 */
const http = require('http');
const https = require('https');
const { aemAuthorization } = require('./aem-auth');

const DITA_FILE = /\.(dita|ditamap|bookmap|xml)$/i;
const QUERY_TIMEOUT_MS = 2 * 60 * 1000;

/** Guides state of every asset under root: Map(path -> { state, uuid }). Rejects on any HTTP or network error. */
function guidesStates({ baseUrl, username, password, agent }, root) {
    const q = new URLSearchParams({
        path: root, type: 'dam:Asset', 'p.limit': '-1', 'p.hits': 'selective',
        'p.properties': 'jcr:path jcr:content/fmUuid jcr:content/guides:assetState',
    });
    const target = new URL(`${String(baseUrl).replace(/\/+$/, '')}/bin/querybuilder.json?${q}`);
    const client = target.protocol === 'https:' ? https : http;
    const options = { method: 'GET', timeout: QUERY_TIMEOUT_MS, headers: { Authorization: aemAuthorization(username, password), Accept: 'application/json' } };
    if (agent) options.agent = agent;
    return new Promise((resolve, reject) => {
        const req = client.request(target, options, (res) => {
            const chunks = [];
            res.on('data', (c) => chunks.push(c));
            res.on('error', reject);
            res.on('end', () => {
                if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
                try {
                    const out = new Map();
                    for (const h of JSON.parse(Buffer.concat(chunks).toString('utf8')).hits || []) {
                        const c = h['jcr:content'] || {};
                        if (typeof h['jcr:path'] === 'string') {
                            out.set(h['jcr:path'].normalize('NFC'), { state: String(c['guides:assetState'] || '').toUpperCase(), uuid: c.fmUuid || null });
                        }
                    }
                    resolve(out);
                } catch (e) { reject(e); }
            });
        });
        req.on('timeout', () => req.destroy(new Error('timed out')));
        req.on('error', reject);
        req.end();
    });
}

/**
 * server: { baseUrl, username, password, agent? }. roots: folders to list. targets: AEM paths to wait for (DITA
 * files, and other files such as images that DITA files link to), or null for every DITA file under the roots. Resolves to { outcome, pending, secs, message } where outcome is
 *   done       every target is processed
 *   no-guides  no target shows any Guides state after graceMs (Guides does not process these folders)
 *   timeout    maxMs went by; pending lists what was not processed
 *   error      AEM could not be asked (message)
 *   cancelled  isCancelled() returned true
 */
async function waitForGuides({ server, roots, targets, graceMs, maxMs, pollMs, isCancelled = () => false, onWaiting }) {
    const started = Date.now();
    const secs = () => Math.round((Date.now() - started) / 1000);
    let pending = targets ? targets.map((t) => String(t).normalize('NFC')) : null;
    if (pending && !pending.length) return { outcome: 'done', pending: [], secs: 0 };
    let seen = false;
    // Other files (images, ...) get a Guides state only on a UUID-based server: when none of them shows one after
    // graceMs they are not waited for
    let otherSeen = false;
    let lastReport = started;
    for (;;) {
        if (isCancelled()) return { outcome: 'cancelled', pending: pending || [], secs: secs() };
        const states = new Map();
        try {
            for (const root of roots) {
                // eslint-disable-next-line no-await-in-loop
                for (const [k, v] of await guidesStates(server, root)) states.set(k, v);
            }
        } catch (e) {
            return { outcome: 'error', pending: pending || [], secs: secs(), message: e.message };
        }
        if (!pending) pending = [...states.keys()].filter((t) => DITA_FILE.test(t));
        pending = pending.filter((t) => {
            const s = states.get(t);
            if (s && (s.state || s.uuid)) { seen = true; if (!DITA_FILE.test(t)) otherSeen = true; }
            return !(s && s.state && s.state !== 'PROCESSING');
        });
        if (!otherSeen && Date.now() - started >= graceMs) pending = pending.filter((t) => DITA_FILE.test(t));
        if (!pending.length) return { outcome: 'done', pending, secs: secs() };
        if (!seen && Date.now() - started >= graceMs) return { outcome: 'no-guides', pending, secs: secs() };
        if (Date.now() - started >= maxMs) return { outcome: 'timeout', pending, secs: secs() };
        if (onWaiting && Date.now() - lastReport >= 60000) {
            lastReport = Date.now();
            onWaiting({ pending: pending.length, secs: secs() });
        }
        // eslint-disable-next-line no-await-in-loop
        await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
}

module.exports = { waitForGuides, guidesStates, DITA_FILE };
