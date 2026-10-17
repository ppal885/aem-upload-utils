/**
 * Guides references in copied DITA files. On UUID-based Guides servers the stored files point at each other by
 * GUID (href="GUID-<uuid>.dita"), and GUIDs are given out per server: copied as they are, every link would break
 * on the target. rewriteGuidRefs turns each GUID back into a relative path (what the source file looked like
 * before Guides converted it); the target's Guides converts those paths to its own GUIDs when it processes them.
 */
const path = require('path');

const XML_EXT = /\.(dita|ditamap|xml|ditaval|bookmap|subjectscheme)$/i;
const GUID = /GUID-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/;
// href / conref / conrefend / data (object) / keyref is key based and left alone
const REF_ATTR = /(\s(?:href|conref|conrefend|data)\s*=\s*)(["'])([^"']*)\2/g;

function isXmlFile(name) {
    return XML_EXT.test(name);
}

function encodeRef(p) {
    return p.split('/').map((s) => s.replace(/[ %#?"'<>]/g, (c) => '%' + c.charCodeAt(0).toString(16).toUpperCase())).join('/');
}

/**
 * Rewrites GUID references in one file. fromPath is the file's AEM path; resolve(guid) returns the AEM path of
 * the asset with that GUID or null. Returns { text, changed, unresolved: Set<guid>, targets: Set<path> }.
 */
function rewriteGuidRefs(text, fromPath, resolve) {
    const unresolved = new Set();
    const targets = new Set();
    let changed = 0;
    const fromDir = path.posix.dirname(fromPath);
    const out = text.replace(REF_ATTR, (whole, pre, q, value) => {
        const m = GUID.exec(value);
        if (!m || m.index !== 0) return whole;
        const guid = m[0];
        const target = resolve(guid);
        if (!target) { unresolved.add(guid); return whole; }
        targets.add(target);
        const rest = value.slice(guid.length).replace(/^\.[A-Za-z0-9]+/, '');
        let rel = path.posix.relative(fromDir, target) || path.posix.basename(target);
        if (!rel.startsWith('.')) rel = rel || '.';
        changed++;
        return `${pre}${q}${encodeRef(rel)}${rest}${q}`;
    });
    return { text: out, changed, unresolved, targets };
}

/** The DOCTYPE public id of a DITA file ("-//OASIS//DTD DITA Topic//EN"), or null. */
function doctypeOf(text) {
    const m = /<!DOCTYPE\s+([\w:.-]+)\s+PUBLIC\s+["']([^"']+)["']/.exec(text.slice(0, 4096));
    return m ? { root: m[1], publicId: m[2] } : null;
}

module.exports = { isXmlFile, rewriteGuidRefs, doctypeOf, GUID };
