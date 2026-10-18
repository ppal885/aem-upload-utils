/**
 * Audit log of Copy from AEM: who copied which folder from which server, and where that content went
 * (uploaded to an allowed server, built into a package, package installed or downloaded).
 * One JSON object per line in COPY_LOG_FILE (default ./copy-log.jsonl), readable by the server's user only.
 * Never holds passwords or file contents. When the file passes MAX_BYTES it is renamed to .1 (one old file kept).
 */
const fs = require('fs');
const path = require('path');
const csv = require('./csv');

const COPY_LOG_FILE = path.resolve(process.env.COPY_LOG_FILE || path.join(__dirname, '..', 'copy-log.jsonl'));
const MAX_BYTES = 10 * 1024 * 1024;

function rotateIfLarge() {
    try {
        if (fs.statSync(COPY_LOG_FILE).size > MAX_BYTES) fs.renameSync(COPY_LOG_FILE, COPY_LOG_FILE + '.1');
    } catch (_) { /* no file yet */ }
}

/** event: copy-start | copy-end | copy-upload | package-built | package-install | package-download */
function record(event, fields, log) {
    const entry = { at: new Date().toISOString(), event, ...fields };
    try {
        rotateIfLarge();
        fs.appendFileSync(COPY_LOG_FILE, JSON.stringify(entry) + '\n', { mode: 0o600 });
    } catch (e) {
        if (log) log.error(`Copy log could not be written (${event} ${fields.copyId || ''}): ${e.message}`);
    }
    return entry;
}

function readFile(file) {
    let text = '';
    try { text = fs.readFileSync(file, 'utf8'); } catch (_) { return []; }
    const out = [];
    for (const line of text.split('\n')) {
        if (!line.trim()) continue;
        try { out.push(JSON.parse(line)); } catch (_) { /* a line cut by a crash */ }
    }
    return out;
}

/** Newest first, from the current and the previous file. */
function read(limit = 1000) {
    const all = readFile(COPY_LOG_FILE + '.1').concat(readFile(COPY_LOG_FILE));
    return all.reverse().slice(0, limit);
}

const CSV_COLUMNS = ['at', 'event', 'copyId', 'by', 'sourceServer', 'sourceFolder', 'sourceUser', 'targetServer', 'targetFolder',
    'package', 'state', 'files', 'bytes', 'verified', 'failed', 'error', 'relatedTo'];

function toCsv(entries) {
    return csv.toCsv(CSV_COLUMNS, entries);
}

module.exports = { COPY_LOG_FILE, CSV_COLUMNS, record, read, toCsv };
