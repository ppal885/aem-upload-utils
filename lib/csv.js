/** CSV for spreadsheets: CRLF lines, quoted where needed, and no cell a spreadsheet would run as a formula. */
function cell(v) {
    let s = v == null ? '' : String(v);
    if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;
    return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

/** columns: [name] or [[name, (row) => value]] */
function toCsv(columns, rows) {
    const cols = columns.map((c) => (Array.isArray(c) ? c : [c, (r) => r[c]]));
    return [cols.map(([n]) => cell(n)).join(',')]
        .concat(rows.map((r) => cols.map(([, get]) => cell(get(r))).join(','))).join('\r\n') + '\r\n';
}

/** Sends rows as a CSV download (with a BOM so Excel reads UTF-8). */
function send(res, filename, columns, rows) {
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Cache-Control', 'no-store');
    res.send('﻿' + toCsv(columns, rows));
}

module.exports = { cell, toCsv, send };
