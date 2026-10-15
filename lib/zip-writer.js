'use strict';

// Minimal, dependency-free streaming ZIP writer (PKZIP APPNOTE 6.3.x subset).
// - Entries are written sequentially to a file; nothing but the current chunk is held in memory.
// - Central-directory records are spilled to a sidecar temp file and appended at finish(),
//   so memory stays flat even for hundreds of thousands of entries.
// - Small entries (Buffers) are written with known CRC/sizes. Streamed entries use DEFLATE with
//   a data descriptor (bit 3). STORED+descriptor is avoided on purpose: java.util.zip.ZipInputStream
//   (used by some FileVault code paths) rejects STORED entries that carry a data descriptor.
// - ZIP64 records are emitted automatically when entry count > 65535, offsets/sizes >= 4 GiB.
// - Names are UTF-8 (general purpose flag bit 11).

const fs = require('node:fs');
const zlib = require('node:zlib');
const { once } = require('node:events');

const U32_MAX = 0xFFFFFFFF;
const U16_MAX = 0xFFFF;
// Streamed entries at or above this uncompressed size get a ZIP64 local header up-front
// (deflate output can be slightly larger than input, so leave headroom below 4 GiB).
const ZIP64_STREAM_THRESHOLD = 0xFFFF0000;
const COALESCE_BYTES = 1 << 20;

let CRC_TABLE = null;
function crc32Fallback(buf, prev) {
    if (!CRC_TABLE) {
        CRC_TABLE = new Uint32Array(256);
        for (let n = 0; n < 256; n++) {
            let c = n;
            for (let k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
            CRC_TABLE[n] = c >>> 0;
        }
    }
    let crc = (prev ^ U32_MAX) >>> 0;
    for (let i = 0; i < buf.length; i++) crc = CRC_TABLE[(crc ^ buf[i]) & 0xFF] ^ (crc >>> 8);
    return (crc ^ U32_MAX) >>> 0;
}
const crc32 = typeof zlib.crc32 === 'function'
    ? (buf, prev = 0) => zlib.crc32(buf, prev) >>> 0
    : (buf, prev = 0) => crc32Fallback(buf, prev);

function dosDateTime(date) {
    const d = date instanceof Date && !Number.isNaN(date.getTime()) ? date : new Date();
    const year = Math.min(Math.max(d.getFullYear(), 1980), 2107);
    const time = (d.getHours() << 11) | (d.getMinutes() << 5) | Math.floor(d.getSeconds() / 2);
    const day = ((year - 1980) << 9) | ((d.getMonth() + 1) << 5) | d.getDate();
    return { time, date: day };
}

function writeU64(buf, value, offset) {
    buf.writeBigUInt64LE(BigInt(value), offset);
}

class ZipWriter {
    /**
     * @param {string} outFile destination .zip path (overwritten)
     * @param {object} [opts]
     * @param {number} [opts.level=6] deflate level
     */
    constructor(outFile, opts = {}) {
        this.outFile = outFile;
        this.cdFile = `${outFile}.cd.tmp`;
        this.level = opts.level == null ? 6 : opts.level;
        this.offset = 0;
        this.entries = 0;
        this.cdSize = 0;
        this.out = fs.createWriteStream(outFile);
        this.cd = fs.createWriteStream(this.cdFile);
        this._error = null;
        this._outQ = { buf: Buffer.allocUnsafe(COALESCE_BYTES), used: 0 };
        this._cdQ = { buf: Buffer.allocUnsafe(COALESCE_BYTES), used: 0 };
        const onErr = (e) => { if (!this._error) this._error = e; };
        this.out.on('error', onErr);
        this.cd.on('error', onErr);
    }

    _check() {
        if (this._error) throw this._error;
    }

    // Small records (an asset is ~20 headers/XML docs of a few hundred bytes) are copied into one reusable
    // 1 MiB staging buffer per stream and written as a single chunk. Each write is awaited until the OS has
    // taken it, so the staging buffer can be reused and nothing queues up inside the WriteStream.
    async _write(stream, buf) {
        this._check();
        const q = stream === this.out ? this._outQ : this._cdQ;
        if (q.used + buf.length > q.buf.length) await this._flush(stream);
        if (buf.length >= q.buf.length) {
            await this._writeNow(stream, buf);
            return;
        }
        buf.copy(q.buf, q.used);
        q.used += buf.length;
    }

    async _flush(stream) {
        const q = stream === this.out ? this._outQ : this._cdQ;
        if (q.used === 0) return;
        const used = q.used;
        q.used = 0;
        await this._writeNow(stream, q.buf.subarray(0, used));
    }

    _writeNow(stream, data) {
        return new Promise((resolve, reject) => {
            stream.write(data, (err) => {
                if (err) {
                    if (!this._error) this._error = err;
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    async _out(buf) {
        await this._write(this.out, buf);
        this.offset += buf.length;
    }

    _localHeader(nameBuf, { flags, method, dt, crc, csize, usize, zip64 }) {
        const extraLen = zip64 ? 20 : 0;
        const h = Buffer.alloc(30 + nameBuf.length + extraLen);
        h.writeUInt32LE(0x04034b50, 0);
        h.writeUInt16LE(zip64 ? 45 : 20, 4);
        h.writeUInt16LE(flags, 6);
        h.writeUInt16LE(method, 8);
        h.writeUInt16LE(dt.time, 10);
        h.writeUInt16LE(dt.date, 12);
        h.writeUInt32LE(crc >>> 0, 14);
        h.writeUInt32LE(zip64 ? U32_MAX : csize, 18);
        h.writeUInt32LE(zip64 ? U32_MAX : usize, 22);
        h.writeUInt16LE(nameBuf.length, 26);
        h.writeUInt16LE(extraLen, 28);
        nameBuf.copy(h, 30);
        if (zip64) {
            const e = 30 + nameBuf.length;
            h.writeUInt16LE(0x0001, e);
            h.writeUInt16LE(16, e + 2);
            writeU64(h, zip64.usize || 0, e + 4);
            writeU64(h, zip64.csize || 0, e + 12);
        }
        return h;
    }

    async _centralRecord(nameBuf, { flags, method, dt, crc, csize, usize, localOffset, isDir }) {
        const extras = [];
        const needU = usize >= U32_MAX;
        const needC = csize >= U32_MAX;
        const needO = localOffset >= U32_MAX;
        if (needU) extras.push(usize);
        if (needC) extras.push(csize);
        if (needO) extras.push(localOffset);
        const zip64 = extras.length > 0;
        const extraLen = zip64 ? 4 + 8 * extras.length : 0;
        const h = Buffer.alloc(46 + nameBuf.length + extraLen);
        h.writeUInt32LE(0x02014b50, 0);
        h.writeUInt16LE(45, 4); // made by: DOS host, spec 4.5
        h.writeUInt16LE(zip64 ? 45 : 20, 6);
        h.writeUInt16LE(flags, 8);
        h.writeUInt16LE(method, 10);
        h.writeUInt16LE(dt.time, 12);
        h.writeUInt16LE(dt.date, 14);
        h.writeUInt32LE(crc >>> 0, 16);
        h.writeUInt32LE(needC ? U32_MAX : csize, 20);
        h.writeUInt32LE(needU ? U32_MAX : usize, 24);
        h.writeUInt16LE(nameBuf.length, 28);
        h.writeUInt16LE(extraLen, 30);
        h.writeUInt16LE(0, 32); // comment
        h.writeUInt16LE(0, 34); // disk
        h.writeUInt16LE(0, 36); // internal attrs
        h.writeUInt32LE(isDir ? 0x10 : 0x20, 38); // external attrs (DOS dir / archive)
        h.writeUInt32LE(needO ? U32_MAX : localOffset, 42);
        nameBuf.copy(h, 46);
        if (zip64) {
            let p = 46 + nameBuf.length;
            h.writeUInt16LE(0x0001, p);
            h.writeUInt16LE(8 * extras.length, p + 2);
            p += 4;
            for (const v of extras) { writeU64(h, v, p); p += 8; }
        }
        await this._write(this.cd, h);
        this.cdSize += h.length;
        this.entries++;
    }

    /** Adds a directory entry (name must end with '/'). */
    async addDirectory(name, mtime) {
        const nameBuf = Buffer.from(name.endsWith('/') ? name : `${name}/`, 'utf8');
        const dt = dosDateTime(mtime);
        const localOffset = this.offset;
        const meta = { flags: 0x0800, method: 0, dt, crc: 0, csize: 0, usize: 0 };
        await this._out(this._localHeader(nameBuf, meta));
        await this._centralRecord(nameBuf, { ...meta, localOffset, isDir: true });
    }

    /**
     * Adds an in-memory entry with CRC and sizes in the local header (no data descriptor).
     * @param {boolean} [store=false] store instead of deflate (for already-compressed data)
     */
    async addBuffer(name, data, mtime, store = false) {
        const nameBuf = Buffer.from(name, 'utf8');
        const dt = dosDateTime(mtime);
        const crc = crc32(data);
        let body = data;
        let method = 0;
        if (!store && data.length > 0) {
            const deflated = zlib.deflateRawSync(data, { level: this.level });
            if (deflated.length < data.length) { body = deflated; method = 8; }
        }
        const localOffset = this.offset;
        const meta = { flags: 0x0800, method, dt, crc, csize: body.length, usize: data.length };
        await this._out(this._localHeader(nameBuf, meta));
        await this._out(body);
        await this._centralRecord(nameBuf, { ...meta, localOffset, isDir: false });
    }

    /**
     * Streams an entry from a readable (e.g. fs.createReadStream). DEFLATE + data descriptor.
     * @param {number} expectedSize uncompressed size if known (decides ZIP64 local header)
     */
    async addStream(name, readable, mtime, expectedSize = 0) {
        const nameBuf = Buffer.from(name, 'utf8');
        const dt = dosDateTime(mtime);
        const zip64 = expectedSize >= ZIP64_STREAM_THRESHOLD;
        const flags = 0x0800 | 0x0008;
        const localOffset = this.offset;
        await this._out(this._localHeader(nameBuf, {
            flags, method: 8, dt, crc: 0, csize: 0, usize: 0, zip64: zip64 ? {} : null,
        }));

        let crc = 0;
        let usize = 0;
        let csize = 0;
        const deflate = zlib.createDeflateRaw({ level: this.level });
        const pump = (async () => {
            for await (const chunk of deflate) {
                csize += chunk.length;
                await this._out(chunk);
            }
        })();
        try {
            for await (const chunk of readable) {
                crc = crc32(chunk, crc);
                usize += chunk.length;
                if (!deflate.write(chunk)) await once(deflate, 'drain');
            }
            deflate.end();
        } catch (e) {
            deflate.destroy(e);
            await pump.catch(() => {});
            throw e;
        }
        await pump;

        if (!zip64 && (usize >= U32_MAX || csize >= U32_MAX)) {
            throw new Error(`zip entry ${name} exceeded 4 GiB but was not announced as ZIP64 (size changed while reading?)`);
        }
        let dd;
        if (zip64) {
            dd = Buffer.alloc(24);
            dd.writeUInt32LE(0x08074b50, 0);
            dd.writeUInt32LE(crc >>> 0, 4);
            writeU64(dd, csize, 8);
            writeU64(dd, usize, 16);
        } else {
            dd = Buffer.alloc(16);
            dd.writeUInt32LE(0x08074b50, 0);
            dd.writeUInt32LE(crc >>> 0, 4);
            dd.writeUInt32LE(csize, 8);
            dd.writeUInt32LE(usize, 12);
        }
        await this._out(dd);
        await this._centralRecord(nameBuf, {
            flags, method: 8, dt, crc, csize, usize, localOffset, isDir: false,
        });
        return { usize, csize, crc };
    }

    /** Appends the central directory and end records, closes the file, removes the sidecar. */
    async finish() {
        this._check();
        const cdOffset = this.offset;
        await this._flush(this.cd);
        this.cd.end();
        await once(this.cd, 'finish');
        this._check();
        for await (const chunk of fs.createReadStream(this.cdFile, { highWaterMark: 1 << 20 })) {
            await this._out(chunk);
        }
        const cdSize = this.cdSize;
        const needZip64 = this.entries > U16_MAX || cdOffset >= U32_MAX || cdSize >= U32_MAX
            || this.offset >= U32_MAX;
        if (needZip64) {
            const z64Offset = this.offset;
            const rec = Buffer.alloc(56);
            rec.writeUInt32LE(0x06064b50, 0);
            writeU64(rec, 44, 4);
            rec.writeUInt16LE(45, 12);
            rec.writeUInt16LE(45, 14);
            rec.writeUInt32LE(0, 16);
            rec.writeUInt32LE(0, 20);
            writeU64(rec, this.entries, 24);
            writeU64(rec, this.entries, 32);
            writeU64(rec, cdSize, 40);
            writeU64(rec, cdOffset, 48);
            await this._out(rec);
            const loc = Buffer.alloc(20);
            loc.writeUInt32LE(0x07064b50, 0);
            loc.writeUInt32LE(0, 4);
            writeU64(loc, z64Offset, 8);
            loc.writeUInt32LE(1, 16);
            await this._out(loc);
        }
        const eocd = Buffer.alloc(22);
        eocd.writeUInt32LE(0x06054b50, 0);
        eocd.writeUInt16LE(0, 4);
        eocd.writeUInt16LE(0, 6);
        eocd.writeUInt16LE(Math.min(this.entries, U16_MAX), 8);
        eocd.writeUInt16LE(Math.min(this.entries, U16_MAX), 10);
        eocd.writeUInt32LE(Math.min(cdSize, U32_MAX), 12);
        eocd.writeUInt32LE(Math.min(cdOffset, U32_MAX), 16);
        eocd.writeUInt16LE(0, 20);
        await this._out(eocd);
        await this._flush(this.out);
        this.out.end();
        await once(this.out, 'finish');
        this._check();
        await fs.promises.rm(this.cdFile, { force: true });
        return { bytes: this.offset, entries: this.entries, zip64: needZip64 };
    }

    /** Aborts: closes streams and removes partial output. */
    async abort() {
        for (const s of [this.out, this.cd]) {
            try { s.destroy(); } catch (_) { /* ignore */ }
        }
        await fs.promises.rm(this.cdFile, { force: true }).catch(() => {});
        await fs.promises.rm(this.outFile, { force: true }).catch(() => {});
    }
}

module.exports = { ZipWriter, crc32 };
