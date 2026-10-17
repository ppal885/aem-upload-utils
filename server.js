const express = require('express');
const nodePath = require('path');
const crypto = require('crypto');
const fs = require('fs');
const fsPromises = require('fs').promises;
const os = require('os');
const { execFile } = require('child_process');
const cors = require('cors');
const compression = require('compression');
const http = require('http');
const https = require('https');
const net = require('net');
const rateLimit = require('express-rate-limit');
const multer = require('multer');
const auth = require('./auth');
const AemNames = require('./public/aem-names');
const mailer = require('./lib/mailer');
const packages = require('./lib/packages');
const backup = require('./lib/backup');
const copyFromAem = require('./lib/copy-from-aem');
const guidesSetup = require('./lib/guides-setup');
const guidesStatus = require('./lib/guides-status');
const guidesProfile = require('./lib/guides-profile');
const { TARGETS, runUpload, scanDirectory, validateUploadConfig, normalizeAemBaseUrl, checkAemCredentials } = require('./upload-service');

const app = express();
const PORT = process.env.PORT || 3000;
// HTTPS: set TLS_CERT_FILE + TLS_KEY_FILE (PEM). HTTPS_ONLY=true turns the plain-HTTP port into a redirect.
const TLS_CERT_FILE = process.env.TLS_CERT_FILE || '';
const TLS_KEY_FILE = process.env.TLS_KEY_FILE || '';
// HTTPS_PORT equal to PORT serves both on one port (when the network only lets one port through)
const HTTPS_PORT = parseInt(process.env.HTTPS_PORT, 10) || 3443;
const HTTPS_ONLY = /^(1|true|yes)$/i.test(process.env.HTTPS_ONLY || '');
const HISTORY_FILE = process.env.HISTORY_FILE ? nodePath.resolve(process.env.HISTORY_FILE) : nodePath.join(__dirname, 'upload-history.json');
const MAX_HISTORY = parseInt(process.env.MAX_HISTORY, 10) || 5000;
const MAX_HISTORY_PER_USER = parseInt(process.env.MAX_HISTORY_PER_USER, 10) || 200;
const SSE_EVENT_CAP = parseInt(process.env.SSE_EVENT_CAP) || 10000;
const MAX_UPLOAD_SIZE = parseInt(process.env.MAX_UPLOAD_SIZE) || 5 * 1024 * 1024 * 1024; // 5 GB default
const TEMP_DIR_PREFIX = 'guides-upload-';
const FILE_BATCH_QUEUE_CAP = parseInt(process.env.FILE_BATCH_QUEUE_CAP) || 5000;
const FILE_BATCH_INTERVAL_MS = parseInt(process.env.FILE_BATCH_INTERVAL_MS) || 150;
const SSE_KEEPALIVE_MS = parseInt(process.env.SSE_KEEPALIVE_MS) || 15000;
const SESSION_REAPER_INTERVAL_MS = parseInt(process.env.SESSION_REAPER_INTERVAL_MS) || 2 * 60 * 1000;
const MEMORY_MONITOR_INTERVAL_MS = parseInt(process.env.MEMORY_MONITOR_INTERVAL_MS) || 5 * 60 * 1000;
const SHUTDOWN_TIMEOUT_MS = parseInt(process.env.SHUTDOWN_TIMEOUT_MS) || 10000;
const FILE_ERRORS_CAP = parseInt(process.env.FILE_ERRORS_CAP) || 1000;
// Staged files take at least one filesystem block each, so many small files need far more
// space than their byte total suggests.
const TEMP_SPACE_PER_FILE_BYTES = 4096;
// 8 pipelines x 4 transfers: 1.7x the old 4 x 2 on AEM Cloud (README, Speed) without one upload flooding a shared server
const DEFAULT_UPLOAD_PIPELINES = parseInt(process.env.UPLOAD_PIPELINES, 10) || 8;
const TEMP_SPACE_MARGIN = 1.2;

// ─── Multer config for browser file uploads ───
// Files are saved to a flat staging dir first, then rearranged into
// the correct folder structure after all form fields are available.
const browserUploadStorage = multer.diskStorage({
    destination: (req, file, cb) => {
        if (!req._tempDir) {
            req._tempDir = nodePath.join(os.tmpdir(), TEMP_DIR_PREFIX + crypto.randomUUID());
            req._stagingDir = nodePath.join(req._tempDir, '__staging');
            fs.mkdirSync(req._stagingDir, { recursive: true });
        }
        cb(null, req._stagingDir);
    },
    filename: (_req, file, cb) => {
        // Use index-based names to avoid duplicate filename collisions
        if (!_req._fileIdx) _req._fileIdx = 0;
        const idx = _req._fileIdx++;
        // Store original name for later reconstruction
        if (!_req._fileOrigNames) _req._fileOrigNames = [];
        _req._fileOrigNames.push(file.originalname);
        cb(null, `__f${idx}__${file.originalname}`);
    },
});
const browserUpload = multer({
    storage: browserUploadStorage,
    limits: { fileSize: MAX_UPLOAD_SIZE, files: 50000 },
});

// ─── Multer config for chunked browser uploads ───
// Each chunk is a separate POST with a sessionId in the URL path.
// Files accumulate in the session's staging dir across chunks.
const chunkUploadStorage = multer.diskStorage({
    destination: (req, file, cb) => {
        const session = browserUploadSessions.get(req.params.sessionId);
        if (!session) return cb(new Error('Invalid or expired upload session'));
        if (!session._stagingReady) {
            const stagingDir = nodePath.join(session.tempDir, '__staging');
            fs.mkdirSync(stagingDir, { recursive: true });
            session._stagingReady = stagingDir;
        }
        cb(null, session._stagingReady);
    },
    filename: (req, file, cb) => {
        const session = browserUploadSessions.get(req.params.sessionId);
        if (!session) return cb(new Error('Invalid or expired upload session'));
        // Atomic counter on session — safe with parallel chunks since Node.js is single-threaded
        if (session._fileCounter == null) session._fileCounter = 0;
        const idx = session._fileCounter++;
        cb(null, `__f${idx}__${file.originalname}`);
    },
});
const chunkUpload = multer({
    storage: chunkUploadStorage,
    limits: { fileSize: MAX_UPLOAD_SIZE, files: 1500 },
});

// Rearranges files from flat staging dir into proper folder structure
const WINDOWS = process.platform === 'win32';

async function rearrangeUploadedFiles(tempDir, files, relativePathsJson) {
    const stagingDir = nodePath.join(tempDir, '__staging');
    const contentDir = nodePath.join(tempDir, 'content');
    await fsPromises.mkdir(contentDir, { recursive: true });

    let relativePaths = [];
    try {
        if (relativePathsJson) {
            relativePaths = typeof relativePathsJson === 'string'
                ? JSON.parse(relativePathsJson) : relativePathsJson;
        }
    } catch (e) {
        log.warn('Failed to parse _relativePaths:', e.message);
    }

    // Collect unique directories and build lightweight move list (src+dest only)
    const dirSet = new Set();
    const BATCH = 1000;
    let totalTasks = 0;
    let rearrangedCount = 0;
    let rearrangeErrors = 0;

    async function moveFile(src, dest) {
        try {
            await fsPromises.rename(src, dest);
        } catch (e) {
            if (e.code === 'EXDEV') {
                await fsPromises.copyFile(src, dest);
                await fsPromises.unlink(src);
            } else {
                throw e;
            }
        }
    }

    // Process in streaming batches to limit peak memory
    for (let b = 0; b < files.length; b += BATCH) {
        const end = Math.min(b + BATCH, files.length);
        const batch = [];
        for (let i = b; i < end; i++) {
            const stagedFile = files[i];
            let relPath = relativePaths[i] || stagedFile.originalname;
            if (!relPath || !relPath.trim()) {
                relPath = stagedFile.originalname || `unnamed_${i}`;
                if (i < 5) log.warn(`  File ${i} had empty relative path, falling back to: ${relPath}`);
            }
            const sanitized = relPath
                .split(/[/\\]/)
                .map(seg => seg.normalize('NFC'))
                .filter(seg => seg.length > 0 && seg !== '.' && seg !== '..')
                .map(seg => (WINDOWS ? seg.replace(/[<>:"|?*]/g, '_') : seg))
                .join(nodePath.sep);
            if (!sanitized) continue;
            const destPath = nodePath.resolve(contentDir, sanitized);
            if (!destPath.startsWith(contentDir + nodePath.sep) && destPath !== contentDir) {
                if (rearrangeErrors <= 3) log.warn(`  Path traversal blocked: ${relPath}`);
                rearrangeErrors++;
                continue;
            }
            const dir = nodePath.dirname(destPath);
            if (!dirSet.has(dir)) {
                dirSet.add(dir);
                await fsPromises.mkdir(dir, { recursive: true });
            }
            batch.push({ src: stagedFile.path, dest: destPath });
            totalTasks++;
        }
        const results = await Promise.allSettled(batch.map(async (t) => {
            await moveFile(t.src, t.dest);
            rearrangedCount++;
        }));
        for (const r of results) {
            if (r.status === 'rejected') {
                rearrangeErrors++;
                if (rearrangeErrors <= 5) log.warn(`  File move failed: ${r.reason?.message || r.reason}`);
            }
        }
    }
    if (rearrangeErrors > 0) log.warn(`  ${rearrangeErrors} file(s) failed to rearrange`);
    log.info(`  Rearranged ${rearrangedCount}/${files.length} files into folder structure (${dirSet.size} directories)`);

    // Remove staging dir
    try { await fsPromises.rm(stagingDir, { recursive: true, force: true }); } catch (_) {}

    return contentDir;
}

// Newest first. Each person keeps their own last MAX_HISTORY_PER_USER uploads, so one busy user cannot
// push everyone else's history out.
function trimHistory() {
    const perOwner = new Map();
    uploadHistory = uploadHistory.filter((h) => {
        const n = (perOwner.get(h.owner) || 0) + 1;
        perOwner.set(h.owner, n);
        return n <= MAX_HISTORY_PER_USER;
    });
    if (uploadHistory.length > MAX_HISTORY) uploadHistory.length = MAX_HISTORY;
}

// ─── Temp directory cleanup ───
async function cleanupTempDir(dirPath) {
    if (!dirPath || !dirPath.includes(TEMP_DIR_PREFIX)) return;
    // Security: ensure path is under os.tmpdir()
    const resolved = nodePath.resolve(dirPath);
    const tmpBase = nodePath.resolve(os.tmpdir());
    if (!resolved.startsWith(tmpBase + nodePath.sep) && resolved !== tmpBase) {
        log.error('cleanupTempDir refused — path outside tmpdir:', resolved);
        return;
    }
    try {
        await fsPromises.rm(resolved, { recursive: true, force: true });
        log.debug('Cleaned up temp dir:', resolved);
    } catch (e) {
        log.warn('Failed to clean temp dir:', resolved, e.message);
    }
}

// The session reaper only knows sessions held in memory, so staging dirs left by a crash or
// restart would otherwise stay forever. Nothing can be using them while the server starts.
async function sweepStaleTempDirs() {
    const tmpBase = os.tmpdir();
    await fsPromises.mkdir(tmpBase, { recursive: true });
    const entries = await fsPromises.readdir(tmpBase, { withFileTypes: true });
    const stale = entries.filter((e) => e.isDirectory() && e.name.startsWith(TEMP_DIR_PREFIX));
    let removed = 0;
    for (const entry of stale) {
        const dir = nodePath.join(tmpBase, entry.name);
        if (await registerInterruptedJob(dir)) continue;
        await cleanupTempDir(dir);
        removed++;
    }
    if (removed > 0) log.info(`Removed ${removed} leftover temp dir(s) from ${tmpBase}`);
    if (interruptedJobs.size > 0) log.info(`${interruptedJobs.size} interrupted upload(s) can be resumed`);
}

// ─── Resume after a restart ───
// Each running upload keeps job.json in its staging dir and deletes every staged file once AEM has it,
// so after a restart the dir holds exactly the unfinished files. Resume needs the AEM password again.
const RESUME_JOB_FILE = 'job.json';
const RESUME_KEEP_MS = 3 * 24 * 60 * 60 * 1000;
const interruptedJobs = new Map();
// Finding interrupted jobs counts their files, which takes a moment; requests wait for it
let startupScan = Promise.resolve();

async function writeResumeJob(tempDir, job) {
    const file = nodePath.join(tempDir, RESUME_JOB_FILE);
    await fsPromises.writeFile(file + '.tmp', JSON.stringify(job));
    await fsPromises.rename(file + '.tmp', file);
}

async function countFiles(paths) {
    let n = 0;
    const walk = async (p) => {
        let st;
        try { st = await fsPromises.stat(p); } catch (_) { return; }
        if (st.isFile()) { n++; return; }
        if (!st.isDirectory()) return;
        for (const name of await fsPromises.readdir(p)) await walk(nodePath.join(p, name));
    };
    for (const p of paths) await walk(p);
    return n;
}

/** True when dir held a resumable job (now listed in interruptedJobs); old or empty jobs are not kept. */
async function registerInterruptedJob(dir) {
    let job;
    try { job = JSON.parse(await fsPromises.readFile(nodePath.join(dir, RESUME_JOB_FILE), 'utf8')); } catch (_) { return false; }
    let mtime = 0;
    try { mtime = (await fsPromises.stat(nodePath.join(dir, RESUME_JOB_FILE))).mtimeMs; } catch (_) { /* gone */ }
    if (Date.now() - mtime > RESUME_KEEP_MS) return false;
    const remaining = await countFiles(job.contentChildren || []);
    if (remaining === 0) return false;
    interruptedJobs.set(job.jobId, { ...job, tempDir: dir, remaining, interruptedAt: new Date(mtime).toISOString() });
    log.info(`Interrupted upload ${job.jobId} (${job.owner}): ${remaining} of ${job.totalFiles} files left -> ${job.damPath}`);
    return true;
}

function publicInterruptedJob(j) {
    return { id: j.jobId, ownerName: j.ownerName, owner: j.owner, baseUrl: j.baseUrl, damPath: j.damPath, username: j.username,
        totalFiles: j.totalFiles, remaining: j.remaining, startedAt: j.startedAt, interruptedAt: j.interruptedAt };
}

// Staging dirs that no live session owns and that are over a day old (a crash mid-upload, a killed
// process) are removed every hour, not only at start-up.
const ORPHAN_TEMP_MAX_AGE_MS = 24 * 60 * 60 * 1000;
async function sweepOrphanTempDirs() {
    const tmpBase = os.tmpdir();
    const owned = new Set();
    uploadSessions.forEach((sess) => { if (sess.tempDir) owned.add(nodePath.resolve(sess.tempDir)); });
    browserUploadSessions.forEach((sess) => { if (sess.tempDir) owned.add(nodePath.resolve(sess.tempDir)); });
    let entries;
    try { entries = await fsPromises.readdir(tmpBase, { withFileTypes: true }); } catch (_) { return; }
    let removed = 0;
    for (const e of entries) {
        if (!e.isDirectory() || !e.name.startsWith(TEMP_DIR_PREFIX)) continue;
        const full = nodePath.resolve(tmpBase, e.name);
        if (owned.has(full)) continue;
        if ([...interruptedJobs.values()].some((j) => nodePath.resolve(j.tempDir) === full)) continue;
        try {
            const st = await fsPromises.stat(full);
            if (Date.now() - st.mtimeMs < ORPHAN_TEMP_MAX_AGE_MS) continue;
        } catch (_) { continue; }
        await cleanupTempDir(full);
        removed++;
    }
    if (removed) log.info(`Hourly cleanup removed ${removed} orphaned temp dir(s)`);
}

async function getTempFreeBytes() {
    try {
        const stats = await fsPromises.statfs(os.tmpdir());
        return stats.bavail * stats.bsize;
    } catch (e) {
        log.warn('Cannot read free space of temp dir:', os.tmpdir(), e.message);
        return null;
    }
}

/**
 * @returns {Promise<string|null>} error message when the temp dir cannot hold the upload
 */
async function checkTempSpace(totalBytes, fileCount) {
    const free = await getTempFreeBytes();
    if (free == null) return null;
    const needed = Math.ceil((totalBytes + fileCount * TEMP_SPACE_PER_FILE_BYTES) * TEMP_SPACE_MARGIN);
    if (free >= needed) return null;
    const mb = (bytes) => `${Math.ceil(bytes / 1048576)} MB`;
    return `The upload server has only ${mb(free)} free for staging files (${os.tmpdir()}), `
        + `but this upload needs about ${mb(needed)}. Free up space on the server or upload fewer files at a time.`;
}

// ─── Structured Logging ───
const LOG_LEVELS = { error: 0, warn: 1, info: 2, debug: 3 };
const LOG_LEVEL = LOG_LEVELS[process.env.LOG_LEVEL || 'info'] ?? LOG_LEVELS.info;
const LOG_FILE = process.env.LOG_FILE || ''; // e.g. "server.log"
let logStream = null;

if (LOG_FILE) {
    logStream = fs.createWriteStream(nodePath.resolve(LOG_FILE), { flags: 'a' });
    logStream.on('error', (e) => console.error('Log file write error:', e.message));
}

function writeLog(level, args) {
    const ts = new Date().toISOString();
    const prefix = `${ts} [${level}]`;
    const msg = args.map(a => (typeof a === 'object' ? JSON.stringify(a) : String(a))).join(' ');
    const line = `${prefix} ${msg}`;

    // Console output
    if (level === 'ERROR') console.error(line);
    else if (level === 'WARN') console.warn(line);
    else console.log(line);

    // File output
    if (logStream) logStream.write(line + '\n');
}

const log = {
    error: (...args) => { if (LOG_LEVEL >= LOG_LEVELS.error) writeLog('ERROR', args); },
    warn:  (...args) => { if (LOG_LEVEL >= LOG_LEVELS.warn)  writeLog('WARN',  args); },
    info:  (...args) => { if (LOG_LEVEL >= LOG_LEVELS.info)  writeLog('INFO',  args); },
    debug: (...args) => { if (LOG_LEVEL >= LOG_LEVELS.debug) writeLog('DEBUG', args); },
};

// ─── Shared SSE broadcast factory ───
function createBroadcast(session) {
    return function broadcast(eventName, data) {
        const payload = JSON.stringify(data);
        const msg = `event: ${eventName}\ndata: ${payload}\n\n`;
        if (eventName === 'done' || (eventName !== 'fileprogress' && session.events.length < SSE_EVENT_CAP)) {
            session.events.push({ eventName, data });
        }
        session.listeners.forEach((sseRes) => {
            try { sseRes.write(msg); if (typeof sseRes.flush === 'function') sseRes.flush(); } catch (_) {}
        });
    };
}

/**
 * Broadcast terminal SSE + persist history when the user cancels an in-flight upload.
 * The underlying @adobe/aem-upload job may still run briefly; duplicate done/history is skipped in .then if session.userCancelled is set.
 */
function emitUserCancelledUpload(uploadId, session, uploadKey) {
    if (session.done) return;
    const meta = session.meta;
    const duration = Date.now() - meta.startTime;
    const broadcast = createBroadcast(session);
    try {
        broadcast('done', {
            success: false,
            totalFiles: session.totalFiles || 0,
            totalBytes: session.totalBytes || 0,
            transferredBytes: session.transferredBytes || 0,
            errorCount: session.failedFiles || 0,
            errors: [],
            uploadedFiles: Math.max(0, (session.completedFiles || 0) - (session.failedFiles || 0)),
            fileErrors: session.fileErrors || {},
            cancelled: true,
            duration,
        });
    } catch (e) { log.error('broadcast error in emitUserCancelledUpload:', e.message); }
    session.done = true;
    session.doneTime = Date.now();
    session.userCancelled = true;
    if (uploadKey && activeUploads.get(uploadKey) === uploadId) activeUploads.delete(uploadKey);
    addHistoryRow(meta, {
        uploadId,
        target: meta.target,
        baseUrl: meta.baseUrl,
        damPath: meta.damPath,
        localPath: meta.localPath,
        totalFiles: session.totalFiles || 0,
        totalBytes: session.totalBytes || 0,
        success: false,
        errorCount: session.failedFiles || 0,
        errors: [],
        uploadedFiles: Math.max(0, (session.completedFiles || 0) - (session.failedFiles || 0)),
        cancelled: true,
        duration,
        timestamp: new Date().toISOString(),
    });
    trimHistory();
    saveHistoryToDisk();
}

// ─── Crash resilience ───
process.on('uncaughtException', (err) => {
    writeLog('FATAL', ['Uncaught exception — server staying alive:', err.message, err.stack]);
});
process.on('unhandledRejection', (reason) => {
    writeLog('FATAL', ['Unhandled promise rejection:', reason instanceof Error ? reason.message : String(reason)]);
});

// ─── Middleware ───

// CORS
app.use(cors({
    origin: process.env.CORS_ORIGIN || '*',
    methods: ['GET', 'POST', 'DELETE'],
}));

// Rate limiting
// Limits count per signed-in person, not per IP: a whole team behind one VPN/proxy address shares an IP
const perUserKey = (req) => (req.user && req.user.key ? 'u:' + req.user.key : 'ip:' + req.ip);
const apiLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 300,
    keyGenerator: perUserKey,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many requests, please try again later' },
    skip: (req) => req.path.startsWith('/upload/browser/chunk/'),
});

// Upload-specific rate limit (init/finalize endpoints)
const uploadLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 60,
    keyGenerator: perUserKey,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many upload requests, please slow down' },
});

// Every API call needs a signed-in user (or an API key for scripts); see auth.js
const API_KEY = process.env.API_KEY || '';
app.use('/api/', express.json({ limit: '1mb' }));
app.use('/api/', auth.requireAuth(log));
app.use('/api/', apiLimiter);

// Counts failed sign-ins only, so many users behind one proxy IP are not locked out by each other
const loginLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 10,
    skipSuccessfulRequests: true,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many failed sign-in attempts. Wait a minute and try again.' },
});

// Sign-up and forgot-password count every request. Generous per address: on launch day a whole team
// signs up at once from one VPN/proxy IP; forgot-password is also limited per email below.
const accountLimiter = rateLimit({
    windowMs: 10 * 60 * 1000,
    max: 300,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many requests. Wait a few minutes and try again.' },
});

// HTTP request logger
app.use('/api/', (req, res, next) => {
    const start = Date.now();
    const method = req.method;
    const url = req.originalUrl.replace(/[?&]apiKey=[^&]*/g, '');
    const ip = req.ip || req.socket.remoteAddress;

    res.on('finish', () => {
        const duration = Date.now() - start;
        const status = res.statusCode;
        const level = status >= 500 ? 'error' : status >= 400 ? 'warn' : 'debug';
        log[level](`${method} ${url} ${status} ${duration}ms [${ip}]`);
    });
    next();
});

app.use(compression({
    threshold: 1024,
    filter: (req, res) => {
        if (req.headers.accept === 'text/event-stream') return false;
        return compression.filter(req, res);
    },
}));
app.use(express.json({ limit: '1mb' }));
// So people can trust the private HTTPS certificate once (tools/make-cert.sh)
app.get('/guides-upload-ca.crt', (_req, res) => {
    const ca = nodePath.join(__dirname, 'certs', 'ca.crt');
    if (!fs.existsSync(ca)) return res.status(404).end();
    res.type('application/x-x509-ca-cert').download(ca, 'guides-upload-ca.crt');
});

// index.html is always revalidated (a deploy shows up at once); the files it links carry a content hash
// (?v=…, stamped by tools/build-css.js), so browsers keep those for a year and skip the request entirely
app.use(express.static(nodePath.join(__dirname, 'public'), {
    etag: true,
    lastModified: true,
    setHeaders: (res, filePath) => {
        if (filePath.endsWith('.html')) {
            res.setHeader('Cache-Control', 'no-cache');
        } else if (res.req && res.req.query && res.req.query.v) {
            res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
        } else {
            res.setHeader('Cache-Control', 'no-cache');
        }
    },
}));

// ─── In-memory stores ───
const uploadSessions = new Map();
const browserUploadSessions = new Map(); // Chunked browser upload sessions
// Chunk session id -> AEM upload id, so a cancel for the session still reaches the started upload
const finalizedBrowserSessions = new Map();
let uploadHistory = [];

// Session reaper: clean up stuck/orphaned sessions every 2 minutes
const SESSION_MAX_AGE_MS = 24 * 60 * 60 * 1000; // 24 hours
const SESSION_DONE_LINGER_MS = 2 * 60 * 1000; // Keep done sessions 2 min for final SSE delivery
setInterval(() => { sweepOrphanTempDirs().catch((e) => log.warn('Temp cleanup failed:', e.message)); }, 60 * 60 * 1000).unref();
const BROWSER_SESSION_MAX_AGE_MS = 8 * 60 * 60 * 1000; // 8 hours — 50K files at 2 AEM threads can take hours
setInterval(() => {
    const now = Date.now();
    let reaped = 0;
    uploadSessions.forEach((session, uploadId) => {
        const startAge = now - session.meta.startTime;
        const doneAge = session.doneTime ? now - session.doneTime : 0;
        if (session.done && doneAge > SESSION_DONE_LINGER_MS && session.runSettled !== false) {
            if (session.tempDir) cleanupTempDir(session.tempDir).catch(() => {});
            session.listeners.forEach((res) => { try { res.end(); } catch (_) {} });
            uploadSessions.delete(uploadId);
            reaped++;
        } else if (session.done && session.events.length > 0) {
            // Eagerly free event buffer from completed sessions
            session.events = [];
            session.fileErrors = {};
        } else if (!session.done && startAge > SESSION_MAX_AGE_MS) {
            log.warn('Reaping stuck session after 24h:', uploadId);
            if (typeof session.cancelFn === 'function') { try { session.cancelFn(); } catch (_) {} }
            if (session.tempDir) cleanupTempDir(session.tempDir).catch(() => {});
            session.listeners.forEach((res) => { try { res.end(); } catch (_) {} });
            uploadSessions.delete(uploadId);
            activeUploads.forEach((uid, key) => { if (uid === uploadId) activeUploads.delete(key); });
            reaped++;
        }
    });
    // Reap stale chunked browser upload sessions
    browserUploadSessions.forEach((session, sessionId) => {
        const age = now - session.createdAt;
        const copyIdle = session.copy && session.copy.finishedAt && now - session.copy.finishedAt > COPY_IDLE_MS;
        if (age > BROWSER_SESSION_MAX_AGE_MS || copyIdle) {
            log.warn(copyIdle ? `Removing copied files nobody used for ${COPY_IDLE_MS / 60000} min: ${sessionId}` : `Reaping stale browser upload session after 8h: ${sessionId}`);
            if (session.copy && session.copy.abort) session.copy.abort.abort();
            session.files = []; session.relativePaths = [];
            cleanupTempDir(session.tempDir).catch(() => {});
            browserUploadSessions.delete(sessionId);
            reaped++;
        }
    });
    if (reaped > 0) log.info(`Session reaper: cleaned ${reaped} sessions (upload=${uploadSessions.size}, browser=${browserUploadSessions.size})`);
}, SESSION_REAPER_INTERVAL_MS);

function addHistoryRow(meta, row) {
    if (meta && meta.skippedFiles) row.skippedFiles = meta.skippedFiles;
    if (meta && meta.folder) row.folder = meta.folder;
    uploadHistory.unshift({ ...row, owner: (meta && meta.owner) || null, ownerName: (meta && meta.ownerName) || null });
}

// Async history persistence with a write lock
let historyWriteInProgress = false;
let historyWriteQueued = false;

async function loadHistoryFromDisk() {
    try {
        const data = await fsPromises.readFile(HISTORY_FILE, 'utf-8');
        const parsed = JSON.parse(data);
        if (!Array.isArray(parsed)) throw new Error('history file is not a JSON array');
        return parsed;
    } catch (e) {
        if (e.code === 'ENOENT') return [];
        // Keep the unreadable file; the next save would otherwise replace it with an empty list
        const backup = `${HISTORY_FILE}.unreadable-${new Date().toISOString().replace(/[:.]/g, '-')}`;
        try { await fsPromises.copyFile(HISTORY_FILE, backup); } catch (_) { /* nothing to keep */ }
        log.error(`Failed to load history (${e.message}); kept the file as ${nodePath.basename(backup)} and started with an empty history`);
        return [];
    }
}

function historyWritesIdle() {
    return !historyWriteInProgress && !historyWriteQueued;
}

async function saveHistoryToDisk() {
    if (historyWriteInProgress) { historyWriteQueued = true; return; }
    historyWriteInProgress = true;
    try {
        // Write then rename, so a crash mid-write cannot leave a truncated history file
        const tmpFile = HISTORY_FILE + '.tmp';
        await fsPromises.writeFile(tmpFile, JSON.stringify(uploadHistory, null, 2));
        await fsPromises.rename(tmpFile, HISTORY_FILE);
    } catch (e) {
        log.error('Failed to save history:', e.message);
    } finally {
        historyWriteInProgress = false;
        if (historyWriteQueued) { historyWriteQueued = false; saveHistoryToDisk(); }
    }
}

// ─── Path analysis & validation ───
// Allowed base directories for browsing (empty = unrestricted)
const BROWSE_ROOTS = process.env.BROWSE_ROOTS ? process.env.BROWSE_ROOTS.split(',').map(s => s.trim()) : [];

function isPathWithinRoots(normalizedPath) {
    if (BROWSE_ROOTS.length === 0) return true;
    return BROWSE_ROOTS.some(root => {
        const nRoot = nodePath.resolve(root);
        return normalizedPath === nRoot || normalizedPath.startsWith(nRoot + nodePath.sep);
    });
}

async function analyzeLocalPath(rawPath) {
    const result = { valid: false, normalized: '', warnings: [], error: null };

    if (!rawPath) { result.error = 'Path is required'; return result; }

    let p = rawPath;

    if (p.includes('\0')) { result.error = 'Invalid path: contains null bytes'; return result; }

    const trimmed = p.trim();
    if (trimmed !== p) { result.warnings.push('Leading/trailing whitespace was trimmed'); p = trimmed; }
    if (!p) { result.error = 'Path is empty after trimming whitespace'; return result; }

    const isUNC = p.startsWith('\\\\') || p.startsWith('//');
    const checkPart = isUNC ? p.substring(2) : p;
    if (/[\\/]{2,}/.test(checkPart)) { result.warnings.push('Duplicate path separators were normalized'); }

    let normalized;
    try { normalized = nodePath.resolve(p); } catch (_) { result.error = 'Invalid path format'; return result; }

    // Check for traversal AFTER normalization (handles ....// tricks etc.)
    const normalizedLower = normalized.toLowerCase();
    if (normalizedLower.includes('..')) {
        result.error = 'Invalid path: directory traversal (..) is not allowed';
        return result;
    }

    // Check against allowed roots
    if (!isPathWithinRoots(normalized)) {
        result.error = 'Access denied: path is outside allowed directories';
        return result;
    }

    const segments = normalized.split(/[\\/]/).filter(Boolean);
    if (segments.length > 10) { result.warnings.push('Path is deeply nested (' + segments.length + ' levels)'); }
    if (/\s/.test(normalized)) { result.warnings.push('Path contains spaces (may cause issues with some tools)'); }

    result.normalized = normalized;

    try {
        const stat = await fsPromises.stat(normalized);
        if (!stat.isDirectory()) {
            result.error = stat.isFile()
                ? 'Path points to a file, not a directory. Please select a folder'
                : 'Path is neither a file nor a directory';
            return result;
        }
    } catch (e) {
        if (e.code === 'ENOENT') { result.error = 'Path does not exist: ' + normalized; return result; }
        result.error = (e.code === 'EACCES' || e.code === 'EPERM')
            ? 'Permission denied: cannot access this path' : 'Cannot access path: ' + e.message;
        return result;
    }

    result.valid = true;
    return result;
}

async function listDir(dirPath, showHidden) {
    try {
        const raw = await fsPromises.readdir(dirPath, { withFileTypes: true });
        const entries = [];
        const fileStatPromises = [];
        for (const entry of raw) {
            if (!showHidden && entry.name.startsWith('.')) continue;
            const fullPath = nodePath.join(dirPath, entry.name);
            if (entry.isDirectory()) {
                entries.push({ name: entry.name, type: 'dir', path: fullPath });
            } else if (entry.isFile()) {
                const idx = entries.length;
                entries.push({ name: entry.name, type: 'file', path: fullPath, size: 0 });
                fileStatPromises.push(
                    fsPromises.stat(fullPath).then(st => { entries[idx].size = st.size; }).catch(() => {})
                );
            }
        }
        await Promise.all(fileStatPromises);
        entries.sort((a, b) => {
            if (a.type !== b.type) return a.type === 'dir' ? -1 : 1;
            return a.name.localeCompare(b.name, undefined, { sensitivity: 'base' });
        });
        return entries;
    } catch (e) {
        log.warn('listDir failed:', dirPath, e.code || e.message);
        return [];
    }
}

// ─── Error classification for structured reporting ───
function classifyError(errMsg) {
    if (!errMsg) return 'UNKNOWN';
    const s = String(errMsg);
    if (/\b40[13]\b|unauthorized|forbidden/i.test(s)) return 'AUTH';
    if (/\b404\b|not.found/i.test(s)) return 'NOT_FOUND';
    if (/\b413\b|too.large|payload.*large|size.limit/i.test(s)) return 'SIZE_LIMIT';
    if (/\b(500|502|503|504)\b|server.error|gateway|service.unavailable/i.test(s)) return 'SERVER';
    if (/ECONNREFUSED|ECONNRESET|ETIMEDOUT|ENOTFOUND|socket.hang/i.test(s)) return 'NETWORK';
    if (/invalid.*name|invalid.*char|illegal.*char/i.test(s)) return 'INVALID_NAME';
    if (/timeout|timed.out/i.test(s)) return 'TIMEOUT';
    return 'UNKNOWN';
}

// ─── Duplicate upload tracking ───
const activeUploads = new Map();

function getUploadKey(baseUrl, damPath, localPath) {
    return `${baseUrl}|${damPath}|${localPath}`;
}

// Clamp numeric upload params to safe ranges
function clampUploadParams(raw) {
    const intOrDefault = (val, def) => { const n = parseInt(val); return Number.isNaN(n) ? def : n; };
    return {
        maxConcurrent: Math.min(Math.max(intOrDefault(raw.maxConcurrent, 4), 1), 10),
        maxUploadFiles: Math.min(Math.max(intOrDefault(raw.maxUploadFiles, 50000), 1), 200000),
        httpRetryCount: Math.min(Math.max(intOrDefault(raw.httpRetryCount, 5), 0), 10),
        httpRetryDelay: Math.min(Math.max(intOrDefault(raw.httpRetryDelay, 8000), 0), 60000),
        httpRequestTimeout: Math.min(Math.max(intOrDefault(raw.httpRequestTimeout, 120000), 5000), 600000),
        uploadPipelines: Math.min(Math.max(intOrDefault(raw.uploadPipelines, DEFAULT_UPLOAD_PIPELINES), 1), 16),
    };
}

// ─── API: sign in / out (team member accounts, see auth.js) ───
app.post('/api/auth/login', loginLimiter, (req, res) => {
    const { email, password } = req.body || {};
    if (!email || !password) return res.status(400).json({ error: 'Enter your email and password.' });
    if (!auth.isAllowedDomain(auth.normalizeEmail(email))) {
        return res.status(400).json({ error: `Use your Adobe email address (${auth.domainHint()}).`, code: 'EMAIL_DOMAIN' });
    }
    let u;
    try {
        u = auth.checkPassword(email, password);
    } catch (e) {
        log.error('Sign-in failed:', e.message);
        return res.status(500).json({ error: 'Sign-in is not available right now. Contact an admin.' });
    }
    if (!u) {
        log.warn(`Sign-in failed for ${auth.normalizeEmail(email)}`);
        return res.status(401).json({ error: 'Wrong email or password.', code: 'AUTH_FAILED' });
    }
    if (u.disabled) {
        log.warn(`Sign-in refused for disabled account ${auth.normalizeEmail(email)}`);
        return res.status(403).json({ error: 'This account is disabled. Contact an admin.', code: 'ACCOUNT_DISABLED' });
    }
    const s = auth.createSession(req, res, u);
    auth.recordSignIn(u.email);
    log.info(`Signed in: ${u.email}${u.role === 'admin' ? ' (admin)' : ''}${u.mustChangePassword ? ' (must change password)' : ''}`);
    res.json({ user: auth.publicUser(s) });
});

app.get('/api/auth/options', (_req, res) => {
    res.json({ ...auth.authOptions(), resetByEmail: mailer.mailEnabled() });
});

app.post('/api/auth/register', accountLimiter, (req, res) => {
    try {
        const u = auth.registerUser(req.body || {});
        const s = auth.createSession(req, res, u);
        auth.recordSignIn(u.email);
        log.info(`Account created: ${u.email}${u.role === 'admin' ? ' (admin)' : ''}`);
        res.status(201).json({ user: auth.publicUser(s) });
    } catch (e) {
        res.status(e.status || 500).json({ error: e.message, code: e.code });
    }
});

function appOrigin(req) {
    return process.env.PUBLIC_URL ? process.env.PUBLIC_URL.replace(/\/+$/, '') : `${req.protocol}://${req.get('host')}`;
}

// The answer is the same whether or not the account exists
const forgotByEmail = new Map();
const FORGOT_PER_EMAIL_PER_HOUR = 5;

app.post('/api/auth/forgot', accountLimiter, async (req, res) => {
    const key = auth.normalizeEmail((req.body || {}).email);
    const now = Date.now();
    const recent = (forgotByEmail.get(key) || []).filter((t) => now - t < 60 * 60 * 1000);
    if (recent.length >= FORGOT_PER_EMAIL_PER_HOUR) {
        return res.status(429).json({ error: 'A reset was already requested several times for this email. Try again in an hour, or ask an admin.' });
    }
    recent.push(now);
    if (forgotByEmail.size > 10000) forgotByEmail.clear();
    forgotByEmail.set(key, recent);
    let request;
    try {
        request = auth.requestPasswordReset((req.body || {}).email);
    } catch (e) {
        return res.status(e.status || 500).json({ error: e.message, code: e.code });
    }
    const viaEmail = mailer.mailEnabled();
    if (request) {
        log.info(`Password reset requested: ${request.user.email}`);
        if (viaEmail) {
            try {
                await mailer.sendPasswordReset(request.user.email, request.user.name, `${appOrigin(req)}/#reset=${request.token}`);
            } catch (e) {
                log.error(`Reset email to ${request.user.email} failed: ${e.message}`);
            }
        }
    }
    res.json({
        viaEmail,
        message: viaEmail
            ? 'If an account exists for this email, a reset link is on its way. It works for 1 hour.'
            : 'Request sent. An admin will reset your password and give you a temporary one.',
    });
});

app.post('/api/auth/reset', accountLimiter, (req, res) => {
    try {
        const b = req.body || {};
        const account = auth.resetPasswordWithToken(String(b.token || ''), b.password);
        log.info(`Password reset with emailed link: ${account.email}`);
        res.json({ message: 'Password changed. Sign in with your new password.', email: account.email });
    } catch (e) {
        res.status(e.status || 500).json({ error: e.message });
    }
});

app.get('/api/auth/me', (req, res) => {
    if (!req.user) return res.status(401).json({ error: 'Not signed in', code: 'AUTH_REQUIRED' });
    res.json({ user: req.user });
});

app.post('/api/auth/logout', (req, res) => {
    if (req.user && !req.user.viaApiKey) log.info(`Signed out: ${req.user.key}`);
    auth.destroySession(req, res);
    res.json({ message: 'Signed out' });
});

app.post('/api/auth/change-password', loginLimiter, (req, res) => {
    if (!req.user || req.user.viaApiKey) return res.status(401).json({ error: 'Not signed in', code: 'AUTH_REQUIRED' });
    try {
        const account = auth.changePassword(req.user.email, req.body && req.body.currentPassword, req.body && req.body.newPassword);
        const s = auth.passwordChanged(req, account);
        log.info(`Password changed: ${account.email}`);
        res.json({ user: s ? auth.publicUser(s) : req.user });
    } catch (e) {
        res.status(e.status || 500).json({ error: e.message });
    }
});

// ─── API: team members (admins) ───
app.get('/api/admin/users', auth.requireAdmin, (_req, res) => {
    res.json({ users: auth.listUsers() });
});

app.post('/api/admin/users', auth.requireAdmin, (req, res) => {
    try {
        const { user, temporaryPassword } = auth.addUser({ ...req.body, createdBy: req.user.email });
        log.info(`Team member added: ${user.email} (${user.role}) by ${req.user.email}`);
        res.status(201).json({ user, temporaryPassword });
    } catch (e) {
        res.status(e.status || 500).json({ error: e.message });
    }
});

app.post('/api/admin/users/:email', auth.requireAdmin, (req, res) => {
    const b = req.body || {};
    const target = auth.normalizeEmail(req.params.email);
    if (target === req.user.email && (b.disabled === true || b.role === 'member')) {
        return res.status(400).json({ error: 'You cannot disable or demote your own account.' });
    }
    try {
        const changes = {};
        if (b.role === 'admin' || b.role === 'member') changes.role = b.role;
        if (typeof b.disabled === 'boolean') changes.disabled = b.disabled;
        if (b.resetPassword === true) changes.resetPassword = true;
        const { user, temporaryPassword } = auth.updateUser(target, changes);
        log.info(`Team member updated: ${user.email} ${JSON.stringify(changes)} by ${req.user.email}`);
        res.json({ user, temporaryPassword });
    } catch (e) {
        res.status(e.status || 500).json({ error: e.message });
    }
});

// ─── API: health check ───
// Changes when a new version of the page is deployed: open tabs offer to refresh (see checkConnection in the page)
const BUILD_ID = (() => {
    try { return crypto.createHash('sha1').update(fs.readFileSync(nodePath.join(__dirname, 'public', 'index.html'))).digest('hex').slice(0, 12); } catch (_) { return String(Date.now()); }
})();

app.get('/api/health', (_req, res) => {
    res.json({
        status: 'ok',
        version: BUILD_ID,
        uptime: Math.floor(process.uptime()),
        activeUploads: uploadSessions.size,
        historyCount: uploadHistory.length,
        platform: process.platform,
        sep: nodePath.sep,
        authEnabled: !!API_KEY,
        signInRequired: true,
    });
});

// ─── API: list preset targets ───
app.get('/api/targets', (_req, res) => {
    const list = Object.values(TARGETS).map(t => ({
        id: t.id,
        label: t.label,
        baseUrl: t.baseUrl,
        damPath: t.damPath,
    }));
    res.json(list);
});

// ─── API: scan local directory ───
app.get('/api/scan', auth.requireAdmin, async (req, res) => {
    const analysis = await analyzeLocalPath(req.query.localPath);
    if (!analysis.valid) {
        log.debug('Scan rejected — invalid path:', req.query.localPath, analysis.error);
        return res.status(400).json({ error: analysis.error, warnings: analysis.warnings });
    }

    try {
        const startMs = Date.now();
        const result = await scanDirectory(analysis.normalized);
        const elapsed = Date.now() - startMs;
        log.info(`Scan complete: ${analysis.normalized} — ${result.fileCount} files, ${result.folderCount} folders, ${(result.totalSize / 1048576).toFixed(1)} MB (${elapsed}ms)`);
        res.json(result);
    } catch (e) {
        log.error('Scan error:', analysis.normalized, e.message);
        res.status(500).json({ error: e.message });
    }
});

// ─── API: start upload ───
// "/content/dam/en/", "content\dam\en" and a pasted Assets URL all mean content/dam/en
function useNormalizedDamPath(req) {
    if (req.body && typeof req.body.damPath === 'string') {
        const dam = AemNames.normalizeDamPath(req.body.damPath);
        if (!dam.error) req.body.damPath = dam.path;
    }
}

app.post('/api/upload', uploadLimiter, auth.requireAdmin, async (req, res) => {
    useNormalizedDamPath(req);
    const { baseUrl, username, password } = auth.resolveAemTarget(req);
    const {
        targetId, damPath, localPath: rawLocalPath,
        maxConcurrent, maxUploadFiles,
        httpRetryCount, httpRetryDelay, httpRequestTimeout
    } = req.body;

    // Validation, the duplicate guard and the upload must all use the same path
    const localPath = typeof rawLocalPath === 'string' && rawLocalPath.trim()
        ? nodePath.resolve(rawLocalPath.trim()) : rawLocalPath;

    // Resolve target
    let resolvedBaseUrl = baseUrl;
    let resolvedDamPath = damPath;
    let targetLabel = 'Custom';

    if (targetId && TARGETS[targetId]) {
        if (!resolvedBaseUrl) resolvedBaseUrl = TARGETS[targetId].baseUrl;
        if (!resolvedDamPath) resolvedDamPath = TARGETS[targetId].damPath;
        targetLabel = TARGETS[targetId].label;
    }

    if (targetLabel === 'Custom' && resolvedBaseUrl) {
        const match = Object.values(TARGETS).find(t => t.baseUrl === resolvedBaseUrl);
        if (match) targetLabel = match.label;
    }

    if (!resolvedBaseUrl || !resolvedDamPath) {
        return res.status(400).json({ error: 'Missing baseUrl or damPath' });
    }

    resolvedBaseUrl = normalizeAemBaseUrl(resolvedBaseUrl);

    // Pre-flight input validation
    const configErr = validateUploadConfig({
        baseUrl: resolvedBaseUrl,
        damPath: resolvedDamPath,
        localPaths: [localPath],
        username,
        password,
    });
    if (configErr) return res.status(400).json({ error: configErr });

    const credCheck = await checkAemCredentials(resolvedBaseUrl, username, password);
    if (!credCheck.ok) return res.status(400).json({ error: credCheck.error, code: credCheck.code });

    // Validate local path
    const pathAnalysis = await analyzeLocalPath(localPath);
    if (!pathAnalysis.valid) return res.status(400).json({ error: pathAnalysis.error });

    // Duplicate prevention
    const uploadKey = getUploadKey(resolvedBaseUrl, resolvedDamPath, localPath);
    if (activeUploads.has(uploadKey)) {
        return res.status(409).json({
            error: 'An upload to this target with this path is already running',
            existingUploadId: activeUploads.get(uploadKey),
        });
    }

    const uploadId = crypto.randomUUID();
    const startTime = Date.now();
    const session = {
        listeners: new Set(),
        events: [],
        done: false,
        cancelFn: null,
        totalBytes: 0,
        transferredBytes: 0,
        totalFiles: 0,
        completedFiles: 0,
        failedFiles: 0,
        fileErrors: {},
        meta: {
            owner: req.user.key,
            ownerName: req.user.name,
            target: targetLabel,
            baseUrl: resolvedBaseUrl,
            damPath: resolvedDamPath,
            localPath,
            startTime,
        },
        uploadKey,
    };
    uploadSessions.set(uploadId, session);
    activeUploads.set(uploadKey, uploadId);

    res.json({ uploadId });

    const broadcast = createBroadcast(session);

    const clamped = clampUploadParams({ maxConcurrent, maxUploadFiles, httpRetryCount, httpRetryDelay, httpRequestTimeout });
    const uploadConfig = {
        baseUrl: resolvedBaseUrl,
        damPath: resolvedDamPath,
        localPaths: [localPath],
        username,
        password,
        ...clamped,
    };

    log.info(`Upload started: ${uploadId} → ${resolvedBaseUrl}/${resolvedDamPath}`);

    const { promise: uploadPromise, cancel: cancelFn } = runUpload(uploadConfig, (eventName, data) => {
        if (eventName === 'uploadplan') {
            session.meta.expectedFiles = data.files;
            session.meta.expectedBytes = data.bytes;
            broadcast('uploadplan', { expectedFiles: data.files, expectedBytes: data.bytes });
            return;
        }
        session.totalBytes = data.totalBytes || session.totalBytes;
        session.transferredBytes = data.transferredBytes || session.transferredBytes;
        if (eventName === 'filestart') session.totalFiles++;
        if (eventName === 'fileend') session.completedFiles++;
        if (eventName === 'fileerror') {
            session.completedFiles++;
            session.failedFiles++;
            const errMsg = (data.errors && data.errors[0]) ? (typeof data.errors[0] === 'string' ? data.errors[0] : data.errors[0].message || String(data.errors[0])) : 'Unknown error';
            if (Object.keys(session.fileErrors).length < FILE_ERRORS_CAP) session.fileErrors[data.fileName] = { message: errMsg, code: classifyError(errMsg) };
        }

        broadcast(eventName, {
            fileName: data.fileName,
            fileSize: data.fileSize,
            transferred: data.transferred,
            totalBytes: data.totalBytes,
            transferredBytes: data.transferredBytes,
            errors: data.errors ? data.errors.map(e => (typeof e === 'string' ? e : e.message || String(e))) : undefined,
        });
    });

    // Store cancel function
    session.cancelFn = cancelFn;
    session.runSettled = false;
    // Cancel can arrive between the response with the uploadId and this point
    if (session.userCancelled) cancelFn();

    uploadPromise.then((result) => {
        if (session.userCancelled) {
            log.info(`Upload ${uploadId} finished after user cancel; skipping duplicate done/history`);
            return;
        }
        const duration = Date.now() - startTime;
        log.info(`Upload ${result.success ? 'completed' : 'finished with errors'}: ${uploadId} (${result.totalFiles} files, ${duration}ms)`);
        try {
            broadcast('done', {
                success: result.success,
                partial: result.partial,
                totalFiles: result.totalFiles,
                totalBytes: result.totalBytes,
                transferredBytes: result.transferredBytes,
                errorCount: result.errors.length,
                errors: result.errors,
                fileErrors: session.fileErrors,
                cancelled: result.cancelled,
                duration,
            });
        } catch (e) { log.error('broadcast error in .then:', e.message); }

        addHistoryRow(session.meta, {
            uploadId,
            target: targetLabel,
            baseUrl: resolvedBaseUrl,
            damPath: resolvedDamPath,
            localPath,
            totalFiles: result.totalFiles,
            totalBytes: result.totalBytes,
            success: result.success,
            partial: result.partial,
            errorCount: result.errors.length,
            errors: result.errors.slice(0, 20),
            cancelled: result.cancelled,
            duration,
            timestamp: new Date().toISOString(),
        });
        trimHistory();
        saveHistoryToDisk();
    }).catch((err) => {
        if (session.userCancelled) {
            log.info(`Upload ${uploadId} rejected after user cancel; ignoring:`, err.message);
            return;
        }
        const duration = Date.now() - startTime;
        log.error(`Upload crashed: ${uploadId}`, err.message);
        try {
            broadcast('done', {
                success: false,
                totalFiles: 0,
                errorCount: 1,
                errors: [err.message || String(err)],
                duration,
            });
        } catch (e) { log.error('broadcast error in .catch:', e.message); }
    }).finally(() => {
        session.done = true;
        session.runSettled = true;
        session.doneTime = Date.now();
        if (activeUploads.get(uploadKey) === uploadId) activeUploads.delete(uploadKey);
        // Free heavy buffers after SSE clients have had time to receive the done event
        setTimeout(() => {
            session.events = [];
            session.fileErrors = {};
        }, 60000);
    });
});

// ─── API: browser-based file upload (files sent from user's browser) ───
app.post('/api/upload/browser', uploadLimiter, browserUpload.any(), async (req, res) => {
    const tempDir = req._tempDir;
    if (!req.files || req.files.length === 0) {
        if (tempDir) cleanupTempDir(tempDir);
        return res.status(400).json({ error: 'No files received' });
    }

    useNormalizedDamPath(req);
    const { baseUrl, username, password } = auth.resolveAemTarget(req);
    const { damPath,
            maxConcurrent, maxUploadFiles,
            httpRetryCount, httpRetryDelay, httpRequestTimeout } = req.body;

    if (!baseUrl || !damPath) {
        cleanupTempDir(tempDir);
        return res.status(400).json({ error: 'Missing baseUrl or damPath' });
    }
    if (!username || !password) {
        cleanupTempDir(tempDir);
        return res.status(400).json({ error: 'Missing credentials' });
    }

    const normalizedBaseUrl = normalizeAemBaseUrl(baseUrl);

    // Validate URL
    const configErr = validateUploadConfig({
        baseUrl: normalizedBaseUrl, damPath, localPaths: [tempDir], username, password,
    });
    if (configErr) {
        cleanupTempDir(tempDir);
        return res.status(400).json({ error: configErr });
    }

    const credCheck = await checkAemCredentials(normalizedBaseUrl, username, password);
    if (!credCheck.ok) {
        cleanupTempDir(tempDir);
        return res.status(400).json({ error: credCheck.error, code: credCheck.code });
    }

    log.info(`Browser upload received: ${req.files.length} files, tempDir=${tempDir}`);
    if (req.body._relativePaths) {
        try {
            const rp = JSON.parse(req.body._relativePaths);
            log.info(`Relative paths (first 5): ${rp.slice(0, 5).join(', ')}${rp.length > 5 ? ' ...' : ''}`);
        } catch (_) { log.warn('_relativePaths present but not valid JSON'); }
    } else {
        log.warn('No _relativePaths field received — folder structure may be lost');
    }

    // Rearrange files from flat staging into proper folder structure
    let contentDir;
    try {
        contentDir = await rearrangeUploadedFiles(tempDir, req.files, req.body._relativePaths);
        log.info(`Files rearranged into folder structure at: ${contentDir}`);
    } catch (e) {
        log.error('Failed to rearrange uploaded files:', e.message);
        cleanupTempDir(tempDir);
        return res.status(500).json({ error: 'Failed to process uploaded files: ' + e.message });
    }

    // Determine target label
    let targetLabel = 'Custom';
    const match = Object.values(TARGETS).find(t => t.baseUrl === normalizedBaseUrl);
    if (match) targetLabel = match.label;

    // Duplicate prevention
    const uploadKey = getUploadKey(normalizedBaseUrl, damPath, 'browser-' + tempDir);
    if (activeUploads.has(uploadKey)) {
        cleanupTempDir(tempDir);
        return res.status(409).json({ error: 'An upload to this target is already running' });
    }

    const uploadId = crypto.randomUUID();
    const startTime = Date.now();
    const totalUploadedBytes = req.files.reduce((sum, f) => sum + f.size, 0);

    const session = {
        listeners: new Set(),
        events: [],
        done: false,
        cancelFn: null,
        totalBytes: 0,
        transferredBytes: 0,
        totalFiles: 0,
        completedFiles: 0,
        failedFiles: 0,
        fileErrors: {},
        tempDir,
        meta: {
            owner: req.user.key,
            ownerName: req.user.name,
            target: targetLabel,
            baseUrl: normalizedBaseUrl,
            damPath,
            localPath: 'Browser upload (' + req.files.length + ' files, ' + Math.round(totalUploadedBytes / 1048576) + ' MB)',
            expectedFiles: req.files.length,
            expectedBytes: totalUploadedBytes,
            startTime,
        },
        uploadKey,
    };
    uploadSessions.set(uploadId, session);
    activeUploads.set(uploadKey, uploadId);

    res.json({ uploadId });

    const broadcast = createBroadcast(session);

    const clamped = clampUploadParams({ maxConcurrent, maxUploadFiles, httpRetryCount, httpRetryDelay, httpRequestTimeout });

    // List children of contentDir so the library uploads contents directly
    // into the DAM path without creating an extra wrapper folder.
    let contentChildren = (await fsPromises.readdir(contentDir)).map(name => nodePath.join(contentDir, name));

    // Prevent duplicate folder nesting (see finalize handler for detailed comment)
    if (contentChildren.length === 1) {
        const childName = nodePath.basename(contentChildren[0]);
        const damLastSeg = damPath.replace(/\/+$/, '').split('/').pop();
        const childStat = await fsPromises.stat(contentChildren[0]);
        if (childStat.isDirectory() && childName === damLastSeg) {
            log.info(`Unwrapping duplicate root folder "${childName}" (matches damPath tail)`);
            contentChildren = (await fsPromises.readdir(contentChildren[0])).map(name => nodePath.join(contentChildren[0], name));
        }
    }

    const uploadConfig = {
        baseUrl: normalizedBaseUrl,
        damPath,
        localPaths: contentChildren,
        username,
        password,
        ...clamped,
    };

    log.info(`Browser upload started: ${uploadId} (${req.files.length} files, ${contentChildren.length} top-level paths) → ${normalizedBaseUrl}/${damPath}`);

    const { promise: uploadPromise, cancel: cancelFn } = runUpload(uploadConfig, (eventName, data) => {
        if (eventName === 'uploadplan') {
            session.meta.expectedFiles = data.files;
            session.meta.expectedBytes = data.bytes;
            broadcast('uploadplan', { expectedFiles: data.files, expectedBytes: data.bytes });
            return;
        }
        session.totalBytes = data.totalBytes || session.totalBytes;
        session.transferredBytes = data.transferredBytes || session.transferredBytes;
        if (eventName === 'filestart') session.totalFiles++;
        if (eventName === 'fileend') session.completedFiles++;
        if (eventName === 'fileerror') {
            session.completedFiles++;
            session.failedFiles++;
            const errMsg = (data.errors && data.errors[0]) ? (typeof data.errors[0] === 'string' ? data.errors[0] : data.errors[0].message || String(data.errors[0])) : 'Unknown error';
            if (Object.keys(session.fileErrors).length < FILE_ERRORS_CAP) session.fileErrors[data.fileName] = { message: errMsg, code: classifyError(errMsg) };
        }
        broadcast(eventName, {
            fileName: data.fileName,
            fileSize: data.fileSize,
            transferred: data.transferred,
            totalBytes: data.totalBytes,
            transferredBytes: data.transferredBytes,
            errors: data.errors ? data.errors.map(e => (typeof e === 'string' ? e : e.message || String(e))) : undefined,
        });
    });

    session.cancelFn = cancelFn;
    session.runSettled = false;
    // Cancel can arrive between the response with the uploadId and this point
    if (session.userCancelled) cancelFn();

    uploadPromise.then((result) => {
        if (session.userCancelled) {
            log.info(`Browser upload ${uploadId} finished after user cancel; skipping duplicate done/history`);
            return;
        }
        const duration = Date.now() - startTime;
        log.info(`Browser upload ${result.success ? 'completed' : 'finished with errors'}: ${uploadId} (${result.totalFiles} files, ${duration}ms)`);
        try {
            broadcast('done', {
                success: result.success,
                partial: result.partial,
                totalFiles: result.totalFiles,
                totalBytes: result.totalBytes,
                transferredBytes: result.transferredBytes,
                errorCount: result.errors.length,
                errors: result.errors,
                fileErrors: session.fileErrors,
                cancelled: result.cancelled,
                duration,
            });
        } catch (e) { log.error('broadcast error in browser .then:', e.message); }

        addHistoryRow(session.meta, {
            uploadId,
            target: targetLabel,
            baseUrl: normalizedBaseUrl,
            damPath,
            localPath: session.meta.localPath,
            totalFiles: result.totalFiles,
            totalBytes: result.totalBytes,
            success: result.success,
            partial: result.partial,
            errorCount: result.errors.length,
            errors: result.errors.slice(0, 20),
            cancelled: result.cancelled,
            duration,
            timestamp: new Date().toISOString(),
        });
        trimHistory();
        saveHistoryToDisk();
    }).catch((err) => {
        if (session.userCancelled) {
            log.info(`Browser upload ${uploadId} rejected after user cancel; ignoring:`, err.message);
            return;
        }
        const duration = Date.now() - startTime;
        log.error(`Browser upload crashed: ${uploadId}`, err.message);
        try {
            broadcast('done', {
                success: false, totalFiles: 0, errorCount: 1,
                errors: [err.message || String(err)], duration,
            });
        } catch (e) { log.error('broadcast error in browser .catch:', e.message); }
    }).finally(() => {
        session.done = true;
        session.runSettled = true;
        session.doneTime = Date.now();
        if (activeUploads.get(uploadKey) === uploadId) activeUploads.delete(uploadKey);
        setTimeout(() => cleanupTempDir(tempDir).catch(() => {}), 30000);
        setTimeout(() => {
            session.events = [];
            session.fileErrors = {};
        }, 60000);
    });
});

// ─── API: chunked browser upload — init session ───
app.post('/api/upload/browser/init', uploadLimiter, async (req, res) => {
    useNormalizedDamPath(req);
    const { baseUrl, username, password } = auth.resolveAemTarget(req);
    const { damPath,
            maxConcurrent, maxUploadFiles,
            httpRetryCount, httpRetryDelay, httpRequestTimeout,
            totalBytes, fileCount, skipUnchanged, keepNodeNames } = req.body;

    if (!baseUrl || !damPath) {
        return res.status(400).json({ error: 'Missing baseUrl or damPath' });
    }
    if (!username || !password) {
        return res.status(400).json({ error: 'Missing credentials' });
    }

    const normalizedInitUrl = normalizeAemBaseUrl(baseUrl);

    const configErr = validateUploadConfig({
        baseUrl: normalizedInitUrl, damPath, localPaths: [os.tmpdir()], username, password,
    });
    if (configErr) {
        return res.status(400).json({ error: configErr });
    }

    const credCheck = await checkAemCredentials(normalizedInitUrl, username, password);
    if (!credCheck.ok) {
        return res.status(400).json({ error: credCheck.error, code: credCheck.code });
    }

    if (Number.isFinite(totalBytes) && Number.isFinite(fileCount)) {
        const spaceErr = await checkTempSpace(totalBytes, fileCount);
        if (spaceErr) {
            log.warn(`Rejected upload of ${fileCount} files: ${spaceErr}`);
            return res.status(507).json({ error: spaceErr, code: 'INSUFFICIENT_SERVER_SPACE' });
        }
    }

    const sessionId = crypto.randomUUID();
    const tempDir = nodePath.join(os.tmpdir(), TEMP_DIR_PREFIX + 'chunk-' + sessionId.slice(0, 8));
    await fsPromises.mkdir(nodePath.join(tempDir, '__staging'), { recursive: true });

    browserUploadSessions.set(sessionId, {
        owner: req.user.key,
        tempDir,
        config: { baseUrl: normalizedInitUrl, damPath, username, password,
                  maxConcurrent, maxUploadFiles,
                  httpRetryCount, httpRetryDelay, httpRequestTimeout, skipUnchanged: skipUnchanged === true,
                  keepNodeNames: keepNodeNames === true },
        receivedFiles: 0,
        totalReceivedBytes: 0,
        relativePaths: [],
        files: [],
        createdAt: Date.now(),
    });

    log.info(`Browser chunk session created: ${sessionId}, tempDir=${tempDir}`);
    res.json({ sessionId });
});

// ─── API: content packages built from a folder sent by the browser (see lib/packages.js) ───
// The files arrive through the same chunk route as an upload: init here, then /api/upload/browser/chunk/:id.
const PACKAGE_FIELD = /^[A-Za-z0-9][A-Za-z0-9._ -]{0,79}$/;

app.post('/api/packages/init', uploadLimiter, async (req, res) => {
    useNormalizedDamPath(req);
    const b = req.body || {};
    const dam = AemNames.normalizeDamPath(b.damPath);
    if (dam.error) return res.status(400).json({ error: dam.error });
    // content/dam itself is fine: the package filter covers each top folder (/content/dam/<folder>), never all of /content/dam
    const packageName = String(b.packageName || '').trim();
    const version = String(b.version || '1.0').trim();
    const group = String(b.group || 'guides-upload').trim();
    if (!PACKAGE_FIELD.test(packageName)) return res.status(400).json({ error: 'Package name: letters, numbers, dot, dash, underscore or space.' });
    if (!PACKAGE_FIELD.test(version)) return res.status(400).json({ error: 'Version: letters, numbers, dot, dash or underscore.' });
    if (!/^[A-Za-z0-9][A-Za-z0-9._/ -]{0,79}$/.test(group) || group.includes('..')) return res.status(400).json({ error: 'Group: letters, numbers, dot, dash, underscore or slash.' });
    const totalBytes = Number(b.totalBytes) || 0;
    const fileCount = Number(b.fileCount) || 0;
    // staging + the zip itself
    const spaceErr = await checkTempSpace(totalBytes * 2, fileCount);
    if (spaceErr) return res.status(507).json({ error: spaceErr, code: 'INSUFFICIENT_SERVER_SPACE' });

    const sessionId = crypto.randomUUID();
    const tempDir = nodePath.join(os.tmpdir(), TEMP_DIR_PREFIX + 'pkg-' + sessionId.slice(0, 8));
    await fsPromises.mkdir(nodePath.join(tempDir, '__staging'), { recursive: true });
    browserUploadSessions.set(sessionId, {
        owner: req.user.key, mode: 'package', tempDir,
        config: { damPath: dam.path, packageName, version, group, description: String(b.description || '').slice(0, 500), keepNames: b.keepNames === true },
        receivedFiles: 0, totalReceivedBytes: 0, relativePaths: [], files: [], createdAt: Date.now(),
    });
    log.info(`Package session created: ${sessionId} (${packageName} ${version}) by ${req.user.key}`);
    res.json({ sessionId });
});

app.post('/api/packages/build/:sessionId', uploadLimiter, async (req, res) => {
    const session = browserUploadSessions.get(req.params.sessionId);
    if (!session || session.mode !== 'package' || !auth.canAccess(req, session.owner)) {
        return res.status(404).json({ error: 'Package session not found or expired' });
    }
    if (session.finalized) return res.status(409).json({ error: 'This package is already being built' });
    if (session.copy && session.copy.state !== 'done') return res.status(409).json({ error: 'The copy from the source AEM has not finished yet' });
    if (!session.files.length) return res.status(400).json({ error: 'No files were received' });
    session.finalized = true;
    let contentDir;
    try {
        contentDir = await rearrangeUploadedFiles(session.tempDir, session.files, session.relativePaths);
    } catch (e) {
        browserUploadSessions.delete(req.params.sessionId);
        cleanupTempDir(session.tempDir).catch(() => {});
        return res.status(500).json({ error: 'Could not prepare the files: ' + e.message });
    }
    const fileCount = session.files.length;
    session.files = []; session.relativePaths = [];
    browserUploadSessions.delete(req.params.sessionId);
    const { damPath, packageName, version, group, description, keepNames } = session.config;
    const record = packages.startBuild({
        owner: session.owner, ownerName: req.user.name, contentDir, damPath, packageName, version, group, description, fileCount, keepNames,
        copiedFrom: session.copy ? { baseUrl: session.copy.source.baseUrl, folder: session.copy.source.folder } : null,
    }, (err, p) => {
        cleanupTempDir(session.tempDir).catch(() => {});
        if (err) log.error(`Package build failed (${p.name} ${p.version}): ${err.message}`);
        else log.info(`Package built: ${p.downloadName} — ${p.files} files, ${(p.zipBytes / 1048576).toFixed(1)} MB in ${p.buildSeconds}s`);
    });
    res.status(202).json({ package: packages.publicPackage(record) });
});

app.get('/api/packages', (req, res) => {
    res.json({ packages: packages.list(req.user.key, !!req.user.isAdmin) });
});

function ownPackage(req, res) {
    const p = packages.get(req.params.id);
    if (!p || !auth.canAccess(req, p.owner)) { res.status(404).json({ error: 'Package not found' }); return null; }
    return p;
}

app.get('/api/packages/:id', (req, res) => {
    const p = ownPackage(req, res);
    if (p) res.json({ package: packages.publicPackage(p) });
});

app.get('/api/packages/:id/download', (req, res) => {
    const p = ownPackage(req, res);
    if (!p) return;
    if (p.state !== 'ready') return res.status(409).json({ error: 'The package is not ready yet' });
    if (p.copiedFrom && !COPY_ALLOW_DOWNLOAD) {
        return res.status(403).json({ error: 'Packages made from content copied from another AEM server can only be installed on an allowed server, not downloaded.' });
    }
    res.download(packages.fileOf(p), p.downloadName, { headers: { 'Cache-Control': 'no-store' } });
});

app.post('/api/packages/:id/install', uploadLimiter, async (req, res) => {
    const p = ownPackage(req, res);
    if (!p) return;
    if (p.state !== 'ready') return res.status(409).json({ error: 'The package is not ready yet' });
    const { baseUrl, username, password } = auth.resolveAemTarget(req);
    if (!baseUrl || !username || !password) return res.status(400).json({ error: 'Enter the AEM server, username and password' });
    const target = normalizeAemBaseUrl(baseUrl);
    if (p.copiedFrom && !isAllowedCopyTarget(target)) {
        return res.status(403).json({ error: copyTargetError(target), code: 'COPY_TARGET_NOT_ALLOWED' });
    }
    if (p.copiedFrom && originOf(p.copiedFrom.baseUrl) === originOf(target) && (p.filterRoots || []).some((r) => overlaps(r, p.copiedFrom.folder))) {
        return res.status(409).json({ error: `This package would replace ${p.copiedFrom.folder}, the folder it was copied from, on the same server.`, code: 'COPY_ONTO_SOURCE' });
    }
    const cred = await checkAemCredentials(target, username, password);
    if (!cred.ok) return res.status(400).json({ error: cred.error, code: cred.code });
    log.info(`Installing package ${p.downloadName} on ${target} as ${username} (by ${req.user.key})`);
    let result;
    try {
        result = await packages.install(p, { baseUrl: target, username, password });
    } catch (e) {
        result = { ok: false, message: e.name === 'AbortError' ? 'AEM did not answer within 30 minutes.' : 'Could not reach AEM: ' + e.message };
    }
    packages.recordInstall(p, { at: new Date().toISOString(), server: target, by: req.user.key, ok: result.ok, message: result.message });
    log.info(`Package install ${result.ok ? 'succeeded' : 'failed'}: ${p.downloadName} on ${target}: ${result.message}`);
    res.status(result.ok ? 200 : 502).json({ ...result, package: packages.publicPackage(p) });
});

// ─── API: AEM Guides readiness and DITA profiles (lib/guides-status.js, lib/guides-profile.js) ───
// Credentials come with the request, or from a copy session (the target of that copy), never stored.
function guidesServer(req) {
    const b = req.body || {};
    if (b.sessionId) {
        const session = browserUploadSessions.get(b.sessionId);
        if (!session || !auth.canAccess(req, session.owner) || !session.config || !session.config.username) return { error: 'Copy session not found or expired' };
        return { server: { baseUrl: originOf(session.config.baseUrl), username: session.config.username, password: session.config.password }, session };
    }
    const baseUrl = originOf(b.baseUrl);
    if (!baseUrl || !b.username || !b.password) return { error: 'Enter the AEM server, username and password' };
    return { server: { baseUrl, username: String(b.username), password: String(b.password) } };
}

function damFolder(raw) {
    const p = '/' + String(raw || '').trim().replace(/\\/g, '/').replace(/^\/+|\/+$/g, '');
    return /^\/content\/dam\/[^/]/.test(p) && !p.split('/').includes('..') ? p : null;
}

app.post('/api/guides/status', uploadLimiter, async (req, res) => {
    const g = guidesServer(req);
    if (g.error) return res.status(400).json({ error: g.error });
    const folder = damFolder(req.body.folder);
    if (!folder) return res.status(400).json({ error: 'Choose a folder inside /content/dam' });
    try {
        res.json({ status: await guidesStatus.folderStatus(g.server, folder) });
    } catch (e) {
        res.status(502).json({ error: e.name === 'TimeoutError' ? 'AEM did not answer in time' : e.message });
    }
});

app.post('/api/guides/reprocess', uploadLimiter, async (req, res) => {
    const g = guidesServer(req);
    if (g.error) return res.status(400).json({ error: g.error });
    const folder = damFolder(req.body.folder);
    if (!folder) return res.status(400).json({ error: 'Choose a folder inside /content/dam' });
    try {
        const r = await guidesStatus.reprocessFolder(g.server, folder);
        log.info(`Reprocess Assets started by ${req.user.key}: ${g.server.baseUrl}${folder}`);
        res.json(r);
    } catch (e) {
        res.status(502).json({ error: e.message });
    }
});

// Creating a DITA profile changes the AEM server's configuration: only on the allowed servers, never editing existing ones
function profileServer(req, res) {
    const g = guidesServer(req);
    if (g.error) { res.status(400).json({ error: g.error }); return null; }
    if (!isAllowedCopyTarget(g.server.baseUrl)) {
        res.status(403).json({ error: `DITA profiles can only be created from here on the allowed servers (${COPY_TARGETS.join(', ') || 'none set'}). Create it in AEM instead (Tools > Guides > DITA Profiles).`, code: 'PROFILE_SERVER_NOT_ALLOWED' });
        return null;
    }
    return g;
}

const profileInput = (b) => ({ name: b.name, paths: [].concat(b.paths || []), catalogs: [].concat(b.catalogs || []), ditaotZip: b.ditaotZip || '' });

app.post('/api/guides/profile/plan', uploadLimiter, async (req, res) => {
    const g = profileServer(req, res);
    if (!g) return;
    try {
        const plan = await guidesProfile.planProfile(g.server, profileInput(req.body));
        res.json({ ok: plan.ok, problems: plan.problems, notes: plan.notes, profile: plan.profile });
    } catch (e) {
        res.status(502).json({ error: e.message });
    }
});

app.post('/api/guides/profile/create', uploadLimiter, async (req, res) => {
    const g = profileServer(req, res);
    if (!g) return;
    try {
        const input = profileInput(req.body);
        const result = await guidesProfile.createProfile(g.server, input);
        log.info(`DITA profile ${result.ok ? 'created' : 'not created'} by ${req.user.key} on ${g.server.baseUrl}: "${input.name}" paths ${JSON.stringify(input.paths)} catalogs ${JSON.stringify(input.catalogs)}`
            + (result.problems ? ` problems ${JSON.stringify(result.problems)}` : '') + (result.errors && result.errors.length ? ` errors ${JSON.stringify(result.errors)}` : ''));
        res.status(result.problems ? 409 : 200).json(result);
    } catch (e) {
        res.status(502).json({ error: e.message });
    }
});

// ─── API: copy a folder from another AEM server (read-only on that server, see lib/copy-from-aem.js) ───
// The copied files fill an upload or package session exactly like files sent from the browser, so the normal
// finalize (upload) or package build runs afterwards. Copied content may only go to the servers in COPY_TARGETS.
const COPY_TARGETS = String(process.env.COPY_TARGETS || '').split(',').map((u) => originOf(u)).filter(Boolean);
const COPY_ALLOW_DOWNLOAD = /^(1|true|yes)$/i.test(process.env.COPY_ALLOW_DOWNLOAD || '');
const COPY_IDLE_MS = (parseInt(process.env.COPY_IDLE_MINUTES, 10) || 60) * 60 * 1000;

function originOf(url) {
    try {
        const u = new URL(normalizeAemBaseUrl(url));
        return /^https?:$/.test(u.protocol) ? u.origin.toLowerCase() : null;
    } catch (_) { return null; }
}

function isAllowedCopyTarget(url) {
    const o = originOf(url);
    return !!o && COPY_TARGETS.includes(o);
}

function copyTargetError(url) {
    return COPY_TARGETS.length
        ? `Copied content can only go to the allowed servers (${COPY_TARGETS.join(', ')}); ${originOf(url) || url} is not one of them.`
        : 'Copying from another AEM server is not set up: the admin has to list the allowed target servers in COPY_TARGETS.';
}

/** Where a copied folder lands: under damPath, or damPath itself when it already ends with the folder's name. */
function copyLanding(folder, damPath) {
    const name = folder.split('/').pop();
    const dam = String(damPath || '').replace(/^\/+|\/+$/g, '');
    return '/' + (dam.split('/').pop() === name ? dam : `${dam}/${name}`);
}

function overlaps(a, b) {
    return a === b || a.startsWith(b + '/') || b.startsWith(a + '/');
}

function copyLabel(copy) {
    return `Copied from ${new URL(copy.source.baseUrl).host}${copy.source.folder}`;
}

function publicCopy(copy) {
    const { abort, ...rest } = copy;
    return { ...rest, failed: rest.failed.slice(0, 100), failedCount: rest.failed.length };
}

app.get('/api/copy/options', (_req, res) => {
    res.json({ enabled: COPY_TARGETS.length > 0, targets: COPY_TARGETS, packageDownload: COPY_ALLOW_DOWNLOAD,
        defaultConcurrency: copyFromAem.DEFAULT_CONCURRENCY, maxConcurrency: copyFromAem.MAX_CONCURRENCY });
});

app.post('/api/copy/:sessionId/start', uploadLimiter, async (req, res) => {
    const session = browserUploadSessions.get(req.params.sessionId);
    if (!session || !auth.canAccess(req, session.owner)) return res.status(404).json({ error: 'Session not found or expired' });
    if (session.copy || session.finalized || session.files.length) return res.status(409).json({ error: 'This session already has files' });
    if (session.mode !== 'package' && !isAllowedCopyTarget(session.config.baseUrl)) {
        return res.status(403).json({ error: copyTargetError(session.config.baseUrl), code: 'COPY_TARGET_NOT_ALLOWED' });
    }
    if (!COPY_TARGETS.length) return res.status(403).json({ error: copyTargetError(''), code: 'COPY_TARGET_NOT_ALLOWED' });
    const b = req.body || {};
    const folder = copyFromAem.normalizeSourceFolder(b.folder);
    if (folder.error) return res.status(400).json({ error: folder.error });
    const sourceUrl = normalizeAemBaseUrl(b.sourceUrl);
    const origin = originOf(sourceUrl);
    const username = String(b.sourceUsername || '');
    const password = String(b.sourcePassword || '');
    if (!origin || !username || !password) return res.status(400).json({ error: 'Enter the source AEM server, username and password' });
    if (session.mode !== 'package' && originOf(session.config.baseUrl) === origin) {
        const landing = copyLanding(folder.path, session.config.damPath);
        if (overlaps(landing, folder.path)) {
            return res.status(409).json({ error: `The copy would land on ${landing}, inside the folder it is copied from on the same server. Choose another target folder.`, code: 'COPY_ONTO_SOURCE' });
        }
    }
    const cred = await checkAemCredentials(origin, username, password);
    if (!cred.ok) return res.status(400).json({ error: 'Source AEM: ' + cred.error, code: cred.code });

    const copy = {
        state: 'listing', source: { baseUrl: origin, folder: folder.path, username }, startedAt: new Date().toISOString(),
        total: 0, totalBytes: 0, done: 0, bytes: 0, verified: 0, unverified: 0, failed: [], abort: new AbortController(),
    };
    session.copy = copy;
    log.info(`Copy started by ${req.user.key}: ${origin}${folder.path} -> ${session.mode === 'package' ? 'package' : session.config.baseUrl}`);
    res.status(202).json({ copy: publicCopy(copy) });

    const stagingDir = nodePath.join(session.tempDir, '__staging');
    copyFromAem.copyFolder({
        baseUrl: origin, username, password, folder: folder.path, stagingDir, concurrency: b.concurrency, signal: copy.abort.signal,
        onListed: async ({ files, bytes }) => {
            Object.assign(copy, { state: 'copying', total: files, totalBytes: bytes });
            const spaceErr = await checkTempSpace(bytes * (session.mode === 'package' ? 2 : 1), files);
            if (spaceErr) { copy.spaceError = spaceErr; copy.abort.abort(); }
        },
        onFile: async (f) => {
            session.files.push({ path: f.path, originalname: f.originalname });
            session.relativePaths.push(f.relativePath);
            session.receivedFiles++;
            session.totalReceivedBytes += f.bytes;
        },
        onProgress: (p) => Object.assign(copy, { done: p.done, bytes: p.bytes }),
    }).then(async (sum) => {
        Object.assign(copy, { state: 'checking', done: sum.files, bytes: sum.bytes, verified: sum.verified, unverified: sum.unverified, failed: sum.failed, dita: sum.dita });
        log.info(`Copy finished: ${origin}${folder.path}: ${sum.files} files (${sum.verified} SHA-1 verified), ${sum.failed.length} failed, ${sum.dita.rewrittenLinks} GUID links rewritten`);
        // Checked now, while the source password is still in memory: it is never stored
        const targetFolder = copyLanding(folder.path, session.config && session.config.damPath);
        const target = session.mode === 'package' ? null
            : { baseUrl: session.config.baseUrl, username: session.config.username, password: session.config.password };
        try {
            copy.setup = await guidesSetup.checkSetup({
                source: { baseUrl: origin, username, password }, target, folder: folder.path, targetFolder,
                doctypes: sum.dita.doctypes, outside: sum.dita.outside,
            });
            copy.setup.targetFolder = targetFolder;
        } catch (e) {
            copy.setup = { error: 'The Guides setup could not be checked: ' + e.message, steps: [], ready: null };
        }
        Object.assign(copy, { state: 'done', finishedAt: Date.now() });
    }).catch((e) => {
        Object.assign(copy, { state: copy.spaceError ? 'failed' : e.cancelled ? 'cancelled' : 'failed', error: copy.spaceError || e.message, finishedAt: Date.now() });
        log.warn(`Copy ${copy.state}: ${origin}${folder.path}: ${copy.error}`);
        if (browserUploadSessions.get(req.params.sessionId) === session) {
            browserUploadSessions.delete(req.params.sessionId);
            cleanupTempDir(session.tempDir).catch(() => {});
        }
    });
});

// "Check again" after the target was set up (DTD folder copied, DITA profile saved): only the target side is read again
app.post('/api/copy/:sessionId/recheck', uploadLimiter, async (req, res) => {
    const session = browserUploadSessions.get(req.params.sessionId);
    if (!session || !session.copy || !auth.canAccess(req, session.owner)) return res.status(404).json({ error: 'No copy for this session' });
    const copy = session.copy;
    if (copy.state !== 'done' || !copy.setup || !copy.setup.source || session.mode === 'package') {
        return res.status(409).json({ error: 'Nothing to check again for this copy' });
    }
    try {
        copy.setup = Object.assign(await guidesSetup.checkSetup({
            previous: copy.setup, target: { baseUrl: session.config.baseUrl, username: session.config.username, password: session.config.password },
            folder: copy.source.folder, targetFolder: copy.setup.targetFolder, outside: (copy.dita && copy.dita.outside) || [],
        }), { targetFolder: copy.setup.targetFolder });
        copy.finishedAt = Date.now();
    } catch (e) {
        return res.status(502).json({ error: 'The Guides setup could not be checked: ' + e.message });
    }
    res.json({ copy: publicCopy(copy) });
});

app.get('/api/copy/:sessionId', (req, res) => {
    const session = browserUploadSessions.get(req.params.sessionId);
    if (!session || !session.copy || !auth.canAccess(req, session.owner)) return res.status(404).json({ error: 'No copy for this session' });
    res.json({ copy: publicCopy(session.copy) });
});

setInterval(() => {
    packages.sweepExpired().then((n) => { if (n) log.info(`Removed ${n} expired package(s)`); }).catch(() => {});
}, 60 * 60 * 1000).unref();

// ─── API: cancel chunked browser session (before finalize) ───
app.post('/api/upload/browser/cancel/:sessionId', uploadLimiter, (req, res) => {
    const ownedSession = browserUploadSessions.get(req.params.sessionId);
    const session = ownedSession && auth.canAccess(req, ownedSession.owner) ? ownedSession : null;
    if (!session) {
        const startedId = finalizedBrowserSessions.get(req.params.sessionId);
        const started = startedId && uploadSessions.get(startedId);
        if (started && !started.done && auth.canAccess(req, started.meta.owner)) {
            if (typeof started.cancelFn === 'function') { try { started.cancelFn(); } catch (_) {} }
            emitUserCancelledUpload(startedId, started, started.uploadKey);
            log.info(`Browser session ${req.params.sessionId} cancelled after its AEM upload ${startedId} started`);
            return res.json({ message: 'Upload cancelled', uploadId: startedId });
        }
        return res.status(404).json({ error: 'Upload session not found or already cancelled' });
    }
    if (session.finalized) {
        // Finalize checks this flag before starting the AEM upload
        session.cancelRequested = true;
        log.info(`Cancel requested for session ${req.params.sessionId} while finalizing`);
        return res.status(202).json({ message: 'Cancelling; the AEM upload will not start' });
    }
    const tempDir = session.tempDir;
    const sid = req.params.sessionId;
    if (session.copy && session.copy.abort) session.copy.abort.abort();
    browserUploadSessions.delete(sid);
    cleanupTempDir(tempDir).catch(() => {});
    log.info(`Browser chunk session cancelled: ${sid}`);
    res.json({ message: 'Session cancelled', sessionId: sid });
});

// ─── API: chunked browser upload — receive chunk ───
app.post('/api/upload/browser/chunk/:sessionId', (req, res, next) => {
    const ownedSession = browserUploadSessions.get(req.params.sessionId);
    const session = ownedSession && auth.canAccess(req, ownedSession.owner) ? ownedSession : null;
    if (!session) {
        return res.status(404).json({ error: 'Upload session not found or expired' });
    }
    if (session.finalized) {
        return res.status(409).json({ error: 'Session already finalized; no more chunks accepted' });
    }
    next();
}, chunkUpload.any(), (req, res) => {
    const session = browserUploadSessions.get(req.params.sessionId);
    if (!session) {
        return res.status(404).json({ error: 'Upload session not found or expired' });
    }

    if (!req.files || req.files.length === 0) {
        return res.status(400).json({ error: 'No files in chunk' });
    }

    // Parse relative paths for this chunk
    let chunkRelPaths = [];
    if (req.body._relativePaths) {
        try {
            chunkRelPaths = JSON.parse(req.body._relativePaths);
        } catch (_) {
            log.warn(`Chunk relative paths not valid JSON for session ${req.params.sessionId}`);
        }
    }

    // Accumulate files and paths
    const chunkSize = req.files.length;
    const chunkBytes = req.files.reduce((s, f) => s + f.size, 0);

    for (let i = 0; i < req.files.length; i++) {
        // Store only the fields rearrangeUploadedFiles needs, not the full Multer object
        session.files.push({ path: req.files[i].path, originalname: req.files[i].originalname });
        session.relativePaths.push(chunkRelPaths[i] || req.files[i].originalname);
    }
    session.receivedFiles += chunkSize;
    session.totalReceivedBytes += chunkBytes;

    const chunkIdx = parseInt(req.body.chunkIndex) || 0;
    const totalChunks = parseInt(req.body.totalChunks) || 1;

    log.info(`Chunk ${chunkIdx + 1}/${totalChunks} for session ${req.params.sessionId}: +${chunkSize} files (${(chunkBytes / 1048576).toFixed(1)} MB), total: ${session.receivedFiles} files (${(session.totalReceivedBytes / 1048576).toFixed(1)} MB)`);

    res.json({
        received: session.receivedFiles,
        receivedBytes: session.totalReceivedBytes,
        chunkIndex: chunkIdx,
        totalChunks,
    });
});

// ─── API: chunked browser upload — finalize and start AEM upload ───
// ─── Skip files that are already in AEM and unchanged ───
// One QueryBuilder search lists the assets under the DAM folder with dam:sha1 (set by AEM asset processing);
// a staged file whose AEM path and SHA-1 match is removed before the transfer. Assets without dam:sha1
// (not processed yet, e.g. installed from a package) are uploaded again: a changed file is never skipped.
const SKIP_QUERY_TIMEOUT_MS = 5 * 60 * 1000;

async function stagedTargets(contentChildren, damPath, keepNodeNames) {
    const fName = keepNodeNames ? AemNames.keepNodeName : AemNames.folderNodeName;
    const aName = keepNodeNames ? AemNames.keepNodeName : AemNames.assetNodeName;
    const out = [];
    const base = '/' + String(damPath).replace(/^\/+|\/+$/g, '');
    const walk = async (abs, remote) => {
        let st;
        try { st = await fsPromises.stat(abs); } catch (_) { return; }
        if (st.isFile()) { out.push({ local: abs, target: remote, size: st.size }); return; }
        if (!st.isDirectory()) return;
        for (const name of await fsPromises.readdir(abs)) {
            if (AemNames.isSystemName(name) || name === RESUME_JOB_FILE) continue;
            const child = nodePath.join(abs, name);
            let cst;
            try { cst = await fsPromises.stat(child); } catch (_) { continue; }
            await walk(child, remote + '/' + (cst.isDirectory() ? fName(name) : aName(name)));
        }
    };
    for (const c of contentChildren) {
        const name = nodePath.basename(c);
        let st;
        try { st = await fsPromises.stat(c); } catch (_) { continue; }
        await walk(c, base + '/' + (st.isDirectory() ? fName(name) : aName(name)));
    }
    return out;
}

function sha1OfFile(file) {
    return new Promise((resolve, reject) => {
        const h = crypto.createHash('sha1');
        fs.createReadStream(file).on('error', reject).on('data', (d) => h.update(d)).on('end', () => resolve(h.digest('hex')));
    });
}

async function aemAssetHashes(baseUrl, damPath, username, password) {
    const q = new URLSearchParams({
        path: '/' + String(damPath).replace(/^\/+|\/+$/g, ''), type: 'dam:Asset', 'p.limit': '-1', 'p.hits': 'selective',
        'p.properties': 'jcr:path jcr:content/metadata/dam:sha1',
    });
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), SKIP_QUERY_TIMEOUT_MS);
    try {
        const r = await fetch(`${baseUrl.replace(/\/+$/, '')}/bin/querybuilder.json?${q}`, {
            headers: { Authorization: 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64') }, signal: ac.signal,
        });
        if (!r.ok) throw new Error(`AEM search answered HTTP ${r.status}`);
        const j = await r.json();
        const map = new Map();
        for (const h of j.hits || []) {
            const sha = h['jcr:content'] && h['jcr:content'].metadata && h['jcr:content'].metadata['dam:sha1'];
            if (h['jcr:path'] && sha) map.set(h['jcr:path'], String(sha).toLowerCase());
        }
        return map;
    } finally {
        clearTimeout(timer);
    }
}

async function removeUnchangedStagedFiles({ baseUrl, damPath, username, password, contentChildren, keepNodeNames }) {
    let inAem;
    try {
        inAem = await aemAssetHashes(baseUrl, damPath, username, password);
    } catch (e) {
        return { skipped: 0, checked: 0, error: e.name === 'AbortError' ? 'AEM search timed out' : e.message };
    }
    const files = (await stagedTargets(contentChildren, damPath, keepNodeNames)).filter((f) => inAem.has(f.target));
    let skipped = 0;
    let next = 0;
    const worker = async () => {
        while (next < files.length) {
            const f = files[next++];
            try {
                if (await sha1OfFile(f.local) === inAem.get(f.target)) {
                    await fsPromises.unlink(f.local);
                    skipped++;
                }
            } catch (_) { /* unreadable: upload it */ }
        }
    };
    await Promise.all(Array.from({ length: 8 }, worker));
    return { skipped, checked: files.length };
}

// ─── Fair queue per AEM server ───
// At most MAX_UPLOADS_PER_SERVER uploads run against one AEM host at a time (each already sends 32 files at
// once); later ones wait in line and see their place. Uploads to other servers are not affected.
const MAX_UPLOADS_PER_SERVER = Math.max(1, parseInt(process.env.MAX_UPLOADS_PER_SERVER, 10) || 3);
const serverSlots = new Map();

function slotKey(baseUrl) {
    try { return new URL(baseUrl).host.toLowerCase(); } catch (_) { return String(baseUrl).toLowerCase(); }
}

function slotsFor(key) {
    if (!serverSlots.has(key)) serverSlots.set(key, { running: new Set(), waiting: [] });
    return serverSlots.get(key);
}

function announceQueue(key) {
    const s = slotsFor(key);
    s.waiting.forEach((w, i) => {
        w.session.queuePosition = i + 1;
        w.broadcast('queued', { position: i + 1, running: s.running.size, server: key });
    });
}

/** Resolves true when the upload may start, false when it was cancelled while waiting. */
function acquireUploadSlot(baseUrl, uploadId, session, broadcast) {
    const key = slotKey(baseUrl);
    const s = slotsFor(key);
    if (s.running.size < MAX_UPLOADS_PER_SERVER) {
        s.running.add(uploadId);
        return Promise.resolve(true);
    }
    return new Promise((resolve) => {
        s.waiting.push({ uploadId, session, broadcast, resolve });
        log.info(`Upload ${uploadId} waiting for ${key}: #${s.waiting.length} in line (${s.running.size} running)`);
        announceQueue(key);
    });
}

function releaseUploadSlot(baseUrl, uploadId) {
    const key = slotKey(baseUrl);
    const s = slotsFor(key);
    s.running.delete(uploadId);
    const i = s.waiting.findIndex((w) => w.uploadId === uploadId);
    if (i >= 0) s.waiting.splice(i, 1)[0].resolve(false);
    while (s.running.size < MAX_UPLOADS_PER_SERVER && s.waiting.length) {
        const next = s.waiting.shift();
        s.running.add(next.uploadId);
        next.session.queuePosition = 0;
        next.broadcast('dequeued', { server: key });
        log.info(`Upload ${next.uploadId} leaves the queue for ${key}`);
        next.resolve(true);
    }
    announceQueue(key);
    if (!s.running.size && !s.waiting.length) serverSlots.delete(key);
}

/**
 * Starts the AEM upload of files staged in tempDir (fresh from finalize, or what is left after a restart).
 * Returns { uploadId } or { status, error }; onStarted(uploadId) runs as soon as the upload exists.
 */
// The folder an upload lands in (for "Check in AEM"): damPath/<folder> when one folder was sent, else damPath
function landedFolder(damPath, contentChildren, keepNames) {
    const root = '/' + String(damPath || '').replace(/^\/+|\/+$/g, '');
    if (!contentChildren || contentChildren.length !== 1) return root;
    let st;
    try { st = fs.statSync(contentChildren[0]); } catch (_) { return root; }
    if (!st.isDirectory()) return root;
    const name = nodePath.basename(contentChildren[0]);
    return `${root}/${keepNames ? AemNames.keepNodeName(name) : AemNames.folderNodeName(name)}`;
}

async function startAemUpload({ owner, ownerName, baseUrl, damPath, username, password, settings, tempDir, contentChildren,
    receivedFiles, totalReceivedBytes, totalFilesOverall, sessionId, resumeOf, sourceLabel, onStarted }) {
    const { maxConcurrent, maxUploadFiles, httpRetryCount, httpRetryDelay, httpRequestTimeout, skipUnchanged, keepNodeNames } = settings;
    // Determine target label
    let targetLabel = 'Custom';
    const match = Object.values(TARGETS).find(t => t.baseUrl === baseUrl);
    if (match) targetLabel = match.label;

    // Duplicate prevention
    const uploadKey = getUploadKey(baseUrl, damPath, 'browser-chunk-' + (sessionId || resumeOf));
    if (activeUploads.has(uploadKey)) {
        return { status: 409, error: 'An upload to this target is already running' };
    }

    const uploadId = crypto.randomUUID();
    const startTime = Date.now();
    if (sessionId) finalizedBrowserSessions.set(sessionId, uploadId);

    const uploadSession = {
        listeners: new Set(),
        events: [],
        done: false,
        cancelFn: null,
        totalBytes: 0,
        transferredBytes: 0,
        totalFiles: 0,
        completedFiles: 0,
        failedFiles: 0,
        fileErrors: {},
        tempDir,
        meta: {
            owner,
            ownerName,
            target: targetLabel,
            baseUrl,
            damPath,
            localPath: resumeOf ? `Resumed upload (${receivedFiles} files left)`
                : sourceLabel ? `${sourceLabel} (${receivedFiles} files, ${Math.round(totalReceivedBytes / 1048576)} MB)`
                : 'Browser upload (' + receivedFiles + ' files, ' + Math.round(totalReceivedBytes / 1048576) + ' MB)',
            expectedFiles: receivedFiles,
            folder: landedFolder(damPath, contentChildren, keepNodeNames === true),
            expectedBytes: totalReceivedBytes,
            startTime,
        },
        uploadKey,
    };
    uploadSessions.set(uploadId, uploadSession);
    activeUploads.set(uploadKey, uploadId);

    onStarted(uploadId);

    const broadcast = createBroadcast(uploadSession);

    const clamped = clampUploadParams({ maxConcurrent, maxUploadFiles, httpRetryCount, httpRetryDelay, httpRequestTimeout });

    // Very large uploads stay at 8 x 4 even if a higher per-pipeline value was chosen
    if (receivedFiles > 5000 && clamped.maxConcurrent > 4) {
        log.info(`[finalize] Large upload (${receivedFiles} files): capping maxConcurrent from ${clamped.maxConcurrent} to 4`);
        clamped.maxConcurrent = 4;
    }

    const uploadConfig = {
        baseUrl,
        damPath,
        localPaths: contentChildren,
        username,
        password,
        deleteStagedOnSuccess: true,
        keepNodeNames: keepNodeNames === true,
        ...clamped,
    };

    // Everything needed to resume after a restart, except the AEM password (never stored)
    await writeResumeJob(tempDir, {
        jobId: resumeOf || uploadId, owner, ownerName, targetLabel, baseUrl, damPath, username,
        settings: { maxConcurrent, maxUploadFiles, httpRetryCount, httpRetryDelay, httpRequestTimeout, skipUnchanged, keepNodeNames },
        contentChildren, totalFiles: totalFilesOverall || receivedFiles, startedAt: new Date(startTime).toISOString(),
    });

    log.info(`[${resumeOf ? 'resume' : 'finalize'}] ${resumeOf || sessionId}: starting AEM upload ${uploadId} (${contentChildren.length} top-level paths) → ${baseUrl}/${damPath}`);

    // Batch per-file events to avoid overwhelming SSE/client with 17K+ individual messages
    let fileBatchQueue = [];
    let fileBatchTimer = null;
    function flushFileBatch() {
        fileBatchTimer = null;
        if (fileBatchQueue.length === 0) return;
        const batch = fileBatchQueue;
        fileBatchQueue = [];
        broadcast('filebatch', {
            files: batch,
            totalBytes: uploadSession.totalBytes,
            transferredBytes: uploadSession.transferredBytes,
            totalFiles: uploadSession.totalFiles,
            completedFiles: uploadSession.completedFiles,
            failedFiles: uploadSession.failedFiles,
        });
    }

    // Wait for a free slot on this AEM server; Cancel while waiting just leaves the line
    uploadSession.cancelFn = () => releaseUploadSlot(baseUrl, uploadId);
    const gotSlot = await acquireUploadSlot(baseUrl, uploadId, uploadSession, broadcast);
    if (!gotSlot || uploadSession.userCancelled || shuttingDown) {
        if (gotSlot) releaseUploadSlot(baseUrl, uploadId);
        uploadSession.done = true;
        uploadSession.runSettled = true;
        uploadSession.doneTime = Date.now();
        if (sessionId) finalizedBrowserSessions.delete(sessionId);
        if (activeUploads.get(uploadKey) === uploadId) activeUploads.delete(uploadKey);
        if (!shuttingDown) cleanupTempDir(tempDir).catch(() => {});
        log.info(`Upload ${uploadId} cancelled while waiting in the queue`);
        return { uploadId };
    }

    if (skipUnchanged) {
        broadcast('skipcheck', { state: 'checking' });
        const skip = await removeUnchangedStagedFiles({ baseUrl, damPath, username, password, contentChildren, keepNodeNames });
        uploadSession.meta.skippedFiles = skip.skipped;
        broadcast('skipcheck', { state: skip.error ? 'failed' : 'done', skipped: skip.skipped, checked: skip.checked, error: skip.error });
        log.info(`Upload ${uploadId}: skip-unchanged ${skip.error ? 'check failed (' + skip.error + '), uploading everything' : `skipped ${skip.skipped} of ${skip.checked} files already in AEM`}`);
    }

    const { promise: uploadPromise, cancel: cancelFn } = runUpload(uploadConfig, (eventName, data) => {
        if (eventName === 'uploadplan') {
            uploadSession.meta.expectedFiles = data.files;
            uploadSession.meta.expectedBytes = data.bytes;
            broadcast('uploadplan', { expectedFiles: data.files, expectedBytes: data.bytes });
            return;
        }
        uploadSession.totalBytes = data.totalBytes || uploadSession.totalBytes;
        uploadSession.transferredBytes = data.transferredBytes || uploadSession.transferredBytes;
        if (eventName === 'filestart') uploadSession.totalFiles++;
        if (eventName === 'fileend') uploadSession.completedFiles++;
        if (eventName === 'fileerror') {
            uploadSession.completedFiles++;
            uploadSession.failedFiles++;
            const errMsg = (data.errors && data.errors[0]) ? (typeof data.errors[0] === 'string' ? data.errors[0] : data.errors[0].message || String(data.errors[0])) : 'Unknown error';
            if (Object.keys(uploadSession.fileErrors).length < FILE_ERRORS_CAP) uploadSession.fileErrors[data.fileName] = { message: errMsg, code: classifyError(errMsg) };
        }
        if (eventName === 'fileprogress') {
            broadcast('fileprogress', {
                fileName: data.fileName,
                fileSize: data.fileSize,
                transferred: data.transferred,
                totalBytes: data.totalBytes,
                transferredBytes: data.transferredBytes,
            });
        } else {
            if (fileBatchQueue.length >= FILE_BATCH_QUEUE_CAP) {
                flushFileBatch();
            }
            fileBatchQueue.push({
                event: eventName,
                fileName: data.fileName,
                fileSize: data.fileSize,
                transferred: data.transferred,
                errors: data.errors ? data.errors.map(e => (typeof e === 'string' ? e : e.message || String(e))) : undefined,
            });
            if (!fileBatchTimer) fileBatchTimer = setTimeout(flushFileBatch, FILE_BATCH_INTERVAL_MS);
        }
    });

    uploadSession.cancelFn = cancelFn;
    uploadSession.runSettled = false;
    // Cancel can arrive between the response with the uploadId and this point
    if (uploadSession.userCancelled) cancelFn();

    uploadPromise.then((result) => {
        if (uploadSession.userCancelled) {
            log.info(`Chunked browser upload ${uploadId} finished after user cancel; skipping duplicate done/history`);
            return;
        }
        if (fileBatchTimer) { clearTimeout(fileBatchTimer); fileBatchTimer = null; }
        flushFileBatch();
        const duration = Date.now() - startTime;
        log.info(`Chunked browser upload ${result.success ? 'completed' : 'finished with errors'}: ${uploadId} (${result.totalFiles} files, ${duration}ms)`);
        try {
            broadcast('done', {
                success: result.success,
                partial: result.partial,
                totalFiles: result.totalFiles,
                totalBytes: result.totalBytes,
                transferredBytes: result.transferredBytes,
                errorCount: result.errors.length,
                errors: result.errors,
                fileErrors: uploadSession.fileErrors,
                cancelled: result.cancelled,
                duration,
            });
        } catch (e) { log.error('broadcast error in chunked browser .then:', e.message); }

        addHistoryRow(uploadSession.meta, {
            uploadId,
            target: targetLabel,
            baseUrl,
            damPath,
            localPath: uploadSession.meta.localPath,
            totalFiles: result.totalFiles,
            totalBytes: result.totalBytes,
            success: result.success,
            partial: result.partial,
            errorCount: result.errors.length,
            errors: result.errors.slice(0, 20),
            cancelled: result.cancelled,
            duration,
            timestamp: new Date().toISOString(),
        });
        trimHistory();
        saveHistoryToDisk();
    }).catch((err) => {
        if (uploadSession.userCancelled) {
            log.info(`Chunked browser upload ${uploadId} rejected after user cancel; ignoring:`, err.message);
            return;
        }
        const duration = Date.now() - startTime;
        log.error(`Chunked browser upload crashed: ${uploadId}`, err.message);
        try {
            broadcast('done', {
                success: false, totalFiles: 0, errorCount: 1,
                errors: [err.message || String(err)], duration,
            });
        } catch (e) { log.error('broadcast error in chunked browser .catch:', e.message); }
    }).finally(() => {
        releaseUploadSlot(baseUrl, uploadId);
        uploadSession.done = true;
        uploadSession.runSettled = true;
        if (sessionId) finalizedBrowserSessions.delete(sessionId);
        uploadSession.doneTime = Date.now();
        if (activeUploads.get(uploadKey) === uploadId) activeUploads.delete(uploadKey);
        // A shutdown leaves the staged files for Resume; a finished, failed or cancelled run removes them
        if (!shuttingDown) setTimeout(() => cleanupTempDir(tempDir).catch(() => {}), 30000);
        setTimeout(() => {
            uploadSession.events = [];
            uploadSession.fileErrors = {};
        }, 60000);
    });
    return { uploadId };
}

app.post('/api/upload/browser/finalize', uploadLimiter, async (req, res) => {
    // Extend request timeout — rearranging 50K files can take many minutes
    req.setTimeout(30 * 60 * 1000);
    if (res.req && res.req.socket) res.req.socket.setTimeout(30 * 60 * 1000);

    const { sessionId } = req.body;
    const ownedSession = browserUploadSessions.get(sessionId);
    const session = ownedSession && auth.canAccess(req, ownedSession.owner) ? ownedSession : null;
    if (!session) {
        return res.status(404).json({ error: 'Upload session not found or expired' });
    }
    if (session.copy && session.copy.state !== 'done') {
        return res.status(409).json({ error: 'The copy from the source AEM has not finished yet' });
    }
    session.finalized = true;

    if (session.receivedFiles === 0) {
        cleanupTempDir(session.tempDir);
        browserUploadSessions.delete(sessionId);
        return res.status(400).json({ error: 'No files were uploaded in this session' });
    }

    const { baseUrl, damPath, username, password,
            maxConcurrent, maxUploadFiles,
            httpRetryCount, httpRetryDelay, httpRequestTimeout, skipUnchanged, keepNodeNames } = session.config;
    const tempDir = session.tempDir;
    const receivedFiles = session.receivedFiles;
    const totalReceivedBytes = session.totalReceivedBytes;

    log.info(`[finalize] Session ${sessionId}: ${receivedFiles} files, ${(totalReceivedBytes / 1048576).toFixed(1)} MB — starting rearrange`);
    const rearrangeStart = Date.now();

    // Rearrange files from flat staging into proper folder structure
    let contentDir;
    try {
        contentDir = await rearrangeUploadedFiles(tempDir, session.files, session.relativePaths);
        const rearrangeMs = Date.now() - rearrangeStart;
        log.info(`[finalize] Session ${sessionId}: rearrange completed in ${(rearrangeMs / 1000).toFixed(1)}s at ${contentDir}`);
    } catch (e) {
        const rearrangeMs = Date.now() - rearrangeStart;
        log.error(`[finalize] Session ${sessionId}: rearrange FAILED after ${(rearrangeMs / 1000).toFixed(1)}s — ${e.message}`);
        cleanupTempDir(tempDir);
        browserUploadSessions.delete(sessionId);
        return res.status(500).json({ error: 'Failed to process uploaded files: ' + e.message });
    }

    // Free large arrays before deleting the session to help GC
    session.files = [];
    session.relativePaths = [];
    session.config = null;
    browserUploadSessions.delete(sessionId);

    if (session.cancelRequested) {
        log.info(`[finalize] Session ${sessionId}: cancelled while finalizing; AEM upload not started`);
        cleanupTempDir(tempDir).catch(() => {});
        return res.status(409).json({ error: 'Upload cancelled', code: 'CANCELLED' });
    }

    let contentChildren = (await fsPromises.readdir(contentDir)).map(name => nodePath.join(contentDir, name));

    // Prevent duplicate folder nesting: if contentChildren is a single directory
    // whose name matches the last segment of damPath, unwrap it so the library
    // uploads its contents directly into damPath rather than creating a duplicate subfolder.
    if (contentChildren.length === 1) {
        const childName = nodePath.basename(contentChildren[0]);
        const damLastSeg = damPath.replace(/\/+$/, '').split('/').pop();
        const childStat = await fsPromises.stat(contentChildren[0]);
        if (childStat.isDirectory() && childName === damLastSeg) {
            log.info(`[finalize] Unwrapping duplicate root folder "${childName}" (matches damPath tail)`);
            contentChildren = (await fsPromises.readdir(contentChildren[0])).map(name => nodePath.join(contentChildren[0], name));
        }
    }

    const started = await startAemUpload({
        owner: req.user.key, ownerName: req.user.name, baseUrl, damPath, username, password,
        settings: { maxConcurrent, maxUploadFiles, httpRetryCount, httpRetryDelay, httpRequestTimeout, skipUnchanged, keepNodeNames: !!session.copy || keepNodeNames === true },
        tempDir, contentChildren, receivedFiles, totalReceivedBytes, sessionId,
        sourceLabel: session.copy ? copyLabel(session.copy) : null,
        onStarted: (uploadId) => res.json({ uploadId }),
    });
    if (started.error) {
        cleanupTempDir(tempDir);
        res.status(started.status || 500).json({ error: started.error });
    }
});

// ─── API: uploads interrupted by a server restart ───
app.get('/api/upload/interrupted', async (req, res) => {
    await startupScan;
    res.json({ jobs: [...interruptedJobs.values()].filter((j) => auth.canAccess(req, j.owner)).map(publicInterruptedJob) });
});

app.post('/api/upload/interrupted/:id/resume', uploadLimiter, async (req, res) => {
    await startupScan;
    const job = interruptedJobs.get(req.params.id);
    if (!job || !auth.canAccess(req, job.owner)) return res.status(404).json({ error: 'Interrupted upload not found' });
    const password = (req.body || {}).password;
    const username = ((req.body || {}).username || job.username || '').trim();
    if (!password) return res.status(400).json({ error: 'Enter the AEM password to resume' });
    const cred = await checkAemCredentials(job.baseUrl, username, password);
    if (!cred.ok) return res.status(400).json({ error: cred.error, code: cred.code });
    const contentChildren = [];
    for (const p of job.contentChildren || []) { try { await fsPromises.stat(p); contentChildren.push(p); } catch (_) { /* finished */ } }
    const remaining = await countFiles(contentChildren);
    if (!remaining) {
        interruptedJobs.delete(job.jobId);
        cleanupTempDir(job.tempDir).catch(() => {});
        return res.status(409).json({ error: 'Nothing left to upload: all files reached AEM before the restart.' });
    }
    interruptedJobs.delete(job.jobId);
    let answered = false;
    const started = await startAemUpload({
        owner: job.owner, ownerName: job.ownerName, baseUrl: job.baseUrl, damPath: job.damPath, username, password,
        settings: job.settings || {}, tempDir: job.tempDir, contentChildren, receivedFiles: remaining, totalReceivedBytes: 0,
        totalFilesOverall: job.totalFiles, resumeOf: job.jobId,
        onStarted: (uploadId) => { answered = true; res.json({ uploadId, remaining }); },
    });
    if (started.error) {
        interruptedJobs.set(job.jobId, job);
        if (!answered) res.status(started.status || 500).json({ error: started.error });
        return;
    }
    log.info(`Resumed interrupted upload ${job.jobId}: ${remaining} files left (by ${req.user.key})`);
});

app.post('/api/upload/interrupted/:id/discard', (req, res) => {
    const job = interruptedJobs.get(req.params.id);
    if (!job || !auth.canAccess(req, job.owner)) return res.status(404).json({ error: 'Interrupted upload not found' });
    interruptedJobs.delete(job.jobId);
    cleanupTempDir(job.tempDir).catch(() => {});
    log.info(`Interrupted upload ${job.jobId} discarded by ${req.user.key}`);
    res.json({ message: 'Discarded' });
});

// ─── API: cancel upload ───
// Upload IDs are crypto.randomUUID() — 128-bit unguessable capability tokens.
// This provides implicit authorization: only clients that initiated the upload know the ID.
app.post('/api/upload/cancel/:uploadId', (req, res) => {
    const found = uploadSessions.get(req.params.uploadId);
    const session = found && auth.canAccess(req, found.meta.owner) ? found : null;
    if (!session) {
        return res.status(404).json({ error: 'Upload session not found' });
    }
    log.info(`Cancel requested for ${req.params.uploadId} from ${req.ip}`);
    if (session.done) {
        return res.status(400).json({ error: 'Upload already finished' });
    }
    if (typeof session.cancelFn === 'function') {
        try { session.cancelFn(); } catch (_) {}
    }
    emitUserCancelledUpload(req.params.uploadId, session, session.uploadKey);
    log.info(`Upload cancel completed (SSE done sent): ${req.params.uploadId}`);
    res.json({ message: 'Cancel requested' });
});

// ─── API: SSE stream for an upload ───
app.get('/api/upload/stream/:uploadId', async (req, res) => {
    log.debug(`SSE stream requested: ${req.params.uploadId} from ${req.ip}`);
    const found = uploadSessions.get(req.params.uploadId);
    const session = found && auth.canAccess(req, found.meta.owner) ? found : null;
    if (!session) {
        return res.status(404).json({ error: 'Upload session not found' });
    }

    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no',
    });

    // Replay buffered events, flushing after each batch to ensure delivery
    const events = session.events;
    const REPLAY_BATCH = 200;
    for (let i = 0; i < events.length; i++) {
        try {
            const { eventName, data } = events[i];
            res.write(`event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`);
        } catch (_) { return; }
        if ((i + 1) % REPLAY_BATCH === 0 || i === events.length - 1) {
            if (typeof res.flush === 'function') res.flush();
            await new Promise(r => setImmediate(r));
        }
    }

    if (session.done) {
        setTimeout(() => { try { res.end(); } catch (_) {} }, 1000);
        return;
    }

    session.listeners.add(res);

    // Keep-alive ping every 15s — also removes dead connections
    const keepAlive = setInterval(() => {
        try { res.write(':ping\n\n'); } catch (_) {
            clearInterval(keepAlive);
            session.listeners.delete(res);
            log.debug(`SSE keep-alive failed, removed dead connection for: ${req.params.uploadId}`);
        }
    }, SSE_KEEPALIVE_MS);

    req.on('close', () => {
        session.listeners.delete(res);
        clearInterval(keepAlive);
        log.debug(`SSE client disconnected: ${req.params.uploadId} (${session.listeners.size} remaining)`);
    });
});

// ─── API: list active uploads ───
app.get('/api/upload/active', (req, res) => {
    const active = [];
    uploadSessions.forEach((session, uploadId) => {
        if (!session.done && auth.canAccess(req, session.meta.owner)) {
            active.push({
                uploadId,
                ownerName: session.meta.ownerName || null,
                target: session.meta.target,
                baseUrl: session.meta.baseUrl,
                damPath: session.meta.damPath,
                localPath: session.meta.localPath,
                totalBytes: session.totalBytes,
                transferredBytes: session.transferredBytes,
                totalFiles: session.totalFiles,
                expectedFiles: session.meta.expectedFiles,
                expectedBytes: session.meta.expectedBytes,
                completedFiles: session.completedFiles,
                failedFiles: session.failedFiles,
                queuePosition: session.queuePosition || 0,
                elapsedMs: Date.now() - session.meta.startTime,
            });
        }
    });
    res.json(active);
});

// ─── API: single upload status ───
app.get('/api/upload/status/:uploadId', (req, res) => {
    const found = uploadSessions.get(req.params.uploadId);
    const session = found && auth.canAccess(req, found.meta.owner) ? found : null;
    if (!session) return res.status(404).json({ error: 'Upload session not found' });
    res.json({
        uploadId: req.params.uploadId,
        done: session.done,
        target: session.meta.target,
        baseUrl: session.meta.baseUrl,
        damPath: session.meta.damPath,
        localPath: session.meta.localPath,
        totalBytes: session.totalBytes,
        transferredBytes: session.transferredBytes,
        totalFiles: session.totalFiles,
        expectedFiles: session.meta.expectedFiles,
        expectedBytes: session.meta.expectedBytes,
        completedFiles: session.completedFiles,
        failedFiles: session.failedFiles,
        elapsedMs: Date.now() - session.meta.startTime,
    });
});

// ─── API: upload history ───
app.get('/api/upload/history', (req, res) => {
    res.json(uploadHistory.filter((h) => auth.canAccess(req, h.owner)));
});

app.delete('/api/upload/history/:uploadId', (req, res) => {
    const id = req.params.uploadId;
    const idx = uploadHistory.findIndex(h => h.uploadId === id && auth.canAccess(req, h.owner));
    if (idx === -1) {
        return res.status(404).json({ error: 'History entry not found' });
    }
    const removed = uploadHistory.splice(idx, 1);
    saveHistoryToDisk();
    log.info(`History entry deleted: uploadId=${id}`);
    res.json({ message: 'Deleted', item: removed[0] });
});

app.delete('/api/upload/history', (req, res) => {
    // Users clear only their own rows; admins clear everything
    const before = uploadHistory.length;
    const kept = uploadHistory.filter((h) => !auth.canAccess(req, h.owner));
    uploadHistory.length = 0;
    uploadHistory.push(...kept);
    saveHistoryToDisk();
    log.info(`History cleared by ${req.user.key}: ${before - kept.length} entries removed, ${kept.length} kept`);
    res.json({ message: 'History cleared', removed: before - kept.length });
});

// ─── API: browse filesystem ───
app.get('/api/browse', auth.requireAdmin, async (req, res) => {
    log.debug('Browse request:', req.query.path || '(root)');
    const requestedPath = (req.query.path || '').trim();
    const showHidden = req.query.showHidden === 'true';
    const platform = process.platform;
    const sep = nodePath.sep;

    if (!requestedPath) {
        if (platform === 'win32') {
            const drives = [];
            for (let code = 65; code <= 90; code++) {
                const letter = String.fromCharCode(code);
                const dp = letter + ':\\';
                try { await fsPromises.access(dp); drives.push({ name: letter + ':', type: 'drive', path: dp }); } catch (_) {}
            }
            return res.json({ path: '', parent: '', entries: drives, isRoot: true, platform, sep });
        }
        const rootEntries = await listDir('/', showHidden);
        return res.json({ path: '/', parent: '', entries: rootEntries, isRoot: true, platform, sep });
    }

    if (requestedPath.includes('\0')) return res.status(400).json({ error: 'Invalid path' });

    let resolved;
    try { resolved = nodePath.resolve(requestedPath); } catch (_) { return res.status(400).json({ error: 'Invalid path' }); }

    // Path traversal check after normalization
    if (!isPathWithinRoots(resolved)) {
        return res.status(403).json({ error: 'Access denied: path is outside allowed directories' });
    }

    try {
        const stat = await fsPromises.stat(resolved);
        if (!stat.isDirectory()) return res.status(400).json({ error: 'Not a directory' });
    } catch (e) {
        if (e.code === 'ENOENT') return res.status(400).json({ error: 'Path does not exist' });
        if (e.code === 'EACCES' || e.code === 'EPERM') return res.status(400).json({ error: 'Permission denied' });
        return res.status(400).json({ error: 'Cannot access: ' + e.message });
    }

    const parent = nodePath.dirname(resolved);
    const entries = await listDir(resolved, showHidden);
    res.json({
        path: resolved,
        parent: parent !== resolved ? parent : '',
        entries,
        isRoot: false,
        platform,
        sep,
    });
});

// ─── API: validate path ───
app.get('/api/validate-path', auth.requireAdmin, async (req, res) => {
    const result = await analyzeLocalPath(req.query.localPath);
    log.debug(`Validate path: "${req.query.localPath}" → valid=${result.valid}${result.error ? ', error=' + result.error : ''}${result.warnings?.length ? ', warnings=' + result.warnings.length : ''}`);
    res.json(result);
});

// ─── API: open path in OS file explorer (command injection hardened) ───
app.post('/api/open-path', auth.requireAdmin, async (req, res) => {
    const reqPath = req.body.path;
    if (!reqPath) return res.status(400).json({ error: 'Path is required' });

    const analysis = await analyzeLocalPath(reqPath);
    if (!analysis.valid) return res.status(400).json({ error: analysis.error });

    const safePath = analysis.normalized;

    // Use execFile with arguments array — no shell interpolation
    let cmd, args;
    if (process.platform === 'win32') {
        cmd = 'explorer.exe';
        args = [safePath];
    } else if (process.platform === 'darwin') {
        cmd = 'open';
        args = [safePath];
    } else {
        cmd = 'xdg-open';
        args = [safePath];
    }

    log.info(`Opening path in explorer: ${safePath}`);
    execFile(cmd, args, (err) => {
        if (err && process.platform !== 'win32') {
            log.warn('open-path failed:', safePath, err.message);
            return res.status(500).json({ error: 'Failed to open file explorer' });
        }
        res.json({ message: 'Opened in file explorer' });
    });
});

// ─── Global error handling ───
app.use((err, req, res, _next) => {
    // Clean up multer temp dir on any error (e.g. LIMIT_FILE_SIZE, client abort)
    if (req._tempDir) {
        cleanupTempDir(req._tempDir).catch(() => {});
    }

    // Handle multer-specific errors with user-friendly messages
    if (err.code === 'LIMIT_FILE_SIZE') {
        log.warn('Upload rejected: file too large');
        return res.status(413).json({ error: 'File too large. Maximum size: ' + Math.round(MAX_UPLOAD_SIZE / 1048576) + ' MB' });
    }
    if (err.code === 'LIMIT_UNEXPECTED_FILE') {
        log.warn('Upload rejected: unexpected field');
        return res.status(400).json({ error: 'Unexpected file field in upload' });
    }

    if (err.code === 'ENOSPC') {
        log.error(`Temp dir full (${os.tmpdir()}): ${req.method} ${req.path}`);
        return res.status(507).json({
            error: `The upload server ran out of disk space for staging files (${os.tmpdir()}). Free up space on the server and try again.`,
            code: 'INSUFFICIENT_SERVER_SPACE',
        });
    }

    log.error('Unhandled express error:', err.message, err.stack);
    res.status(500).json({ error: 'Internal server error' });
});

// NOTE: uncaughtException / unhandledRejection handlers are registered at
// the top of the file (lines ~203-208) to keep the process alive and log errors.

// ─── Graceful shutdown ───
let server;
let httpsServer;

let shuttingDown = false;

async function shutdown(signal) {
    shuttingDown = true;
    log.info(`${signal} received. Shutting down gracefully...`);
    uploadSessions.forEach((session, uploadId) => {
        if (session.done) return;
        const meta = session.meta;
        addHistoryRow(meta, {
            uploadId,
            target: meta.target,
            baseUrl: meta.baseUrl,
            damPath: meta.damPath,
            localPath: meta.localPath,
            totalFiles: session.totalFiles || 0,
            totalBytes: session.totalBytes || 0,
            uploadedFiles: Math.max(0, (session.completedFiles || 0) - (session.failedFiles || 0)),
            success: false,
            partial: false,
            errorCount: session.failedFiles || 0,
            errors: ['Interrupted: the upload server was stopped or restarted during this upload. Resume it from New upload.'],
            interrupted: true,
            cancelled: false,
            duration: Date.now() - meta.startTime,
            timestamp: new Date().toISOString(),
        });
        log.warn(`Upload ${uploadId} interrupted by shutdown (${session.completedFiles || 0} files done)`);
    });
    trimHistory();
    await saveHistoryToDisk();
    for (let i = 0; i < 100 && !historyWritesIdle(); i++) await new Promise((r) => setTimeout(r, 50));

    // Clean up temp directories from in-progress browser uploads
    const cleanupPromises = [];
    uploadSessions.forEach((session) => {
        // A running upload's staged files stay for Resume (see writeResumeJob)
        if (session.tempDir && session.done) cleanupPromises.push(cleanupTempDir(session.tempDir));
        session.listeners.forEach((sseRes) => {
            try { sseRes.end(); } catch (_) {}
        });
    });
    // Clean up chunked browser upload sessions
    browserUploadSessions.forEach((session) => {
        if (session.tempDir) cleanupPromises.push(cleanupTempDir(session.tempDir));
    });
    browserUploadSessions.clear();
    await Promise.allSettled(cleanupPromises);

    // Stop accepting new connections, wait for active ones to drain
    if (httpsServer && httpsServer.listening) httpsServer.close();
    if (server) {
        server.close(() => {
            log.info('Server closed.');
            process.exit(0);
        });
        setTimeout(() => {
            log.warn('Forced shutdown after timeout.');
            process.exit(1);
        }, SHUTDOWN_TIMEOUT_MS);
    } else {
        process.exit(0);
    }
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// ─── Start ───
(async () => {
    uploadHistory = await loadHistoryFromDisk();
    backup.scheduleBackups({
        'users.json': auth.USERS_FILE,
        'upload-history.json': HISTORY_FILE,
        'packages.json': nodePath.join(packages.PACKAGES_DIR, 'packages.json'),
    }, log);
    const tempFree = await getTempFreeBytes();
    const tls = TLS_CERT_FILE && TLS_KEY_FILE
        ? { cert: fs.readFileSync(TLS_CERT_FILE), key: fs.readFileSync(TLS_KEY_FILE) } : null;
    const samePort = tls && HTTPS_PORT === Number(PORT);
    // With HTTPS_ONLY plain HTTP only sends people to the HTTPS address (health checks still answer)
    const plain = tls && HTTPS_ONLY
        ? (req, res) => {
            if (req.url === '/api/health') return app(req, res);
            const host = String(req.headers.host || 'localhost').replace(/:\d+$/, '');
            // 302, not 301: browsers keep a 301 forever, so a wrong HTTPS address could not be taken back
            res.writeHead(302, { Location: `https://${host}${HTTPS_PORT === 443 ? '' : ':' + HTTPS_PORT}${req.url}` });
            res.end();
        }
        : app;
    if (tls && !samePort) {
        httpsServer = https.createServer(tls, app).listen(HTTPS_PORT, () => log.info(`HTTPS on port ${HTTPS_PORT}${HTTPS_ONLY ? ` (port ${PORT} redirects here)` : ''}`));
        httpsServer.on('error', (err) => { log.error(`Cannot listen on HTTPS port ${HTTPS_PORT}: ${err.code || err.message}`); process.exit(1); });
    }
    let listener;
    if (samePort) {
        // One port, both protocols: a TLS connection starts with a handshake record (first byte 0x16)
        const secure = https.createServer(tls, app);
        const insecure = http.createServer(plain);
        httpsServer = secure;
        listener = net.createServer((socket) => {
            socket.once('data', (first) => {
                socket.pause();
                socket.unshift(first);
                (first[0] === 0x16 ? secure : insecure).emit('connection', socket);
                process.nextTick(() => socket.resume());
            });
            socket.on('error', () => socket.destroy());
        });
        log.info(`HTTPS and HTTP on port ${PORT}${HTTPS_ONLY ? ' (HTTP redirects to HTTPS)' : ''}`);
    } else {
        listener = http.createServer(plain);
    }
    server = listener.listen(PORT, () => {
        // Only after the port is ours: a second instance must not delete the running one's temp dirs
        startupScan = sweepStaleTempDirs().catch((e) => log.warn('Temp dir sweep failed:', e.message));
        log.info(`GUIdes Upload Utility running at http://localhost:${PORT}`);
        log.info(`Platform: ${process.platform}, Node: ${process.version}, PID: ${process.pid}`);
        log.info(`Heap limit: ${(require('v8').getHeapStatistics().heap_size_limit / 1048576).toFixed(0)} MB`);
        log.info(`Log level: ${process.env.LOG_LEVEL || 'info'}${LOG_FILE ? ', file: ' + LOG_FILE : ''}`);
        log.info(`Max browser upload size: ${(MAX_UPLOAD_SIZE / 1048576).toFixed(0)} MB`);
        log.info(`History entries loaded: ${uploadHistory.length}`);
        log.info(`Temp dir: ${os.tmpdir()}${tempFree != null ? ` (${Math.floor(tempFree / 1048576)} MB free)` : ''}`);
        if (API_KEY) log.info('API key authentication is ENABLED');
        else log.warn('API key authentication is DISABLED (set API_KEY env var to enable)');
        if (BROWSE_ROOTS.length > 0) log.info('Browse roots restricted to: ' + BROWSE_ROOTS.join(', '));
    });
    server.on('error', (err) => {
        // Exit without the shutdown handler, which would save this process's history over the live one's
        log.error(`Cannot listen on port ${PORT}: ${err.code || err.message}. Is another instance running?`);
        process.exit(1);
    });

    // Periodic memory monitoring — log heap usage every 5 minutes
    setInterval(() => {
        const mem = process.memoryUsage();
        const heapMB = (mem.heapUsed / 1048576).toFixed(1);
        const rssMB = (mem.rss / 1048576).toFixed(1);
        const sessions = uploadSessions.size;
        const browserSessions = browserUploadSessions.size;
        log.info(`[Memory] Heap: ${heapMB} MB, RSS: ${rssMB} MB | Sessions: upload=${sessions}, browser=${browserSessions}`);
        // Force GC if available (run with --expose-gc)
        if (global.gc) { global.gc(); log.debug('[Memory] Forced GC'); }
    }, MEMORY_MONITOR_INTERVAL_MS);
})();

module.exports = { app };
