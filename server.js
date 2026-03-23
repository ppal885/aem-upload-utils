const express = require('express');
const nodePath = require('path');
const crypto = require('crypto');
const fs = require('fs');
const fsPromises = require('fs').promises;
const os = require('os');
const { execFile } = require('child_process');
const cors = require('cors');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const multer = require('multer');
const { TARGETS, runUpload, scanDirectory, validateUploadConfig, normalizeAemBaseUrl } = require('./upload-service');

const app = express();
const PORT = process.env.PORT || 3000;
const HISTORY_FILE = nodePath.join(__dirname, 'upload-history.json');
const MAX_HISTORY = parseInt(process.env.MAX_HISTORY) || 100;
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
                .filter(seg => seg.length > 0)
                .map(seg => seg.replace(/\.\./g, '_').replace(/[<>"|?*]/g, '_'))
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
        if (eventName !== 'fileprogress' && session.events.length < SSE_EVENT_CAP) {
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
            errorCount: 0,
            errors: ['Upload cancelled by user'],
            fileErrors: session.fileErrors || {},
            cancelled: true,
            duration,
        });
    } catch (e) { log.error('broadcast error in emitUserCancelledUpload:', e.message); }
    session.done = true;
    session.doneTime = Date.now();
    session.userCancelled = true;
    if (uploadKey) activeUploads.delete(uploadKey);
    uploadHistory.unshift({
        uploadId,
        target: meta.target,
        baseUrl: meta.baseUrl,
        damPath: meta.damPath,
        localPath: meta.localPath,
        totalFiles: session.totalFiles || 0,
        totalBytes: session.totalBytes || 0,
        success: false,
        errorCount: 0,
        errors: ['Upload cancelled by user'],
        cancelled: true,
        duration,
        timestamp: new Date().toISOString(),
    });
    if (uploadHistory.length > MAX_HISTORY) uploadHistory.length = MAX_HISTORY;
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
const apiLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 200,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many requests, please try again later' },
    skip: (req) => req.path.startsWith('/upload/browser/chunk/'),
});
app.use('/api/', apiLimiter);

// Upload-specific rate limit (init/finalize endpoints)
const uploadLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 30,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many upload requests, please slow down' },
});

// Optional API key authentication
const API_KEY = process.env.API_KEY || '';

function authMiddleware(req, res, next) {
    if (!API_KEY) return next();
    const provided = req.headers['x-api-key'] || req.query.apiKey;
    if (provided === API_KEY) return next();
    log.warn(`Auth rejected: ${req.method} ${req.originalUrl.replace(/[?&]apiKey=[^&]*/g, '')} from ${req.ip}`);
    return res.status(401).json({ error: 'Unauthorized: invalid or missing API key' });
}

app.use('/api/', authMiddleware);

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
app.use(express.static(nodePath.join(__dirname, 'public'), {
    etag: false,
    lastModified: true,
    setHeaders: (res, filePath) => {
        // Prevent caching of HTML to ensure latest code is always served
        if (filePath.endsWith('.html')) {
            res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
            res.setHeader('Pragma', 'no-cache');
        }
    },
}));

// ─── In-memory stores ───
const uploadSessions = new Map();
const browserUploadSessions = new Map(); // Chunked browser upload sessions
let uploadHistory = [];

// Session reaper: clean up stuck/orphaned sessions every 2 minutes
const SESSION_MAX_AGE_MS = 24 * 60 * 60 * 1000; // 24 hours
const SESSION_DONE_LINGER_MS = 2 * 60 * 1000; // Keep done sessions 2 min for final SSE delivery
const BROWSER_SESSION_MAX_AGE_MS = 8 * 60 * 60 * 1000; // 8 hours — 50K files at 2 AEM threads can take hours
setInterval(() => {
    const now = Date.now();
    let reaped = 0;
    uploadSessions.forEach((session, uploadId) => {
        const startAge = now - session.meta.startTime;
        const doneAge = session.doneTime ? now - session.doneTime : 0;
        if (session.done && doneAge > SESSION_DONE_LINGER_MS) {
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
        if (age > BROWSER_SESSION_MAX_AGE_MS) {
            log.warn('Reaping stale browser upload session after 8h:', sessionId);
            session.files = []; session.relativePaths = [];
            cleanupTempDir(session.tempDir).catch(() => {});
            browserUploadSessions.delete(sessionId);
            reaped++;
        }
    });
    if (reaped > 0) log.info(`Session reaper: cleaned ${reaped} sessions (upload=${uploadSessions.size}, browser=${browserUploadSessions.size})`);
}, SESSION_REAPER_INTERVAL_MS);

// Async history persistence with a write lock
let historyWriteInProgress = false;
let historyWriteQueued = false;

async function loadHistoryFromDisk() {
    try {
        const data = await fsPromises.readFile(HISTORY_FILE, 'utf-8');
        return JSON.parse(data);
    } catch (e) {
        if (e.code !== 'ENOENT') log.warn('Failed to load history:', e.message);
        return [];
    }
}

async function saveHistoryToDisk() {
    if (historyWriteInProgress) { historyWriteQueued = true; return; }
    historyWriteInProgress = true;
    try {
        await fsPromises.writeFile(HISTORY_FILE, JSON.stringify(uploadHistory, null, 2));
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
        maxConcurrent: Math.min(Math.max(intOrDefault(raw.maxConcurrent, 2), 1), 10),
        maxUploadFiles: Math.min(Math.max(intOrDefault(raw.maxUploadFiles, 50000), 1), 200000),
        httpRetryCount: Math.min(Math.max(intOrDefault(raw.httpRetryCount, 5), 0), 10),
        httpRetryDelay: Math.min(Math.max(intOrDefault(raw.httpRetryDelay, 8000), 0), 60000),
        httpRequestTimeout: Math.min(Math.max(intOrDefault(raw.httpRequestTimeout, 120000), 5000), 600000),
    };
}

// ─── API: health check ───
app.get('/api/health', (_req, res) => {
    res.json({
        status: 'ok',
        uptime: Math.floor(process.uptime()),
        activeUploads: uploadSessions.size,
        historyCount: uploadHistory.length,
        platform: process.platform,
        sep: nodePath.sep,
        authEnabled: !!API_KEY,
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
app.get('/api/scan', async (req, res) => {
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
app.post('/api/upload', uploadLimiter, async (req, res) => {
    const {
        targetId, baseUrl, damPath, localPath,
        username, password,
        maxConcurrent, maxUploadFiles,
        httpRetryCount, httpRetryDelay, httpRequestTimeout
    } = req.body;

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

        uploadHistory.unshift({
            uploadId,
            target: targetLabel,
            baseUrl: resolvedBaseUrl,
            damPath: resolvedDamPath,
            localPath,
            totalFiles: result.totalFiles,
            totalBytes: result.totalBytes,
            success: result.success,
            errorCount: result.errors.length,
            errors: result.errors.slice(0, 20),
            cancelled: result.cancelled,
            duration,
            timestamp: new Date().toISOString(),
        });
        if (uploadHistory.length > MAX_HISTORY) uploadHistory.length = MAX_HISTORY;
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
        session.doneTime = Date.now();
        activeUploads.delete(uploadKey);
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

    const { baseUrl, damPath, username, password,
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
            target: targetLabel,
            baseUrl: normalizedBaseUrl,
            damPath,
            localPath: 'Browser upload (' + req.files.length + ' files, ' + Math.round(totalUploadedBytes / 1048576) + ' MB)',
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

        uploadHistory.unshift({
            uploadId,
            target: targetLabel,
            baseUrl: normalizedBaseUrl,
            damPath,
            localPath: session.meta.localPath,
            totalFiles: result.totalFiles,
            totalBytes: result.totalBytes,
            success: result.success,
            errorCount: result.errors.length,
            errors: result.errors.slice(0, 20),
            cancelled: result.cancelled,
            duration,
            timestamp: new Date().toISOString(),
        });
        if (uploadHistory.length > MAX_HISTORY) uploadHistory.length = MAX_HISTORY;
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
        session.doneTime = Date.now();
        activeUploads.delete(uploadKey);
        setTimeout(() => cleanupTempDir(tempDir).catch(() => {}), 30000);
        setTimeout(() => {
            session.events = [];
            session.fileErrors = {};
        }, 60000);
    });
});

// ─── API: chunked browser upload — init session ───
app.post('/api/upload/browser/init', uploadLimiter, async (req, res) => {
    const { baseUrl, damPath, username, password,
            maxConcurrent, maxUploadFiles,
            httpRetryCount, httpRetryDelay, httpRequestTimeout } = req.body;

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

    const sessionId = crypto.randomUUID();
    const tempDir = nodePath.join(os.tmpdir(), TEMP_DIR_PREFIX + 'chunk-' + sessionId.slice(0, 8));
    await fsPromises.mkdir(nodePath.join(tempDir, '__staging'), { recursive: true });

    browserUploadSessions.set(sessionId, {
        tempDir,
        config: { baseUrl: normalizedInitUrl, damPath, username, password,
                  maxConcurrent, maxUploadFiles,
                  httpRetryCount, httpRetryDelay, httpRequestTimeout },
        receivedFiles: 0,
        totalReceivedBytes: 0,
        relativePaths: [],
        files: [],
        createdAt: Date.now(),
    });

    log.info(`Browser chunk session created: ${sessionId}, tempDir=${tempDir}`);
    res.json({ sessionId });
});

// ─── API: cancel chunked browser session (before finalize) ───
app.post('/api/upload/browser/cancel/:sessionId', uploadLimiter, (req, res) => {
    const session = browserUploadSessions.get(req.params.sessionId);
    if (!session) {
        return res.status(404).json({ error: 'Upload session not found or already cancelled' });
    }
    if (session.finalized) {
        return res.status(409).json({ error: 'Session already finalized; cancel the AEM upload instead' });
    }
    const tempDir = session.tempDir;
    const sid = req.params.sessionId;
    browserUploadSessions.delete(sid);
    cleanupTempDir(tempDir).catch(() => {});
    log.info(`Browser chunk session cancelled: ${sid}`);
    res.json({ message: 'Session cancelled', sessionId: sid });
});

// ─── API: chunked browser upload — receive chunk ───
app.post('/api/upload/browser/chunk/:sessionId', (req, res, next) => {
    const session = browserUploadSessions.get(req.params.sessionId);
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
app.post('/api/upload/browser/finalize', uploadLimiter, async (req, res) => {
    // Extend request timeout — rearranging 50K files can take many minutes
    req.setTimeout(30 * 60 * 1000);
    if (res.req && res.req.socket) res.req.socket.setTimeout(30 * 60 * 1000);

    const { sessionId } = req.body;
    const session = browserUploadSessions.get(sessionId);
    if (!session) {
        return res.status(404).json({ error: 'Upload session not found or expired' });
    }
    session.finalized = true;

    if (session.receivedFiles === 0) {
        cleanupTempDir(session.tempDir);
        browserUploadSessions.delete(sessionId);
        return res.status(400).json({ error: 'No files were uploaded in this session' });
    }

    const { baseUrl, damPath, username, password,
            maxConcurrent, maxUploadFiles,
            httpRetryCount, httpRetryDelay, httpRequestTimeout } = session.config;
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

    // Determine target label
    let targetLabel = 'Custom';
    const match = Object.values(TARGETS).find(t => t.baseUrl === baseUrl);
    if (match) targetLabel = match.label;

    // Duplicate prevention
    const uploadKey = getUploadKey(baseUrl, damPath, 'browser-chunk-' + sessionId);
    if (activeUploads.has(uploadKey)) {
        cleanupTempDir(tempDir);
        return res.status(409).json({ error: 'An upload to this target is already running' });
    }

    const uploadId = crypto.randomUUID();
    const startTime = Date.now();

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
            target: targetLabel,
            baseUrl,
            damPath,
            localPath: 'Browser upload (' + receivedFiles + ' files, ' + Math.round(totalReceivedBytes / 1048576) + ' MB)',
            startTime,
        },
        uploadKey,
    };
    uploadSessions.set(uploadId, uploadSession);
    activeUploads.set(uploadKey, uploadId);

    res.json({ uploadId });

    const broadcast = createBroadcast(uploadSession);

    const clamped = clampUploadParams({ maxConcurrent, maxUploadFiles, httpRetryCount, httpRetryDelay, httpRequestTimeout });

    // Auto-cap concurrency for large uploads — AEM handles max ~2 concurrent
    if (receivedFiles > 5000 && clamped.maxConcurrent > 2) {
        log.info(`[finalize] Large upload (${receivedFiles} files): capping maxConcurrent from ${clamped.maxConcurrent} to 2`);
        clamped.maxConcurrent = 2;
    }

    // List children of contentDir so the library uploads contents directly
    // into the DAM path without creating an extra wrapper folder.
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

    const uploadConfig = {
        baseUrl,
        damPath,
        localPaths: contentChildren,
        username,
        password,
        ...clamped,
    };

    log.info(`[finalize] Session ${sessionId}: rearrange done, starting AEM upload ${uploadId} (${contentChildren.length} top-level paths) → ${baseUrl}/${damPath}`);

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

    const { promise: uploadPromise, cancel: cancelFn } = runUpload(uploadConfig, (eventName, data) => {
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

        uploadHistory.unshift({
            uploadId,
            target: targetLabel,
            baseUrl,
            damPath,
            localPath: uploadSession.meta.localPath,
            totalFiles: result.totalFiles,
            totalBytes: result.totalBytes,
            success: result.success,
            errorCount: result.errors.length,
            errors: result.errors.slice(0, 20),
            cancelled: result.cancelled,
            duration,
            timestamp: new Date().toISOString(),
        });
        if (uploadHistory.length > MAX_HISTORY) uploadHistory.length = MAX_HISTORY;
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
        uploadSession.done = true;
        uploadSession.doneTime = Date.now();
        activeUploads.delete(uploadKey);
        setTimeout(() => cleanupTempDir(tempDir).catch(() => {}), 30000);
        setTimeout(() => {
            uploadSession.events = [];
            uploadSession.fileErrors = {};
        }, 60000);
    });
});

// ─── API: cancel upload ───
// Upload IDs are crypto.randomUUID() — 128-bit unguessable capability tokens.
// This provides implicit authorization: only clients that initiated the upload know the ID.
app.post('/api/upload/cancel/:uploadId', (req, res) => {
    const session = uploadSessions.get(req.params.uploadId);
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
    const session = uploadSessions.get(req.params.uploadId);
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
app.get('/api/upload/active', (_req, res) => {
    const active = [];
    uploadSessions.forEach((session, uploadId) => {
        if (!session.done) {
            active.push({
                uploadId,
                target: session.meta.target,
                baseUrl: session.meta.baseUrl,
                damPath: session.meta.damPath,
                localPath: session.meta.localPath,
                totalBytes: session.totalBytes,
                transferredBytes: session.transferredBytes,
                totalFiles: session.totalFiles,
                completedFiles: session.completedFiles,
                failedFiles: session.failedFiles,
                elapsedMs: Date.now() - session.meta.startTime,
            });
        }
    });
    res.json(active);
});

// ─── API: single upload status ───
app.get('/api/upload/status/:uploadId', (req, res) => {
    const session = uploadSessions.get(req.params.uploadId);
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
        completedFiles: session.completedFiles,
        failedFiles: session.failedFiles,
        elapsedMs: Date.now() - session.meta.startTime,
    });
});

// ─── API: upload history ───
app.get('/api/upload/history', (_req, res) => {
    res.json(uploadHistory);
});

app.delete('/api/upload/history/:uploadId', (req, res) => {
    const id = req.params.uploadId;
    const idx = uploadHistory.findIndex(h => h.uploadId === id);
    if (idx === -1) {
        return res.status(404).json({ error: 'History entry not found' });
    }
    const removed = uploadHistory.splice(idx, 1);
    saveHistoryToDisk();
    log.info(`History entry deleted: uploadId=${id}`);
    res.json({ message: 'Deleted', item: removed[0] });
});

app.delete('/api/upload/history', (_req, res) => {
    const count = uploadHistory.length;
    uploadHistory.length = 0;
    saveHistoryToDisk();
    log.info(`All history cleared (${count} entries)`);
    res.json({ message: 'All history cleared' });
});

// ─── API: browse filesystem ───
app.get('/api/browse', async (req, res) => {
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
app.get('/api/validate-path', async (req, res) => {
    const result = await analyzeLocalPath(req.query.localPath);
    log.debug(`Validate path: "${req.query.localPath}" → valid=${result.valid}${result.error ? ', error=' + result.error : ''}${result.warnings?.length ? ', warnings=' + result.warnings.length : ''}`);
    res.json(result);
});

// ─── API: open path in OS file explorer (command injection hardened) ───
app.post('/api/open-path', async (req, res) => {
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

    log.error('Unhandled express error:', err.message, err.stack);
    res.status(500).json({ error: 'Internal server error' });
});

// NOTE: uncaughtException / unhandledRejection handlers are registered at
// the top of the file (lines ~203-208) to keep the process alive and log errors.

// ─── Graceful shutdown ───
let server;

async function shutdown(signal) {
    log.info(`${signal} received. Shutting down gracefully...`);
    await saveHistoryToDisk();

    // Clean up temp directories from in-progress browser uploads
    const cleanupPromises = [];
    uploadSessions.forEach((session) => {
        if (session.tempDir) cleanupPromises.push(cleanupTempDir(session.tempDir));
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
    server = app.listen(PORT, () => {
        log.info(`GUIdes Upload Utility running at http://localhost:${PORT}`);
        log.info(`Platform: ${process.platform}, Node: ${process.version}, PID: ${process.pid}`);
        log.info(`Heap limit: ${(require('v8').getHeapStatistics().heap_size_limit / 1048576).toFixed(0)} MB`);
        log.info(`Log level: ${process.env.LOG_LEVEL || 'info'}${LOG_FILE ? ', file: ' + LOG_FILE : ''}`);
        log.info(`Max browser upload size: ${(MAX_UPLOAD_SIZE / 1048576).toFixed(0)} MB`);
        log.info(`History entries loaded: ${uploadHistory.length}`);
        if (API_KEY) log.info('API key authentication is ENABLED');
        else log.warn('API key authentication is DISABLED (set API_KEY env var to enable)');
        if (BROWSE_ROOTS.length > 0) log.info('Browse roots restricted to: ' + BROWSE_ROOTS.join(', '));
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
