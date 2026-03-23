const {
    FileSystemUploadOptions,
    FileSystemUpload
} = require('@adobe/aem-upload');
const { RegularExpressions } = require('@adobe/aem-upload/src/constants');
const fs = require('fs');
const fsPromises = require('fs').promises;
const https = require('https');
const path = require('path');

const ALLOW_SELF_SIGNED = /^(1|true|yes)$/i.test(process.env.AEM_ALLOW_SELF_SIGNED || '');

// ─── Structured Logging (aligned with server.js format) ───
const LOG_LEVELS = { error: 0, warn: 1, info: 2, debug: 3 };
const LOG_LEVEL = LOG_LEVELS[process.env.LOG_LEVEL || 'info'] ?? LOG_LEVELS.info;
const LOG_FILE = process.env.LOG_FILE || '';
let _svcLogStream = null;
if (LOG_FILE) {
    try { _svcLogStream = fs.createWriteStream(path.resolve(LOG_FILE), { flags: 'a' }); } catch (_) {}
}
function _svcWriteLog(level, args) {
    const ts = new Date().toISOString();
    const msg = args.map(a => (typeof a === 'object' ? JSON.stringify(a) : String(a))).join(' ');
    const line = `${ts} [${level}][upload-svc] ${msg}`;
    if (level === 'ERROR') console.error(line);
    else if (level === 'WARN') console.warn(line);
    else console.log(line);
    if (_svcLogStream) _svcLogStream.write(line + '\n');
}
const svcLog = {
    error: (...args) => { if (LOG_LEVEL >= LOG_LEVELS.error) _svcWriteLog('ERROR', args); },
    warn:  (...args) => { if (LOG_LEVEL >= LOG_LEVELS.warn)  _svcWriteLog('WARN',  args); },
    info:  (...args) => { if (LOG_LEVEL >= LOG_LEVELS.info)  _svcWriteLog('INFO',  args); },
    debug: (...args) => { if (LOG_LEVEL >= LOG_LEVELS.debug) _svcWriteLog('DEBUG', args); },
};

const TARGETS = {};

/**
 * Normalize common AEM base URL typos (e.g. http:://host → http://host).
 * @param {string} raw
 * @returns {string}
 */
function normalizeAemBaseUrl(raw) {
    if (raw == null) return '';
    let s = String(raw).trim();
    if (!s) return '';
    // http::// or https::// → single colon scheme
    s = s.replace(/^(https?):{2,}(\/\/)(.*)$/i, '$1://$3');
    // Collapse accidental duplicate slashes right after scheme (http:////host)
    s = s.replace(/^(https?:\/\/)\/+/, '$1');
    return s;
}

/**
 * Recursively scan a directory and return file/folder stats.
 * @param {string} dirPath
 * @returns {Promise<{fileCount: number, totalSize: number, folderCount: number, extensions: object}>}
 */
async function scanDirectory(dirPath) {
    let fileCount = 0;
    let totalSize = 0;
    let folderCount = 0;
    const extensions = {};

    async function walk(dir) {
        let entries;
        try { entries = await fsPromises.readdir(dir, { withFileTypes: true }); }
        catch (e) { svcLog.warn('scanDirectory: cannot read', dir, e.code || e.message); return; }
        for (const entry of entries) {
            const full = path.join(dir, entry.name);
            if (entry.isDirectory()) {
                folderCount++;
                await walk(full);
            } else if (entry.isFile()) {
                fileCount++;
                try {
                    const stat = await fsPromises.stat(full);
                    totalSize += stat.size;
                } catch (_) { /* skip unreadable */ }
                const ext = path.extname(entry.name).toLowerCase() || '(no ext)';
                extensions[ext] = (extensions[ext] || 0) + 1;
            }
        }
    }

    await walk(dirPath);
    return { fileCount, totalSize, folderCount, extensions };
}

/**
 * Validate upload configuration before starting.
 * @param {object} config
 * @returns {string|null} error message or null if valid
 */
function validateUploadConfig(config) {
    const { damPath, localPaths, username, password } = config;
    let { baseUrl } = config;
    if (!baseUrl) return 'baseUrl is required';
    if (!damPath) return 'damPath is required';
    if (!localPaths || !localPaths.length) return 'At least one local path is required';
    if (!username) return 'username is required';
    if (!password) return 'password is required';

    baseUrl = normalizeAemBaseUrl(baseUrl);

    try {
        const parsed = new URL(baseUrl);
        if (!['http:', 'https:'].includes(parsed.protocol)) return 'baseUrl must use http or https';
        if (!parsed.hostname || parsed.hostname.length === 0) return 'baseUrl hostname is empty';
        // Private IPs allowed — required for on-prem AEM (e.g. http://10.x.x.x:4502)
    } catch (_) {
        return 'baseUrl is not a valid URL';
    }

    if (/[<>"|*?]/.test(damPath)) return 'damPath contains invalid characters';
    if (damPath.includes('..')) return 'damPath must not contain directory traversal (..)';

    return null;
}

/**
 * Run an AEM upload with the given configuration.
 * Returns an object with { promise, cancel } so the caller can cancel.
 *
 * @param {object} config
 * @param {function} [eventCallback] (eventName, data) => void
 * @returns {{ promise: Promise<object>, cancel: function }}
 */
function runUpload(config, eventCallback) {
    const baseUrl = normalizeAemBaseUrl(config.baseUrl);
    const { damPath, localPaths, username, password } = config;

    const url = `${baseUrl.replace(/\/+$/, '')}/${damPath.replace(/^\/+/, '')}`;
    const credentials = Buffer.from(`${username}:${password}`).toString('base64');

    svcLog.info(`Upload starting → ${url}`);
    svcLog.info(`  Local paths: ${localPaths.join(', ')}`);
    svcLog.info(`  Config: concurrent=${config.maxConcurrent || 2}, maxFiles=${config.maxUploadFiles || 50000}, retries=${config.httpRetryCount ?? 3}, timeout=${config.httpRequestTimeout ?? 60000}ms`);

    const maxConcurrent = config.maxConcurrent || 2;
    const maxUploadFiles = config.maxUploadFiles || 50000;
    const httpRetryCount = config.httpRetryCount != null ? config.httpRetryCount : 5;
    const httpRetryDelay = config.httpRetryDelay != null ? config.httpRetryDelay : 8000;
    const httpRequestTimeout = config.httpRequestTimeout != null ? config.httpRequestTimeout : 120000;

    const httpOptions = {
        headers: {
            Authorization: `Basic ${credentials}`,
        },
    };
    // Allow self-signed certs for localhost HTTPS (e.g. https://localhost:4502) when AEM_ALLOW_SELF_SIGNED=1
    if (ALLOW_SELF_SIGNED) {
        try {
            const parsed = new URL(baseUrl);
            const isLocal = ['localhost', '127.0.0.1', '::1'].includes(parsed.hostname.toLowerCase());
            if (parsed.protocol === 'https:' && isLocal) {
                httpOptions.agent = new https.Agent({ rejectUnauthorized: false });
                svcLog.info('Using self-signed cert bypass for localhost HTTPS');
            }
        } catch (_) { /* ignore */ }
    }

    const options = new FileSystemUploadOptions()
        .withUrl(url)
        .withDeepUpload(true)
        .withMaxUploadFiles(maxUploadFiles)
        .withMaxConcurrent(maxConcurrent)
        .withHttpRetryCount(httpRetryCount)
        .withHttpRetryDelay(httpRetryDelay)
        .withHttpRequestTimeout(httpRequestTimeout)
        .withHttpOptions(httpOptions)
        .withFolderNodeNameProcessor(async (folderName) => {
            return folderName.replace(
                RegularExpressions.INVALID_FOLDER_CHARACTERS_REGEX,
                '-',
            );
        })
        .withAssetNodeNameProcessor(async (assetName) => {
            return assetName.replace(
                RegularExpressions.INVALID_ASSET_CHARACTERS_REGEX,
                '-',
            );
        });

    const fileUpload = new FileSystemUpload();
    const errors = [];
    let totalFiles = 0;
    let totalBytes = 0;
    let transferredBytes = 0;
    let cancelled = false;
    const fileBytes = {};

    const uploadStartTime = Date.now();
    let lastProgressLog = 0;

    let lastProgressBroadcast = 0;
    let pendingProgressData = null;
    let progressFlushTimer = null;
    const PROGRESS_THROTTLE_MS = 80;

    function flushProgress() {
        progressFlushTimer = null;
        if (pendingProgressData && eventCallback) {
            try { eventCallback('fileprogress', pendingProgressData); } catch (_) {}
            pendingProgressData = null;
        }
    }

    const events = ['filestart', 'fileprogress', 'fileend', 'fileerror', 'filecancelled'];
    events.forEach((evt) => {
        fileUpload.on(evt, (data) => {
            if (evt === 'filestart') {
                totalFiles++;
                totalBytes += (data.fileSize || 0);
                svcLog.debug(`filestart: ${data.fileName} (${((data.fileSize || 0) / 1024).toFixed(1)} KB)`);
            }
            if (evt === 'fileprogress') {
                const prev = fileBytes[data.fileName] || 0;
                const curr = data.transferred || 0;
                transferredBytes += (curr - prev);
                fileBytes[data.fileName] = curr;
                const now = Date.now();
                if (now - lastProgressLog > 5000) {
                    const pct = totalBytes > 0 ? ((transferredBytes / totalBytes) * 100).toFixed(1) : '0.0';
                    svcLog.debug(`Progress: ${pct}% — ${(transferredBytes / 1048576).toFixed(1)}/${(totalBytes / 1048576).toFixed(1)} MB`);
                    lastProgressLog = now;
                }
                // Throttle fileprogress broadcasts to reduce SSE/DOM overhead
                const progressPayload = { ...data, totalBytes, transferredBytes, errors };
                if (now - lastProgressBroadcast >= PROGRESS_THROTTLE_MS) {
                    lastProgressBroadcast = now;
                    pendingProgressData = null;
                    if (progressFlushTimer) { clearTimeout(progressFlushTimer); progressFlushTimer = null; }
                    if (eventCallback) { try { eventCallback(evt, progressPayload); } catch (_) {} }
                } else {
                    pendingProgressData = progressPayload;
                    if (!progressFlushTimer) progressFlushTimer = setTimeout(flushProgress, PROGRESS_THROTTLE_MS);
                }
                return;
            }
            if (evt === 'fileend') {
                const prev = fileBytes[data.fileName] || 0;
                const fileSize = data.fileSize || 0;
                transferredBytes += (fileSize - prev);
                delete fileBytes[data.fileName];
                svcLog.debug(`fileend: ${data.fileName} (${((fileSize) / 1024).toFixed(1)} KB)`);
            }
            if (evt === 'fileerror') {
                const msg = (data.errors && data.errors[0] && data.errors[0].message) || 'Unknown error';
                errors.push(`${data.fileName}: ${msg}`);
                delete fileBytes[data.fileName];
                svcLog.error(`fileerror: ${data.fileName} — ${msg}`);
            }
            if (evt === 'filecancelled') {
                cancelled = true;
                delete fileBytes[data.fileName];
                svcLog.warn(`filecancelled: ${data.fileName}`);
            }
            // Flush any pending throttled progress before sending fileend/fileerror
            if (pendingProgressData) { flushProgress(); }
            if (eventCallback) {
                try {
                    eventCallback(evt, { ...data, totalBytes, transferredBytes, errors });
                } catch (_) {}
            }
        });
    });

    const promise = fileUpload.upload(options, localPaths)
        .then(() => {
            const elapsed = ((Date.now() - uploadStartTime) / 1000).toFixed(1);
            const speedMBs = elapsed > 0 ? ((transferredBytes / 1048576) / elapsed).toFixed(2) : '0';
            svcLog.info(`Upload finished: ${totalFiles} files, ${(transferredBytes / 1048576).toFixed(1)} MB in ${elapsed}s (${speedMBs} MB/s), errors: ${errors.length}, cancelled: ${cancelled}`);
            return { success: errors.length === 0 && !cancelled, totalFiles, totalBytes, transferredBytes, errors, cancelled };
        })
        .catch((err) => {
            svcLog.error('Upload failed with exception:', err.message);
            errors.push(err.message || String(err));
            return { success: false, totalFiles, totalBytes, transferredBytes, errors, cancelled };
        });

    function cancel() {
        svcLog.warn('Upload cancellation triggered');
        cancelled = true;
        try { fileUpload.cancel && fileUpload.cancel(); } catch (_) {}
    }

    return { promise, cancel };
}

module.exports = { TARGETS, runUpload, scanDirectory, validateUploadConfig, normalizeAemBaseUrl };
