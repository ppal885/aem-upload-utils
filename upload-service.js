const fs = require('fs');
const fsPromises = require('fs').promises;
const path = require('path');
const AemNames = require('./public/aem-names');
const adaptivePipelines = require('./lib/adaptive-pipelines');
const { aemAuthorization } = require('./lib/aem-auth');
const ditaOrder = require('./lib/dita-order');
const guidesWait = require('./lib/guides-wait');

// @adobe/aem-upload 2.0.3 silently drops every file named TestFile*, *.tmp or starting with "~"
// (desktop-app scratch files). Real content uses such names, so only system files are skipped;
// the walker is replaced before the library loads because it imports these functions by name.
const aemUtils = require('@adobe/aem-upload/src/utils');
const UploadError = require('@adobe/aem-upload/src/upload-error');
const ErrorCodes = require('@adobe/aem-upload/src/error-codes');
aemUtils.isTempPath = (p) => AemNames.isSystemName(path.basename(String(p)));
aemUtils.walkDirectory = async function walkDirectory(rootDir, maximumPaths = 5000, includeDescendents = true) {
    const directories = [];
    const files = [];
    const errors = [];
    let totalSize = 0;
    let pending = [rootDir];
    while (pending.length) {
        const next = [];
        for (const dir of pending) {
            let entries;
            try { entries = await fsPromises.readdir(dir, { withFileTypes: true }); } catch (e) { errors.push(e); continue; }
            const stats = await Promise.all(entries
                .filter((e) => !AemNames.isSystemName(e.name))
                .map(async (e) => {
                    const full = path.join(dir, e.name);
                    try { return { full, stat: await fsPromises.stat(full) }; } catch (err) { errors.push(err); return null; }
                }));
            for (const item of stats) {
                if (!item) continue;
                if (item.stat.isDirectory()) {
                    directories.push({ path: item.full });
                    if (includeDescendents) next.push(item.full);
                } else if (item.stat.isFile()) {
                    files.push({ path: item.full, size: item.stat.size });
                    totalSize += item.stat.size;
                }
            }
            // The limit is on files (maxUploadFiles): 49,500 files in 600 folders is a 49,500-file upload
            if (files.length > maximumPaths) {
                throw new UploadError(`The upload has more than the maximum of ${maximumPaths} files`, ErrorCodes.TOO_LARGE);
            }
        }
        pending = next;
    }
    return { directories, files, errors, totalSize };
};

const {
    DirectBinaryUpload,
    DirectBinaryUploadOptions,
    FileSystemUploadOptions,
    FileSystemUpload
} = require('@adobe/aem-upload');
const UploadResult = require('@adobe/aem-upload/src/upload-result');
const http = require('http');
const https = require('https');

const ALLOW_SELF_SIGNED = /^(1|true|yes)$/i.test(process.env.AEM_ALLOW_SELF_SIGNED || '');

// AEM as a Cloud Service can return 404 on initiateUpload for a folder created moments ago
// (the new folder is not yet visible to every author pod). @adobe/aem-upload never retries 4xx,
// so files that fail with 404 are re-uploaded after these delays.
const NOT_FOUND_RETRY_DELAYS_MS = [15000, 30000];
const CREDENTIAL_CHECK_TIMEOUT_MS = 15000;
const FOLDER_CREATE_CONCURRENCY = 5;
// A folder that fails with 429 (AEM Cloud throttling), 5xx or a network error is tried again after these delays
const FOLDER_RETRY_DELAYS_MS = [2000, 8000, 30000];
const DEFAULT_UPLOAD_PIPELINES = 8;
// @adobe/aem-upload cannot abort a running transfer, so each pipeline uploads in batches and
// checks for a cancel between them; this bounds how much is still sent after Cancel.
const PIPELINE_BATCH_SIZE = 50;
// Adaptive pipelines (lib/adaptive-pipelines.js): measured every window, never above the max
const MAX_UPLOAD_PIPELINES = parseInt(process.env.UPLOAD_MAX_PIPELINES, 10) || 24;
const ADAPT_WINDOW_MS = parseInt(process.env.UPLOAD_ADAPT_WINDOW_MS, 10) || 30000;
const LOCAL_HOSTNAMES = ['localhost', '127.0.0.1', '::1'];
// Uploads with DITA maps go in waves (lib/dita-order.js); before each map wave, the files of the previous waves
// must be processed by AEM Guides, or the maps get GUIDs no file has. Files Guides processes:
const GUIDES_FILE = /\.(dita|ditamap|bookmap|xml)$/i;
const GUIDES_WAIT_POLL_MS = parseInt(process.env.GUIDES_WAIT_POLL_MS, 10) || 3000;
// Larger uploads are checked less often: every check lists the whole upload folder
const GUIDES_WAIT_POLL_LARGE_MS = 15000;
// No Guides state on any file after this long: Guides does not process this folder (or is not installed)
const GUIDES_WAIT_GRACE_MS = parseInt(process.env.GUIDES_WAIT_GRACE_MS, 10) || 60000;
const GUIDES_WAIT_MAX_MS = parseInt(process.env.GUIDES_WAIT_MAX_MS, 10) || 15 * 60 * 1000;

function buildSelfSignedAgent(baseUrl) {
    if (!ALLOW_SELF_SIGNED) return null;
    try {
        const parsed = new URL(baseUrl);
        if (parsed.protocol === 'https:' && LOCAL_HOSTNAMES.includes(parsed.hostname.toLowerCase())) {
            return new https.Agent({ rejectUnauthorized: false });
        }
    } catch (_) { /* ignore */ }
    return null;
}

// ─── Structured Logging (aligned with server.js format) ───
const LOG_LEVELS = { error: 0, warn: 1, info: 2, debug: 3 };
const LOG_LEVEL = LOG_LEVELS[process.env.LOG_LEVEL || 'info'] ?? LOG_LEVELS.info;
// @adobe/httptransfer prints ~10 unconditional console.log lines per file ("Opening file …"); on a 59K-file
// upload that is hundreds of MB of PM2 log and synchronous stdout writes. Our own log lines start with an
// ISO timestamp, so anything else is library chatter and is only shown with LOG_LEVEL=debug.
if (LOG_LEVEL < LOG_LEVELS.debug) {
    const consoleLog = console.log.bind(console);
    console.log = (...args) => {
        if (typeof args[0] === 'string' && /^\d{4}-\d\d-\d\dT/.test(args[0])) consoleLog(...args);
    };
}
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

    const dam = AemNames.normalizeDamPath(damPath);
    if (dam.error) return dam.error;

    return null;
}

/**
 * Verify that AEM is reachable and accepts the credentials, before any file is sent.
 * Only a definite rejection (401/403, anonymous session) or a connection failure is reported
 * as a failure; any other response is treated as reachable so unusual AEM setups are not blocked.
 *
 * @param {string} rawBaseUrl
 * @param {string} username
 * @param {string} password
 * @returns {Promise<{ok: boolean, code?: string, status?: number, error?: string}>}
 */
function checkAemCredentials(rawBaseUrl, username, password) {
    const hardTimeout = new Promise((resolve) => {
        const t = setTimeout(() => resolve({ ok: false, code: 'AEM_UNREACHABLE',
            error: `AEM at ${normalizeAemBaseUrl(rawBaseUrl)} did not answer the login check in time.` }),
        CREDENTIAL_CHECK_TIMEOUT_MS + 5000);
        if (t.unref) t.unref();
    });
    return Promise.race([checkAemCredentialsOnce(rawBaseUrl, username, password), hardTimeout]);
}

function checkAemCredentialsOnce(rawBaseUrl, username, password) {
    const baseUrl = normalizeAemBaseUrl(rawBaseUrl).replace(/\/+$/, '');
    const checkUrl = new URL(`${baseUrl}/libs/granite/security/currentuser.json`);
    const isLocalHost = LOCAL_HOSTNAMES.includes(checkUrl.hostname.toLowerCase());
    const client = checkUrl.protocol === 'https:' ? https : http;
    const requestOptions = {
        method: 'GET',
        timeout: CREDENTIAL_CHECK_TIMEOUT_MS,
        headers: {
            Authorization: aemAuthorization(username, password),
            Accept: 'application/json',
        },
    };
    const agent = buildSelfSignedAgent(baseUrl);
    if (agent) requestOptions.agent = agent;

    return new Promise((resolve) => {
        const req = client.request(checkUrl, requestOptions, (res) => {
            let body = '';
            res.setEncoding('utf8');
            res.on('error', (err) => resolve({ ok: false, code: 'AEM_UNREACHABLE',
                error: `Cannot reach AEM at ${baseUrl} (${err.code || err.message}).` }));
            res.on('data', (chunk) => { if (body.length < 65536) body += chunk; });
            res.on('end', () => {
                const status = res.statusCode;
                if (status === 401 || status === 403) {
                    resolve({ ok: false, code: 'AUTH_FAILED', status,
                        error: `AEM rejected the username or password (HTTP ${status}). Check the credentials and try again.` });
                    return;
                }
                let aemUser = null;
                if (status === 200) {
                    try {
                        const user = JSON.parse(body);
                        if (user && user.authorizableId === 'anonymous') {
                            resolve({ ok: false, code: 'AUTH_FAILED', status,
                                error: 'AEM did not accept the username or password (signed in as anonymous). Check the credentials and try again.' });
                            return;
                        }
                        if (user && user.authorizableId) {
                            aemUser = { id: user.authorizableId, name: user.name || user.authorizableId };
                        }
                    } catch (_) { /* non-JSON 200: treat as reachable */ }
                }
                if (status !== 200) svcLog.warn(`Credential check: unexpected HTTP ${status} from ${checkUrl.href}; continuing`);
                resolve({ ok: true, status, user: aemUser });
            });
        });
        req.on('timeout', () => req.destroy(Object.assign(new Error('timed out'), { code: 'ETIMEDOUT' })));
        req.on('error', (err) => {
            let error = `Cannot reach AEM at ${baseUrl} (${err.code || err.message}).`;
            if (isLocalHost) {
                error += ' Note: "localhost" means the machine running this upload utility, not your computer.'
                    + ' Use the AEM host name or IP address instead.';
            }
            resolve({ ok: false, code: 'AEM_UNREACHABLE', error });
        });
        req.end();
    });
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

    svcLog.info(`Upload starting → ${url}`);
    svcLog.info(`  Local paths: ${localPaths.join(', ')}`);
    svcLog.info(`  Config: concurrent=${config.maxConcurrent || 4}, pipelines=${config.uploadPipelines || DEFAULT_UPLOAD_PIPELINES}, maxFiles=${config.maxUploadFiles || 50000}, retries=${config.httpRetryCount ?? 3}, timeout=${config.httpRequestTimeout ?? 60000}ms`);

    const maxConcurrent = config.maxConcurrent || 4;
    const maxUploadFiles = config.maxUploadFiles || 50000;
    const httpRetryCount = config.httpRetryCount != null ? config.httpRetryCount : 5;
    const httpRetryDelay = config.httpRetryDelay != null ? config.httpRetryDelay : 8000;
    const httpRequestTimeout = config.httpRequestTimeout != null ? config.httpRequestTimeout : 120000;
    const uploadPipelines = config.uploadPipelines || DEFAULT_UPLOAD_PIPELINES;
    // Grows only when the caller says this is the only upload to that AEM server (config.canGrow)
    const pipelines = typeof config.canGrow === 'function'
        ? adaptivePipelines.createController({ start: uploadPipelines, max: Math.max(uploadPipelines, MAX_UPLOAD_PIPELINES) })
        : { target: uploadPipelines, next: null };

    const httpOptions = {
        headers: {
            Authorization: aemAuthorization(username, password),
        },
        // httptransfer drops the library's own timeout, so without this a stalled request never ends
        timeout: httpRequestTimeout,
        // Replaces the library's retry options (which retry only 5xx) so AEM Cloud throttling (429) is retried too
        retryOptions: {
            retryInitialDelay: httpRetryDelay,
            retryMaxCount: httpRetryCount,
            retryAllErrors: false,
            retryOnHttpResponseError: (e) => e.status === 429 || e.status >= 500,
        },
    };
    // Allow self-signed certs for localhost HTTPS (e.g. https://localhost:4502) when AEM_ALLOW_SELF_SIGNED=1
    const selfSignedAgent = buildSelfSignedAgent(baseUrl);
    if (selfSignedAgent) {
        httpOptions.agent = selfSignedAgent;
        svcLog.info('Using self-signed cert bypass for localhost HTTPS');
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
        // A copy from another AEM keeps the source's node names exactly: links and DITA profile paths use them
        .withFolderNodeNameProcessor(async (folderName) => (config.keepNodeNames ? AemNames.keepUploadFolderName(folderName) : AemNames.folderNodeName(folderName)))
        .withAssetNodeNameProcessor(async (assetName) => (config.keepNodeNames ? AemNames.keepUploadAssetName(assetName) : AemNames.assetNodeName(assetName)));

    const errors = [];
    let totalFiles = 0;
    let totalBytes = 0;
    let transferredBytes = 0;
    let cancelled = false;
    const fileBytes = {};
    // Files whose attempt hit a 404 and that will be re-uploaded: targetFile -> event data
    const heldNotFound = new Map();
    const startedFiles = new Set();
    let retryPassesLeft = NOT_FOUND_RETRY_DELAYS_MS.length;

    const uploadStartTime = Date.now();
    let lastProgressLog = 0;
    // One info line a minute, so a running upload is visible in the server log
    let filesDone = 0;
    let plannedFiles = 0;
    let lastSummaryLog = Date.now();
    // AEM path -> staged local file, so a finished file can be removed (what is left after a restart is
    // exactly the unfinished work; see the resume job in server.js)
    const stagedByTarget = new Map();
    // AEM paths of the files that finished uploading
    const uploadedTargets = new Set();
    // AEM paths of the files sent a second time once Guides processed everything (links lib/dita-order.js cannot order)
    const sendAgainTargets = new Set();
    // AEM paths of the files other files of the upload link to (images too: on a UUID server they get GUIDs as well)
    const linkedTargets = new Set();
    // false once AEM Guides is seen not to process this upload: later waves do not wait
    let guidesProcessing = true;

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

    function fileKey(data) {
        return data.targetFile || data.fileName;
    }

    function isNotFound(data) {
        const err = data.errors && data.errors[0];
        if (!err) return false;
        return err.code === 404 || err.status === 404 || /status code 404\b/.test(err.message || String(err));
    }

    function recordFileError(data) {
        const msg = (data.errors && data.errors[0] && data.errors[0].message) || 'Unknown error';
        errors.push(`${data.fileName}: ${msg}`);
        svcLog.error(`fileerror: ${data.fileName} — ${msg}`);
    }

    function emit(evt, data) {
        // Flush any pending throttled progress before sending fileend/fileerror
        if (pendingProgressData) { flushProgress(); }
        if (eventCallback) {
            try {
                eventCallback(evt, { ...data, totalBytes, transferredBytes });
            } catch (_) {}
        }
    }

    const events = ['filestart', 'fileprogress', 'fileend', 'fileerror', 'filecancelled'];
    const attachEvents = (uploader) => events.forEach((evt) => {
        uploader.on(evt, (data) => {
            if (evt === 'filestart') {
                // A re-uploaded file was already counted and reported on its first attempt
                if (startedFiles.has(fileKey(data))) return;
                startedFiles.add(fileKey(data));
                totalFiles++;
                totalBytes += (data.fileSize || 0);
                svcLog.debug(`filestart: ${data.fileName} (${((data.fileSize || 0) / 1024).toFixed(1)} KB)`);
            }
            if (evt === 'fileprogress') {
                const prev = fileBytes[fileKey(data)] || 0;
                const curr = data.transferred || 0;
                transferredBytes += (curr - prev);
                fileBytes[fileKey(data)] = curr;
                const now = Date.now();
                if (now - lastProgressLog > 5000) {
                    const pct = totalBytes > 0 ? ((transferredBytes / totalBytes) * 100).toFixed(1) : '0.0';
                    svcLog.debug(`Progress: ${pct}% — ${(transferredBytes / 1048576).toFixed(1)}/${(totalBytes / 1048576).toFixed(1)} MB`);
                    lastProgressLog = now;
                }
                // Throttle fileprogress broadcasts to reduce SSE/DOM overhead
                const progressPayload = { ...data, totalBytes, transferredBytes };
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
                const prev = fileBytes[fileKey(data)] || 0;
                const fileSize = data.fileSize || 0;
                transferredBytes += (fileSize - prev);
                delete fileBytes[fileKey(data)];
                svcLog.debug(`fileend: ${data.fileName} (${((fileSize) / 1024).toFixed(1)} KB)`);
                filesDone++;
                if (data.targetFile) uploadedTargets.add(String(data.targetFile).normalize('NFC'));
                // A file that is sent a second time (sendAgain) keeps its staged copy until then
                const staged = data.targetFile && !sendAgainTargets.has(String(data.targetFile).normalize('NFC')) && stagedByTarget.get(data.targetFile);
                if (staged) {
                    stagedByTarget.delete(data.targetFile);
                    fsPromises.unlink(staged).catch(() => {});
                }
                if (Date.now() - lastSummaryLog >= 60000) {
                    lastSummaryLog = Date.now();
                    const secs = (Date.now() - uploadStartTime) / 1000;
                    svcLog.info(`Progress: ${filesDone}/${plannedFiles || '?'} files${plannedFiles ? ` (${((filesDone * 100) / plannedFiles).toFixed(1)}%)` : ''}, `
                        + `${(filesDone / secs).toFixed(1)} files/s, ${errors.length} error(s), ${Math.round(secs / 60)} min so far`);
                }
            }
            if (evt === 'fileerror') {
                if (!startedFiles.has(fileKey(data))) {
                    startedFiles.add(fileKey(data));
                    totalFiles++;
                    totalBytes += (data.fileSize || 0);
                    emit('filestart', data);
                }
                transferredBytes -= (fileBytes[fileKey(data)] || 0);
                delete fileBytes[fileKey(data)];
                if (!cancelled && retryPassesLeft > 0 && isNotFound(data)) {
                    heldNotFound.set(fileKey(data), data);
                    svcLog.warn(`fileerror (404, will retry): ${data.fileName}`);
                    return;
                }
                recordFileError(data);
            }
            if (evt === 'filecancelled') {
                cancelled = true;
                delete fileBytes[fileKey(data)];
                svcLog.warn(`filecancelled: ${data.fileName}`);
            }
            emit(evt, data);
        });
    });

    /**
     * Reports a file that is not sent to AEM at all, so it is counted as failed like an upload error.
     * Not added to startedFiles: a clashing file shares its AEM path with the one that is uploaded.
     */
    function reportNotUploaded(info, message) {
        const data = { fileName: path.basename(info.filePath || ''), fileSize: info.fileSize || 0, errors: [{ message }] };
        totalFiles++;
        emit('filestart', data);
        recordFileError(data);
        emit('fileerror', data);
    }

    function decodedTarget(info) {
        try { return decodeURIComponent(new URL(info.fileUrl).pathname); } catch (_) { return info.fileUrl; }
    }

    function releaseHeldErrors() {
        heldNotFound.forEach((data) => {
            recordFileError(data);
            emit('fileerror', data);
        });
        heldNotFound.clear();
    }

    // @adobe/httptransfer completes uploaded files strictly one at a time (one completeUpload
    // request per file) whatever maxConcurrent is, which makes it the bottleneck for many small
    // files. Running several independent pipelines over slices of the file list parallelises it.
    function uploadInPipelines(files) {
        // Batches in one queue: pipelines take the next batch when they finish one, so their number can change
        const queue = [];
        for (let b = 0; b < files.length; b += PIPELINE_BATCH_SIZE) queue.push(files.slice(b, b + PIPELINE_BATCH_SIZE));
        let want = Math.max(1, Math.min(pipelines.target, queue.length));
        let running = 0;
        let failure = null;
        return new Promise((resolve, reject) => {
            let timer = null;
            const settle = () => {
                if (running > 0 || (queue.length && !cancelled && !failure)) return;
                clearInterval(timer);
                if (failure) reject(failure); else resolve();
            };
            const worker = async () => {
                running++;
                try {
                    while (!cancelled && !failure && queue.length && running <= want) {
                        const batchOptions = new DirectBinaryUploadOptions()
                            .withUrl(url)
                            .withUploadFiles(queue.shift())
                            .withMaxConcurrent(maxConcurrent)
                            .withHttpRetryCount(httpRetryCount)
                            .withHttpRetryDelay(httpRetryDelay)
                            .withHttpRequestTimeout(httpRequestTimeout)
                            .withHttpOptions(httpOptions);
                        const uploader = new DirectBinaryUpload();
                        attachEvents(uploader);
                        await uploader.uploadFiles(batchOptions);
                    }
                } catch (e) {
                    failure = failure || e;
                } finally {
                    running--;
                    settle();
                }
            };
            const fill = () => { while (running < want && queue.length && !cancelled && !failure) worker(); };
            if (pipelines.next) {
                let lastDone = filesDone;
                let lastErrors = errors.length;
                let lastAt = Date.now();
                timer = setInterval(() => {
                    const now = Date.now();
                    const rate = (filesDone - lastDone) / ((now - lastAt) / 1000);
                    const d = pipelines.next({ rate, errors: errors.length - lastErrors, canGrow: config.canGrow() });
                    lastDone = filesDone; lastErrors = errors.length; lastAt = now;
                    if (d.changed) {
                        svcLog.info(`Pipelines ${want} -> ${d.target}: ${d.reason}`);
                        want = d.target;
                        if (eventCallback) { try { eventCallback('pipelines', { pipelines: want, reason: d.reason }); } catch (_) {} }
                        fill();
                    }
                }, ADAPT_WINDOW_MS);
                if (timer.unref) timer.unref();
            }
            fill();
            settle();
        });
    }

    async function createFolderWithRetry(fsUpload, uploadResult, dir) {
        for (let attempt = 0; ; attempt++) {
            try {
                return await fsUpload.createAemFolderFromFileSystemInfo(options, uploadResult, dir);
            } catch (e) {
                const status = Number((/status code (\d+)/.exec(e.message || '') || [])[1]) || 0;
                const transient = !status || status === 429 || status >= 500;
                if (!transient || attempt >= FOLDER_RETRY_DELAYS_MS.length || cancelled) throw e;
                svcLog.warn(`Folder ${dir.getRemotePath()} failed (${e.message}); retrying in ${FOLDER_RETRY_DELAYS_MS[attempt] / 1000}s`);
                await new Promise((resolve) => setTimeout(resolve, FOLDER_RETRY_DELAYS_MS[attempt]));
            }
        }
    }

    // Parents must exist before children, so folders are created level by level,
    // with each level's folders created a few at a time.
    // Returns the remote paths of folders that could not be created; their files are not uploaded.
    async function createFolders(fsUpload, uploadResult, directories) {
        const failed = [];
        const underFailed = (remote) => failed.some((f) => remote.startsWith(f + '/'));
        const byDepth = new Map();
        directories.forEach((dir) => {
            const depth = dir.getRemotePath().split('/').length;
            if (!byDepth.has(depth)) byDepth.set(depth, []);
            byDepth.get(depth).push(dir);
        });
        const depths = [...byDepth.keys()].sort((a, b) => a - b);
        for (const depth of depths) {
            const level = byDepth.get(depth).filter((dir) => !underFailed(dir.getRemotePath()));
            for (let i = 0; i < level.length; i += FOLDER_CREATE_CONCURRENCY) {
                if (cancelled) return failed;
                const slice = level.slice(i, i + FOLDER_CREATE_CONCURRENCY);
                const results = await Promise.allSettled(slice.map((dir) => createFolderWithRetry(fsUpload, uploadResult, dir)));
                results.forEach((r, k) => {
                    if (r.status === 'fulfilled') return;
                    const remote = slice[k].getRemotePath();
                    const msg = (r.reason && r.reason.message) || String(r.reason);
                    failed.push(remote);
                    errors.push(`Folder ${remote}: ${msg}`);
                    svcLog.error(`Folder ${remote} could not be created: ${msg}`);
                });
            }
        }
        return failed;
    }

    /**
     * Two local names can clean up to the same AEM name ("a#b.dita" and "a-b.dita", or "My Folder" and "My-Folder").
     * Uploading both would silently keep only one, so only the first is uploaded and the others are reported.
     */
    function dropNameClashes(uploadFiles) {
        const byTarget = new Map();
        const keep = [];
        uploadFiles.forEach((info) => {
            const target = decodedTarget(info).normalize('NFC');
            const first = byTarget.get(target);
            if (!first) { byTarget.set(target, info); keep.push(info); return; }
            reportNotUploaded(info, `Not uploaded: ${info.filePath} and ${first.filePath} both become ${target} in AEM. Rename one of them.`);
        });
        return keep;
    }

    async function retryNotFoundFiles(uploadFiles) {
        const fileInfoByTarget = new Map();
        uploadFiles.forEach((info) => {
            try { fileInfoByTarget.set(decodeURIComponent(new URL(info.fileUrl).pathname), info); } catch (_) {}
        });

        for (const delayMs of NOT_FOUND_RETRY_DELAYS_MS) {
            if (heldNotFound.size === 0 || cancelled) break;
            const retryFiles = [];
            heldNotFound.forEach((_data, key) => {
                const info = fileInfoByTarget.get(key);
                if (info && info.filePath) {
                    retryFiles.push(info);
                    heldNotFound.delete(key);
                }
            });
            if (retryFiles.length === 0) break;
            retryPassesLeft--;

            svcLog.warn(`${retryFiles.length} file(s) failed with 404 (new folder may not be visible yet); retrying in ${delayMs / 1000}s`);
            await new Promise((resolve) => setTimeout(resolve, delayMs));
            if (cancelled) break;
            await uploadInPipelines(retryFiles);
        }
        // Anything still held (cancelled, or no local file info to retry with) is a real failure
        releaseHeldErrors();
    }

    function buildResult() {
        const success = errors.length === 0 && !cancelled;
        const partial = !success && !cancelled && errors.length > 0 && totalFiles > errors.length;
        return { success, partial, totalFiles, totalBytes, transferredBytes, errors, cancelled };
    }

    /**
     * Waits until AEM Guides has processed the DITA files of `items` that were uploaded, so the next wave's maps
     * link to their GUIDs (lib/guides-wait.js). Never fails the upload: when AEM cannot be asked, Guides shows no
     * state at all, or it takes longer than GUIDES_WAIT_MAX_MS, the upload goes on.
     */
    /** Tells the caller (the page) what the upload waits for: { state: 'waiting' | 'again' | 'done', ... }. */
    function guidesEvent(data) {
        if (eventCallback) { try { eventCallback('guideswait', data); } catch (_) {} }
    }

    async function waitForGuides(items, step = {}) {
        if (!guidesProcessing || cancelled) return;
        const targets = items.map((i) => decodedTarget(i).normalize('NFC')).filter((t) => uploadedTargets.has(t) && (GUIDES_FILE.test(t) || linkedTargets.has(t)));
        if (!targets.length) return;
        let root;
        try { root = decodeURIComponent(new URL(url).pathname).replace(/\/+$/, ''); } catch (_) { return; }
        svcLog.info(`Waiting for AEM Guides to process ${targets.length} file(s) before uploading the files that link to them`);
        // step: { wave, waves } (the next wave and how many there are) or { again: n } (before the second upload)
        const event = (pending, secs) => ({ state: 'waiting', files: targets.length, processed: targets.length - pending, secs, ...step });
        guidesEvent(event(targets.length, 0));
        const r = await guidesWait.waitForGuides({
            server: { baseUrl, username, password, agent: selfSignedAgent }, roots: [root], targets,
            graceMs: GUIDES_WAIT_GRACE_MS, maxMs: GUIDES_WAIT_MAX_MS,
            pollMs: plannedFiles > 5000 ? Math.max(GUIDES_WAIT_POLL_MS, GUIDES_WAIT_POLL_LARGE_MS) : GUIDES_WAIT_POLL_MS,
            isCancelled: () => cancelled,
            onWaiting: ({ pending, secs }) => svcLog.info(`Still waiting for AEM Guides: ${pending} of ${targets.length} file(s) not processed after ${secs}s`),
            onPoll: ({ pending, secs }) => guidesEvent(event(pending, secs)),
        });
        guidesEvent({ state: 'done', outcome: r.outcome, secs: r.secs, files: targets.length, processed: targets.length - r.pending.length });
        if (r.outcome === 'done') svcLog.info(`AEM Guides processed them in ${r.secs}s`);
        else if (r.outcome === 'error') svcLog.warn(`Could not read the AEM Guides state of ${root} (${r.message}); uploading the next maps without waiting`);
        else if (r.outcome === 'no-guides') {
            guidesProcessing = false;
            svcLog.warn(`AEM Guides shows no processing state on these files after ${r.secs}s (Guides does not process ${root}?); uploading the rest without waiting`);
        } else if (r.outcome === 'timeout') {
            svcLog.warn(`AEM Guides has not finished ${r.pending.length} file(s) after ${r.secs}s (for example ${r.pending[0]}); uploading the next maps anyway. Check their links with Check in AEM.`);
        }
    }

    async function uploadAll() {
        // Nothing left to send (every file was already in AEM): a finished upload of 0 files
        if (!localPaths.length) {
            svcLog.info('Nothing to upload: every file is already in AEM');
            if (eventCallback) { try { eventCallback('uploadplan', { files: 0, bytes: 0 }); } catch (_) {} }
            return;
        }
        const fsUpload = new FileSystemUpload();
        const uploadResult = new UploadResult(fsUpload.getOptions(), options);
        await fsUpload.createTargetFolder(options, uploadResult);
        const { directories, files, errors: walkErrors } = await fsUpload.getUploadInformation(options, localPaths);
        walkErrors.forEach((e) => {
            const msg = (e && e.message) || String(e);
            svcLog.warn('Could not read local path:', msg);
            errors.push(`Could not read local path: ${msg}`);
        });
        let uploadFiles = fsUpload.convertToUploadFilesWithUrl(options, files);
        plannedFiles = uploadFiles.length;
        if (config.deleteStagedOnSuccess) {
            uploadFiles.forEach((info) => {
                try { stagedByTarget.set(decodeURIComponent(new URL(info.fileUrl).pathname), info.filePath); } catch (_) {}
            });
        }
        // The library skips temp/hidden files (".*", "~*", "*.tmp", Thumbs.db, ...), so tell the caller the real totals
        if (eventCallback) {
            try {
                eventCallback('uploadplan', { files: uploadFiles.length, bytes: uploadFiles.reduce((sum, f) => sum + (f.fileSize || 0), 0) });
            } catch (_) {}
        }

        uploadFiles = dropNameClashes(uploadFiles);

        const foldersStart = Date.now();
        const failedFolders = await createFolders(fsUpload, uploadResult, directories);
        svcLog.info(`Folders ready: ${directories.length - failedFolders.length} of ${directories.length} in ${((Date.now() - foldersStart) / 1000).toFixed(1)}s`);
        if (cancelled) return;
        if (failedFolders.length) {
            uploadFiles = uploadFiles.filter((info) => {
                const target = decodedTarget(info);
                const folder = failedFolders.find((f) => target.startsWith(f + '/'));
                if (!folder) return true;
                reportNotUploaded(info, `Not uploaded: AEM could not create the folder ${folder}`);
                return false;
            });
        }
        if (uploadFiles.length === 0) return;

        const filesStart = Date.now();
        svcLog.info(`Uploading ${uploadFiles.length} files in ${Math.min(pipelines.target, uploadFiles.length)} parallel pipelines${pipelines.next ? ` (adaptive, up to ${MAX_UPLOAD_PIPELINES})` : ''}`);
        const plan = await ditaOrder.planUpload(uploadFiles);
        const { waves } = plan;
        plan.again.forEach((i) => sendAgainTargets.add(decodedTarget(i).normalize('NFC')));
        plan.linked.forEach((i) => linkedTargets.add(decodedTarget(i).normalize('NFC')));
        if (waves.length > 1 || plan.again.length) {
            svcLog.info(`DITA links found: uploading in ${waves.length} wave(s) (${waves.map((w) => w.length).join(' + ')} files), each file after AEM Guides `
                + 'has processed the files it links to (maps last)'
                + (plan.again.length ? `; ${plan.again.length} file(s) that link to each other or sit deeper than the last wave are sent once more at the end` : ''));
        }
        for (let w = 0; w < waves.length && !cancelled; w++) {
            if (w > 0) {
                await waitForGuides(waves.slice(0, w).flat(), { wave: w + 1, waves: waves.length });
                if (cancelled) break;
                retryPassesLeft = NOT_FOUND_RETRY_DELAYS_MS.length;
            }
            await uploadInPipelines(waves[w]);
            if (heldNotFound.size > 0) await retryNotFoundFiles(waves[w]);
        }
        if (plan.again.length) await sendAgain(plan.again, waves.flat());
        svcLog.info(`File pass done in ${((Date.now() - filesStart) / 1000).toFixed(1)}s`);
    }

    /**
     * Sends `items` a second time once AEM Guides has processed every file of the upload, so the links Guides could
     * not resolve the first time (files that link to each other) get the GUIDs of their targets. The second upload
     * updates each asset in place (it keeps its GUID) and is not counted again in the progress; a failure is
     * reported as an error of the upload.
     */
    async function sendAgain(items, all) {
        const files = items.filter((i) => uploadedTargets.has(decodedTarget(i).normalize('NFC')));
        // The uploaded ones lose their staged copy now, as after a normal upload; a failed one keeps it
        const removeStaged = () => files.forEach((i) => {
            const t = decodedTarget(i).normalize('NFC');
            sendAgainTargets.delete(t);
            const staged = stagedByTarget.get(t);
            if (staged) { stagedByTarget.delete(t); fsPromises.unlink(staged).catch(() => {}); }
        });
        if (cancelled || !files.length) { if (!cancelled) removeStaged(); return; }
        await waitForGuides(all, { again: files.length });
        if (cancelled) return;
        if (!guidesProcessing) { removeStaged(); return; }
        svcLog.info(`Sending ${files.length} file(s) once more so AEM Guides links them to the GUIDs of the files they point at`);
        guidesEvent({ state: 'again', files: files.length });
        const uploader = new DirectBinaryUpload();
        uploader.on('fileerror', (data) => {
            const e = data.errors && data.errors[0];
            const msg = (e && e.message) || e || 'upload failed';
            errors.push(`${data.fileName}: links not refreshed (second upload failed: ${msg})`);
            svcLog.error(`Second upload of ${data.fileName} failed: ${msg}`);
        });
        try {
            await uploader.uploadFiles(new DirectBinaryUploadOptions()
                .withUrl(url)
                .withUploadFiles(files)
                .withMaxConcurrent(maxConcurrent)
                .withHttpRetryCount(httpRetryCount)
                .withHttpRetryDelay(httpRetryDelay)
                .withHttpRequestTimeout(httpRequestTimeout)
                .withHttpOptions(httpOptions));
        } catch (e) {
            errors.push(`Links of ${files.length} file(s) not refreshed: ${e.message || e}`);
        }
        guidesEvent({ state: 'done', outcome: 'again', files: files.length });
        removeStaged();
    }

    const promise = uploadAll()
        .then(() => {
            const elapsed = ((Date.now() - uploadStartTime) / 1000).toFixed(1);
            const speedMBs = elapsed > 0 ? ((transferredBytes / 1048576) / elapsed).toFixed(2) : '0';
            svcLog.info(`Upload finished: ${totalFiles} files, ${(transferredBytes / 1048576).toFixed(1)} MB in ${elapsed}s (${speedMBs} MB/s), errors: ${errors.length}, cancelled: ${cancelled}`);
            return buildResult();
        })
        .catch((err) => {
            svcLog.error('Upload failed with exception:', err.message);
            releaseHeldErrors();
            errors.push(err.message || String(err));
            return buildResult();
        });

    function cancel() {
        svcLog.warn('Upload cancellation triggered');
        cancelled = true;
        // Checked before folder creation, between pipeline batches and before 404 retries
    }

    return { promise, cancel };
}

/**
 * Sends local files to exact AEM asset paths in folders that already exist: the maps of a package, over the copies
 * the package installed (lib/package-maps.js). No replace and no new version is asked for, so AEM updates the
 * binary of an existing asset in place and the asset keeps its node and its Guides GUID.
 * files: [{ localPath, aemPath }]. Resolves to { ok, uploaded: [aemPath], errors: [string] }; never rejects.
 */
async function uploadToPaths({ baseUrl: rawBaseUrl, username, password, maxConcurrent = 4 }, files) {
    const baseUrl = normalizeAemBaseUrl(rawBaseUrl).replace(/\/+$/, '');
    const encode = (p) => p.split('/').map(encodeURIComponent).join('/');
    const errors = [];
    const uploaded = [];
    const uploadFiles = [];
    for (const f of files) {
        const name = path.posix.basename(f.aemPath);
        // AEM's initiateUpload refuses these characters in a file name (see public/aem-names.js)
        if (/[{}&?#%]/.test(name)) { errors.push(`${f.aemPath}: AEM's direct upload does not accept this file name`); continue; }
        let size;
        try { size = (await fsPromises.stat(f.localPath)).size; } catch (e) { errors.push(`${f.aemPath}: ${e.message}`); continue; }
        uploadFiles.push({ fileUrl: `${baseUrl}${encode(path.posix.dirname(f.aemPath))}/${encodeURIComponent(name)}`, filePath: f.localPath, fileSize: size });
    }
    if (!uploadFiles.length) return { ok: errors.length === 0, uploaded, errors };
    const httpOptions = {
        headers: { Authorization: aemAuthorization(username, password) },
        timeout: 120000,
        retryOptions: { retryInitialDelay: 8000, retryMaxCount: 5, retryAllErrors: false, retryOnHttpResponseError: (e) => e.status === 429 || e.status >= 500 },
    };
    const agent = buildSelfSignedAgent(baseUrl);
    if (agent) httpOptions.agent = agent;
    const uploader = new DirectBinaryUpload();
    uploader.on('fileend', (d) => uploaded.push(String(d.targetFile || '').normalize('NFC')));
    uploader.on('fileerror', (d) => {
        const e = d.errors && d.errors[0];
        errors.push(`${d.targetFile || d.fileName}: ${(e && e.message) || e || 'upload failed'}`);
    });
    try {
        await uploader.uploadFiles(new DirectBinaryUploadOptions()
            .withUrl(`${baseUrl}/content/dam`)
            .withUploadFiles(uploadFiles)
            .withMaxConcurrent(maxConcurrent)
            .withHttpRetryCount(5)
            .withHttpRetryDelay(8000)
            .withHttpRequestTimeout(120000)
            .withHttpOptions(httpOptions));
    } catch (e) {
        errors.push(e.message || String(e));
    }
    return { ok: errors.length === 0, uploaded, errors };
}

module.exports = { TARGETS, runUpload, uploadToPaths, scanDirectory, validateUploadConfig, normalizeAemBaseUrl, checkAemCredentials };
