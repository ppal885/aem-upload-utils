const fs = require('fs');
const fsPromises = require('fs').promises;
const path = require('path');
const AemNames = require('./public/aem-names');

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
            if (directories.length + files.length > maximumPaths) {
                throw new UploadError(`Walked directory exceeded the maximum number of ${maximumPaths} paths`, ErrorCodes.TOO_LARGE);
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
const DEFAULT_UPLOAD_PIPELINES = 8;
// @adobe/aem-upload cannot abort a running transfer, so each pipeline uploads in batches and
// checks for a cancel between them; this bounds how much is still sent after Cancel.
const PIPELINE_BATCH_SIZE = 50;
const LOCAL_HOSTNAMES = ['localhost', '127.0.0.1', '::1'];

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
            Authorization: `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`,
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
    const credentials = Buffer.from(`${username}:${password}`).toString('base64');

    svcLog.info(`Upload starting → ${url}`);
    svcLog.info(`  Local paths: ${localPaths.join(', ')}`);
    svcLog.info(`  Config: concurrent=${config.maxConcurrent || 4}, pipelines=${config.uploadPipelines || DEFAULT_UPLOAD_PIPELINES}, maxFiles=${config.maxUploadFiles || 50000}, retries=${config.httpRetryCount ?? 3}, timeout=${config.httpRequestTimeout ?? 60000}ms`);

    const maxConcurrent = config.maxConcurrent || 4;
    const maxUploadFiles = config.maxUploadFiles || 50000;
    const httpRetryCount = config.httpRetryCount != null ? config.httpRetryCount : 5;
    const httpRetryDelay = config.httpRetryDelay != null ? config.httpRetryDelay : 8000;
    const httpRequestTimeout = config.httpRequestTimeout != null ? config.httpRequestTimeout : 120000;
    const uploadPipelines = config.uploadPipelines || DEFAULT_UPLOAD_PIPELINES;

    const httpOptions = {
        headers: {
            Authorization: `Basic ${credentials}`,
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
        .withFolderNodeNameProcessor(async (folderName) => (config.keepNodeNames ? AemNames.keepNodeName(folderName) : AemNames.folderNodeName(folderName)))
        .withAssetNodeNameProcessor(async (assetName) => (config.keepNodeNames ? AemNames.keepNodeName(assetName) : AemNames.assetNodeName(assetName)));

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
                const prev = fileBytes[data.fileName] || 0;
                const fileSize = data.fileSize || 0;
                transferredBytes += (fileSize - prev);
                delete fileBytes[data.fileName];
                svcLog.debug(`fileend: ${data.fileName} (${((fileSize) / 1024).toFixed(1)} KB)`);
                filesDone++;
                const staged = data.targetFile && stagedByTarget.get(data.targetFile);
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
                transferredBytes -= (fileBytes[data.fileName] || 0);
                delete fileBytes[data.fileName];
                if (!cancelled && retryPassesLeft > 0 && isNotFound(data)) {
                    heldNotFound.set(fileKey(data), data);
                    svcLog.warn(`fileerror (404, will retry): ${data.fileName}`);
                    return;
                }
                recordFileError(data);
            }
            if (evt === 'filecancelled') {
                cancelled = true;
                delete fileBytes[data.fileName];
                svcLog.warn(`filecancelled: ${data.fileName}`);
            }
            emit(evt, data);
        });
    });

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
        const pipelineCount = Math.max(1, Math.min(uploadPipelines, files.length));
        const sliceSize = Math.ceil(files.length / pipelineCount);
        const runs = [];
        for (let i = 0; i < files.length; i += sliceSize) {
            const slice = files.slice(i, i + sliceSize);
            runs.push((async () => {
                for (let b = 0; b < slice.length; b += PIPELINE_BATCH_SIZE) {
                    if (cancelled) return;
                    const batchOptions = new DirectBinaryUploadOptions()
                        .withUrl(url)
                        .withUploadFiles(slice.slice(b, b + PIPELINE_BATCH_SIZE))
                        .withMaxConcurrent(maxConcurrent)
                        .withHttpRetryCount(httpRetryCount)
                        .withHttpRetryDelay(httpRetryDelay)
                        .withHttpRequestTimeout(httpRequestTimeout)
                        .withHttpOptions(httpOptions);
                    const uploader = new DirectBinaryUpload();
                    attachEvents(uploader);
                    await uploader.uploadFiles(batchOptions);
                }
            })());
        }
        return Promise.all(runs);
    }

    // Parents must exist before children, so folders are created level by level,
    // with each level's folders created a few at a time.
    async function createFolders(fsUpload, uploadResult, directories) {
        const byDepth = new Map();
        directories.forEach((dir) => {
            const depth = dir.getRemotePath().split('/').length;
            if (!byDepth.has(depth)) byDepth.set(depth, []);
            byDepth.get(depth).push(dir);
        });
        const depths = [...byDepth.keys()].sort((a, b) => a - b);
        for (const depth of depths) {
            const level = byDepth.get(depth);
            for (let i = 0; i < level.length; i += FOLDER_CREATE_CONCURRENCY) {
                if (cancelled) return;
                await Promise.all(level.slice(i, i + FOLDER_CREATE_CONCURRENCY)
                    .map((dir) => fsUpload.createAemFolderFromFileSystemInfo(options, uploadResult, dir)));
            }
        }
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

    async function uploadAll() {
        const fsUpload = new FileSystemUpload();
        const uploadResult = new UploadResult(fsUpload.getOptions(), options);
        await fsUpload.createTargetFolder(options, uploadResult);
        const { directories, files, errors: walkErrors } = await fsUpload.getUploadInformation(options, localPaths);
        walkErrors.forEach((e) => {
            const msg = (e && e.message) || String(e);
            svcLog.warn('Could not read local path:', msg);
            errors.push(`Could not read local path: ${msg}`);
        });
        const uploadFiles = fsUpload.convertToUploadFilesWithUrl(options, files);
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

        const foldersStart = Date.now();
        await createFolders(fsUpload, uploadResult, directories);
        svcLog.info(`Folders ready: ${directories.length} in ${((Date.now() - foldersStart) / 1000).toFixed(1)}s`);
        if (cancelled || uploadFiles.length === 0) return;

        const filesStart = Date.now();
        svcLog.info(`Uploading ${uploadFiles.length} files in ${Math.min(uploadPipelines, uploadFiles.length)} parallel pipelines`);
        await uploadInPipelines(uploadFiles);
        svcLog.info(`File pass done in ${((Date.now() - filesStart) / 1000).toFixed(1)}s`);

        if (heldNotFound.size > 0) await retryNotFoundFiles(uploadFiles);
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

module.exports = { TARGETS, runUpload, scanDirectory, validateUploadConfig, normalizeAemBaseUrl, checkAemCredentials };
