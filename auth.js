/**
 * Sign-in with AEM credentials and per-user access.
 *
 * Users sign in with an AEM server URL, username and password, checked against that server. The
 * password is kept only in this process's memory for the session (it is needed to upload on the
 * user's behalf) and is dropped on sign-out, idle expiry or restart; it is never written to disk.
 */
const crypto = require('crypto');

const COOKIE_NAME = 'gus_session';
const SESSION_IDLE_MS = (parseInt(process.env.SESSION_IDLE_HOURS, 10) || 8) * 60 * 60 * 1000;
const SESSION_MAX_MS = (parseInt(process.env.SESSION_MAX_HOURS, 10) || 24) * 60 * 60 * 1000;
const API_KEY = process.env.API_KEY || '';
// Comma-separated AEM user ids, optionally scoped to a host: "jdoe" or "jdoe@author-p1-e2.adobeaemcloud.com"
const ADMIN_USERS = (process.env.ADMIN_USERS || '').split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);

const sessions = new Map();

function parseCookies(req) {
    const out = {};
    (req.headers.cookie || '').split(';').forEach((part) => {
        const i = part.indexOf('=');
        if (i > 0) out[part.slice(0, i).trim()] = decodeURIComponent(part.slice(i + 1).trim());
    });
    return out;
}

function isAdminUser(id, host) {
    const lid = String(id).toLowerCase();
    return ADMIN_USERS.includes(lid) || ADMIN_USERS.includes(`${lid}@${String(host).toLowerCase()}`);
}

function publicUser(s) {
    return { id: s.user.id, name: s.user.name, key: s.user.key, aemUrl: s.aem.baseUrl, host: s.user.host, isAdmin: s.isAdmin };
}

function cookieHeader(req, value, maxAgeSec) {
    const secure = req.secure || req.headers['x-forwarded-proto'] === 'https' ? '; Secure' : '';
    return `${COOKIE_NAME}=${encodeURIComponent(value)}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${maxAgeSec}${secure}`;
}

function createSession(req, res, { baseUrl, username, password, userId, userName }) {
    const host = new URL(baseUrl).host;
    const id = userId || username;
    const sid = crypto.randomBytes(32).toString('hex');
    const now = Date.now();
    const s = {
        sid,
        user: { id, name: userName || id, host, key: `${id}@${host}`.toLowerCase() },
        aem: { baseUrl, username, password },
        isAdmin: isAdminUser(id, host),
        createdAt: now,
        lastSeen: now,
    };
    sessions.set(sid, s);
    res.setHeader('Set-Cookie', cookieHeader(req, sid, Math.floor(SESSION_MAX_MS / 1000)));
    return s;
}

function getSession(req) {
    const sid = parseCookies(req)[COOKIE_NAME];
    if (!sid) return null;
    const s = sessions.get(sid);
    if (!s) return null;
    const now = Date.now();
    if (now - s.lastSeen > SESSION_IDLE_MS || now - s.createdAt > SESSION_MAX_MS) {
        sessions.delete(sid);
        return null;
    }
    s.lastSeen = now;
    return s;
}

function destroySession(req, res) {
    const sid = parseCookies(req)[COOKIE_NAME];
    if (sid) sessions.delete(sid);
    res.setHeader('Set-Cookie', cookieHeader(req, '', 0));
}

// Drop expired sessions (and their in-memory passwords) even if their users never come back
setInterval(() => {
    const now = Date.now();
    sessions.forEach((s, sid) => {
        if (now - s.lastSeen > SESSION_IDLE_MS || now - s.createdAt > SESSION_MAX_MS) sessions.delete(sid);
    });
}, 10 * 60 * 1000).unref();

const PUBLIC_API_PATHS = ['/health', '/auth/login', '/auth/me', '/auth/logout'];

/**
 * Sets req.user for every /api request: a signed-in session, or an API-key client (admin, sends
 * AEM credentials in the request body as before). Rejects everything else with 401.
 * Mounted on '/api/', so req.path is relative to /api.
 */
function requireAuth(log) {
    return (req, res, next) => {
        // Cookies are sent with cross-site requests in some cases; reject writes from other origins
        if (req.method !== 'GET' && req.headers.origin) {
            let originHost = '';
            try { originHost = new URL(req.headers.origin).host; } catch (_) { /* invalid origin */ }
            if (originHost !== req.headers.host) {
                log.warn(`Rejected cross-origin ${req.method} ${req.originalUrl} from ${req.headers.origin}`);
                return res.status(403).json({ error: 'Cross-origin request rejected' });
            }
        }
        if (API_KEY) {
            const provided = req.headers['x-api-key'] || req.query.apiKey;
            if (provided === API_KEY) {
                req.user = { id: 'api-key', name: 'API key', key: 'api-key', isAdmin: true, viaApiKey: true };
                return next();
            }
        }
        const s = getSession(req);
        if (s) {
            req.user = { ...publicUser(s), aem: s.aem };
            return next();
        }
        if (PUBLIC_API_PATHS.includes(req.path)) return next();
        return res.status(401).json({ error: 'Your session has ended. Please sign in again.', code: 'AUTH_REQUIRED' });
    };
}

function requireAdmin(req, res, next) {
    if (req.user && req.user.isAdmin) return next();
    return res.status(403).json({ error: 'Only administrators can do this.', code: 'ADMIN_ONLY' });
}

/** Whether the current user may see or act on something owned by ownerKey (null = created before sign-in existed). */
function canAccess(req, ownerKey) {
    if (!req.user) return false;
    if (req.user.isAdmin) return true;
    return !!ownerKey && ownerKey === req.user.key;
}

/** AEM target and credentials for an upload: the signed-in session's, or the request body's for API-key clients. */
function resolveAemTarget(req) {
    if (req.user && req.user.aem) return { ...req.user.aem };
    const b = req.body || {};
    return { baseUrl: b.baseUrl, username: b.username, password: b.password };
}

module.exports = {
    createSession, getSession, destroySession, publicUser, requireAuth, requireAdmin, canAccess, resolveAemTarget,
    sessionCount: () => sessions.size, adminsConfigured: () => ADMIN_USERS.length > 0, apiKeyEnabled: () => !!API_KEY,
};
