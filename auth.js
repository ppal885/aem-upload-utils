/**
 * Team member accounts and sessions.
 *
 * Team members sign in with their work email (ALLOWED_EMAIL_DOMAINS, default adobe.com) and a password
 * for this tool. People create their own account on the sign-in page, or an admin adds them (Team
 * members page, or tools/manage-users.js) with a temporary password that must be changed at first
 * sign-in. Emails listed in ADMIN_EMAILS become admins when they create their account. Passwords are stored only as scrypt hashes in users.json.
 * AEM credentials are not part of the session: they are entered with each upload.
 */
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const COOKIE_NAME = 'gus_session';
const SESSION_IDLE_MS = (parseInt(process.env.SESSION_IDLE_HOURS, 10) || 8) * 60 * 60 * 1000;
const SESSION_MAX_MS = (parseInt(process.env.SESSION_MAX_HOURS, 10) || 24) * 60 * 60 * 1000;
const API_KEY = process.env.API_KEY || '';
const USERS_FILE = process.env.USERS_FILE || path.join(__dirname, 'users.json');
// No length rule by default (team decision); set MIN_PASSWORD_LENGTH to require one
const MIN_PASSWORD_LENGTH = Math.max(parseInt(process.env.MIN_PASSWORD_LENGTH, 10) || 1, 1);
const ALLOWED_EMAIL_DOMAINS = (process.env.ALLOWED_EMAIL_DOMAINS || 'adobe.com')
    .split(',').map((d) => d.trim().toLowerCase().replace(/^@/, '')).filter(Boolean);
const ADMIN_EMAILS = (process.env.ADMIN_EMAILS || '').split(',').map((e) => e.trim().toLowerCase()).filter(Boolean);
const SIGNUP_ENABLED = !/^(0|false|no)$/i.test(process.env.SIGNUP_ENABLED || '');
const SCRYPT = { N: 16384, r: 8, p: 1, keylen: 64 };

// Sessions survive a restart, so a deploy does not sign everyone out. The file holds only a SHA-256 of each
// session id (the cookie value), so it cannot be used to sign in; it is written atomically, readable by the
// server's user only, at most every few seconds and once more at shutdown (flushSessions).
const SESSIONS_FILE = process.env.SESSIONS_FILE || path.join(__dirname, 'sessions.json');
const SESSIONS_SAVE_DELAY_MS = 5000;
const sessions = new Map();
const sidKey = (sid) => crypto.createHash('sha256').update(String(sid)).digest('hex');
let saveTimer = null;

function flushSessions() {
    clearTimeout(saveTimer);
    saveTimer = null;
    try {
        const list = [...sessions.entries()].map(([key, s]) => ({ key, user: s.user, mustChangePassword: s.mustChangePassword, createdAt: s.createdAt, lastSeen: s.lastSeen }));
        const tmp = SESSIONS_FILE + '.tmp';
        fs.writeFileSync(tmp, JSON.stringify(list), { mode: 0o600 });
        fs.renameSync(tmp, SESSIONS_FILE);
    } catch (_) { /* sessions stay in memory; the next save tries again */ }
}

function saveSessionsSoon() {
    if (!saveTimer) saveTimer = setTimeout(flushSessions, SESSIONS_SAVE_DELAY_MS);
    if (saveTimer.unref) saveTimer.unref();
}

// ─── User store ───
function normalizeEmail(email) {
    return String(email || '').trim().toLowerCase();
}

// Sessions from before a restart: expired ones, and those of deleted or disabled accounts, are dropped;
// name and role come from the account, so a change made by an admin applies
function loadSessions() {
    let list = [];
    try { list = JSON.parse(fs.readFileSync(SESSIONS_FILE, 'utf8')); } catch (_) { return; }
    if (!Array.isArray(list)) return;
    const users = new Map(loadUsers().map((u) => [u.email, u]));
    const now = Date.now();
    for (const s of list) {
        const u = s && s.user && users.get(s.user.email);
        if (!u || u.disabled || typeof s.key !== 'string') continue;
        if (now - s.lastSeen > SESSION_IDLE_MS || now - s.createdAt > SESSION_MAX_MS) continue;
        sessions.set(s.key, { user: { email: u.email, name: u.name, role: u.role }, mustChangePassword: !!(s.mustChangePassword || u.mustChangePassword), createdAt: s.createdAt, lastSeen: s.lastSeen });
    }
}

function isValidEmail(email) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function isAllowedDomain(email) {
    const domain = String(email).split('@').pop();
    return ALLOWED_EMAIL_DOMAINS.includes(domain);
}

function domainHint() {
    return ALLOWED_EMAIL_DOMAINS.map((d) => '@' + d).join(' or ');
}

/** Throws a 400 unless the email is well formed and on an allowed domain. */
function assertTeamEmail(e) {
    if (!isValidEmail(e)) throw Object.assign(new Error('Enter a valid email address.'), { status: 400 });
    if (!isAllowedDomain(e)) throw Object.assign(new Error(`Use your Adobe email address (${domainHint()}).`), { status: 400, code: 'EMAIL_DOMAIN' });
}

function assertNewPassword(pw, email) {
    if (!pw) throw Object.assign(new Error('Enter a password.'), { status: 400 });
    if (pw.length < MIN_PASSWORD_LENGTH) throw Object.assign(new Error(`The password must have at least ${MIN_PASSWORD_LENGTH} characters.`), { status: 400 });
    if (pw.toLowerCase() === email || pw.toLowerCase() === email.split('@')[0]) {
        throw Object.assign(new Error('The password cannot be your email or user name.'), { status: 400 });
    }
}

function loadUsers() {
    try {
        const users = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
        return Array.isArray(users) ? users : [];
    } catch (e) {
        if (e.code === 'ENOENT') return [];
        throw new Error(`Cannot read ${USERS_FILE}: ${e.message}`);
    }
}

function saveUsers(users) {
    const tmp = USERS_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(users, null, 2), { mode: 0o600 });
    fs.renameSync(tmp, USERS_FILE);
}

function hashPassword(password) {
    const salt = crypto.randomBytes(16);
    const hash = crypto.scryptSync(password, salt, SCRYPT.keylen, { N: SCRYPT.N, r: SCRYPT.r, p: SCRYPT.p });
    return `scrypt$${salt.toString('hex')}$${hash.toString('hex')}`;
}

function verifyPassword(password, stored) {
    const [scheme, saltHex, hashHex] = String(stored || '').split('$');
    if (scheme !== 'scrypt' || !saltHex || !hashHex) return false;
    const expected = Buffer.from(hashHex, 'hex');
    const actual = crypto.scryptSync(password, Buffer.from(saltHex, 'hex'), expected.length, { N: SCRYPT.N, r: SCRYPT.r, p: SCRYPT.p });
    return crypto.timingSafeEqual(actual, expected);
}

function generateTemporaryPassword() {
    // 16 chars from an unambiguous alphabet (no 0/O/1/l/I)
    const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789';
    const bytes = crypto.randomBytes(16);
    return Array.from(bytes, (b) => alphabet[b % alphabet.length]).join('');
}

function publicAccount(u) {
    return { email: u.email, name: u.name, role: u.role, disabled: !!u.disabled, mustChangePassword: !!u.mustChangePassword,
        createdAt: u.createdAt, createdBy: u.createdBy || null, lastSignInAt: u.lastSignInAt || null,
        resetRequestedAt: u.resetRequestedAt || null };
}

function listUsers() {
    return loadUsers().map(publicAccount);
}

/** Creates an account with a temporary password; returns { user, temporaryPassword }. */
function addUser({ email, name, role, createdBy }) {
    const e = normalizeEmail(email);
    assertTeamEmail(e);
    const r = role === 'admin' ? 'admin' : 'member';
    const users = loadUsers();
    if (users.some((u) => u.email === e)) throw Object.assign(new Error('A team member with this email already exists.'), { status: 409 });
    const temporaryPassword = generateTemporaryPassword();
    const user = { email: e, name: String(name || '').trim() || e.split('@')[0], role: r, passwordHash: hashPassword(temporaryPassword),
        mustChangePassword: true, disabled: false, createdAt: new Date().toISOString(), createdBy: createdBy || null };
    users.push(user);
    saveUsers(users);
    return { user: publicAccount(user), temporaryPassword };
}

/** Self sign-up from the sign-in page; the person chooses their own password. */
function registerUser({ email, name, password }) {
    if (!SIGNUP_ENABLED) throw Object.assign(new Error('Creating accounts is turned off. Ask an admin to add you.'), { status: 403 });
    const e = normalizeEmail(email);
    assertTeamEmail(e);
    const n = String(name || '').trim();
    if (!n) throw Object.assign(new Error('Enter your name.'), { status: 400 });
    const pw = String(password || '');
    assertNewPassword(pw, e);
    const users = loadUsers();
    if (users.some((u) => u.email === e)) {
        throw Object.assign(new Error('An account with this email already exists. Sign in, or ask an admin to reset your password.'), { status: 409 });
    }
    // ADMIN_EMAILS become admins; with none configured, the very first account runs the tool
    const role = ADMIN_EMAILS.includes(e) || (!ADMIN_EMAILS.length && !users.some((u) => u.role === 'admin')) ? 'admin' : 'member';
    const user = { email: e, name: n.slice(0, 80), role, passwordHash: hashPassword(pw), mustChangePassword: false, disabled: false,
        createdAt: new Date().toISOString(), createdBy: 'self sign-up' };
    users.push(user);
    saveUsers(users);
    return user;
}

const RESET_TOKEN_MS = 60 * 60 * 1000;

function tokenHash(token) {
    return crypto.createHash('sha256').update(String(token)).digest('hex');
}

/**
 * "Forgot password": returns { user, token } for an active account (token goes in the emailed link),
 * or null. The request is also recorded so admins see it on the Team members page.
 */
function requestPasswordReset(email) {
    const e = normalizeEmail(email);
    assertTeamEmail(e);
    const users = loadUsers();
    const u = users.find((x) => x.email === e);
    if (!u || u.disabled) return null;
    const token = crypto.randomBytes(32).toString('base64url');
    u.resetTokenHash = tokenHash(token);
    u.resetExpiresAt = new Date(Date.now() + RESET_TOKEN_MS).toISOString();
    u.resetRequestedAt = new Date().toISOString();
    saveUsers(users);
    return { user: publicAccount(u), token };
}

function resetPasswordWithToken(token, newPassword) {
    const h = tokenHash(token);
    const users = loadUsers();
    const u = users.find((x) => x.resetTokenHash && x.resetTokenHash === h);
    if (!u || u.disabled || !u.resetExpiresAt || Date.parse(u.resetExpiresAt) < Date.now()) {
        throw Object.assign(new Error('This reset link has expired or was already used. Ask for a new one.'), { status: 400 });
    }
    const pw = String(newPassword || '');
    assertNewPassword(pw, u.email);
    u.passwordHash = hashPassword(pw);
    u.mustChangePassword = false;
    delete u.resetTokenHash; delete u.resetExpiresAt; delete u.resetRequestedAt;
    saveUsers(users);
    endSessionsFor(u.email);
    return publicAccount(u);
}

function authOptions() {
    return { emailDomains: ALLOWED_EMAIL_DOMAINS, signup: SIGNUP_ENABLED, minPasswordLength: MIN_PASSWORD_LENGTH };
}

function updateUser(email, changes) {
    const e = normalizeEmail(email);
    const users = loadUsers();
    const u = users.find((x) => x.email === e);
    if (!u) throw Object.assign(new Error('No team member with this email.'), { status: 404 });
    if (changes.role && changes.role !== u.role) {
        if (u.role === 'admin' && users.filter((x) => x.role === 'admin' && !x.disabled).length === 1) {
            throw Object.assign(new Error('There must be at least one active admin.'), { status: 400 });
        }
        u.role = changes.role === 'admin' ? 'admin' : 'member';
    }
    if (typeof changes.disabled === 'boolean' && changes.disabled !== !!u.disabled) {
        if (changes.disabled && u.role === 'admin' && users.filter((x) => x.role === 'admin' && !x.disabled).length === 1) {
            throw Object.assign(new Error('There must be at least one active admin.'), { status: 400 });
        }
        u.disabled = changes.disabled;
    }
    let temporaryPassword = null;
    if (changes.resetPassword) {
        temporaryPassword = generateTemporaryPassword();
        u.passwordHash = hashPassword(temporaryPassword);
        u.mustChangePassword = true;
        delete u.resetTokenHash; delete u.resetExpiresAt; delete u.resetRequestedAt;
    }
    saveUsers(users);
    // A disabled account, or a reset password, ends that person's sessions; a new role or name applies at once
    if (u.disabled || changes.resetPassword) endSessionsFor(u.email);
    else sessions.forEach((s) => { if (s.user.email === u.email) { s.user.role = u.role; s.user.name = u.name; saveSessionsSoon(); } });
    return { user: publicAccount(u), temporaryPassword };
}

function checkPassword(email, password) {
    const e = normalizeEmail(email);
    const u = loadUsers().find((x) => x.email === e);
    // Same work whether or not the account exists, so timing does not reveal valid emails
    if (!u) { verifyPassword(String(password), hashPassword('timing-equalizer')); return null; }
    if (!verifyPassword(String(password), u.passwordHash)) return null;
    if (u.disabled) return { disabled: true };
    return u;
}

function recordSignIn(email) {
    const users = loadUsers();
    const u = users.find((x) => x.email === normalizeEmail(email));
    if (u) { u.lastSignInAt = new Date().toISOString(); saveUsers(users); }
}

function changePassword(email, currentPassword, newPassword) {
    const e = normalizeEmail(email);
    const users = loadUsers();
    const u = users.find((x) => x.email === e);
    if (!u || !verifyPassword(String(currentPassword || ''), u.passwordHash)) {
        throw Object.assign(new Error('Your current password is not correct.'), { status: 400 });
    }
    const pw = String(newPassword || '');
    assertNewPassword(pw, e);
    if (pw === currentPassword) throw Object.assign(new Error('Choose a password different from the current one.'), { status: 400 });
    u.passwordHash = hashPassword(pw);
    u.mustChangePassword = false;
    saveUsers(users);
    return publicAccount(u);
}

// ─── Sessions ───
function parseCookies(req) {
    const out = {};
    (req.headers.cookie || '').split(';').forEach((part) => {
        const i = part.indexOf('=');
        if (i > 0) out[part.slice(0, i).trim()] = decodeURIComponent(part.slice(i + 1).trim());
    });
    return out;
}

function cookieHeader(req, value, maxAgeSec) {
    // Only a real HTTPS connection: a Secure cookie sent over plain HTTP is dropped by the browser, which
    // signed people out on every reload when a proxy added X-Forwarded-Proto: https
    const secure = req.secure ? '; Secure' : '';
    return `${COOKIE_NAME}=${encodeURIComponent(value)}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${maxAgeSec}${secure}`;
}

function publicUser(s) {
    const u = s.user;
    return { id: u.email, email: u.email, key: u.email, name: u.name, role: u.role, isAdmin: u.role === 'admin', mustChangePassword: !!s.mustChangePassword };
}

function createSession(req, res, user) {
    const sid = crypto.randomBytes(32).toString('hex');
    const now = Date.now();
    const s = { user: { email: user.email, name: user.name, role: user.role }, mustChangePassword: !!user.mustChangePassword, createdAt: now, lastSeen: now };
    sessions.set(sidKey(sid), s);
    saveSessionsSoon();
    res.setHeader('Set-Cookie', cookieHeader(req, sid, Math.floor(SESSION_MAX_MS / 1000)));
    return s;
}

function getSession(req) {
    const sid = parseCookies(req)[COOKIE_NAME];
    if (!sid) return null;
    const key = sidKey(sid);
    const s = sessions.get(key);
    if (!s) return null;
    const now = Date.now();
    if (now - s.lastSeen > SESSION_IDLE_MS || now - s.createdAt > SESSION_MAX_MS) {
        sessions.delete(key);
        saveSessionsSoon();
        return null;
    }
    // lastSeen only matters to the minute for the idle limit: do not rewrite the file on every request
    if (now - s.lastSeen > 60 * 1000) saveSessionsSoon();
    s.lastSeen = now;
    return s;
}

function destroySession(req, res) {
    const sid = parseCookies(req)[COOKIE_NAME];
    if (sid) { sessions.delete(sidKey(sid)); saveSessionsSoon(); }
    res.setHeader('Set-Cookie', cookieHeader(req, '', 0));
}

function endSessionsFor(email, exceptSid) {
    const keep = exceptSid ? sidKey(exceptSid) : null;
    sessions.forEach((s, key) => { if (s.user.email === email && key !== keep) sessions.delete(key); });
    saveSessionsSoon();
}

/** After a password change: this session is cleared to continue, other sessions of the account end. */
function passwordChanged(req, user) {
    const sid = parseCookies(req)[COOKIE_NAME];
    const s = sid && sessions.get(sidKey(sid));
    if (s) { s.mustChangePassword = false; s.user.role = user.role; s.user.name = user.name; saveSessionsSoon(); }
    endSessionsFor(user.email, sid);
    return s;
}

setInterval(() => {
    const now = Date.now();
    sessions.forEach((s, key) => {
        if (now - s.lastSeen > SESSION_IDLE_MS || now - s.createdAt > SESSION_MAX_MS) { sessions.delete(key); saveSessionsSoon(); }
    });
}, 10 * 60 * 1000).unref();

const PUBLIC_API_PATHS = ['/health', '/auth/login', '/auth/me', '/auth/logout', '/auth/register', '/auth/options', '/auth/forgot', '/auth/reset'];
// Allowed while a temporary password still has to be changed
const PASSWORD_CHANGE_PATHS = ['/health', '/auth/me', '/auth/logout', '/auth/change-password'];

/**
 * Sets req.user for every /api request: a signed-in team member, or an API-key client (admin).
 * Mounted on '/api/', so req.path is relative to /api.
 */
function requireAuth(log) {
    return (req, res, next) => {
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
                req.user = { id: 'api-key', email: 'api-key', key: 'api-key', name: 'API key', role: 'admin', isAdmin: true, viaApiKey: true };
                return next();
            }
        }
        const s = getSession(req);
        if (s) {
            req.user = publicUser(s);
            if (s.mustChangePassword && !PASSWORD_CHANGE_PATHS.includes(req.path)) {
                return res.status(403).json({ error: 'Change your temporary password first.', code: 'PASSWORD_CHANGE_REQUIRED' });
            }
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

/** Whether the current user may see or act on something owned by ownerKey (null = created before accounts existed). */
function canAccess(req, ownerKey) {
    if (!req.user) return false;
    if (req.user.isAdmin) return true;
    return !!ownerKey && ownerKey === req.user.key;
}

/** AEM target and credentials for an upload: always from the request (they are entered with each upload). */
function resolveAemTarget(req) {
    const b = req.body || {};
    return { baseUrl: b.baseUrl, username: b.username, password: b.password };
}

loadSessions();

function sessionCount() {
    return sessions.size;
}

module.exports = {
    createSession, getSession, destroySession, flushSessions, SESSIONS_FILE, sessionCount, publicUser, requireAuth, requireAdmin, canAccess, resolveAemTarget,
    checkPassword, recordSignIn, changePassword, passwordChanged, listUsers, addUser, registerUser, updateUser, normalizeEmail,
    isAllowedDomain, domainHint, authOptions, requestPasswordReset, resetPasswordWithToken, MIN_PASSWORD_LENGTH, USERS_FILE,
};
