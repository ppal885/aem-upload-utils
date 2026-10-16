// Self sign-up, Adobe-only emails, forgot/reset password. Runs against the preview on 3930
// (ALLOWED_EMAIL_DOMAINS=adobe.com,test.local) and, for the emailed-link path, against auth.js directly.
const fs = require('fs');
const os = require('os');
const path = require('path');

const BASE = 'http://127.0.0.1:3930';
const results = [];
const record = (name, ok, detail) => { results.push(ok); console.log(`[${ok ? 'PASS' : 'FAIL'}] ${name}${ok ? '' : ': ' + detail}`); };
const H = { 'Content-Type': 'application/json', Origin: BASE };
const post = async (p, body, cookie) => {
    const r = await fetch(BASE + p, { method: 'POST', headers: { ...H, ...(cookie ? { Cookie: cookie } : {}) }, body: JSON.stringify(body) });
    return { status: r.status, body: await r.json().catch(() => ({})), cookie: (r.headers.get('set-cookie') || '').split(';')[0] };
};
const get = async (p, cookie) => {
    const r = await fetch(BASE + p, { headers: cookie ? { Cookie: cookie } : {} });
    return { status: r.status, body: await r.json().catch(() => ({})) };
};

(async () => {
    const opts = await get('/api/auth/options');
    record('options tell the page which email domains are allowed', opts.status === 200 && opts.body.emailDomains.includes('adobe.com') && opts.body.signup === true, JSON.stringify(opts.body));

    const gmail = await post('/api/auth/register', { name: 'X', email: 'someone@gmail.com', password: 'long-enough-pw' });
    record('sign-up with a non-Adobe email is refused', gmail.status === 400 && /Adobe email/.test(gmail.body.error), JSON.stringify(gmail.body));

    const email = `qe.signup${Date.now()}@adobe.com`;
    const weak = await post('/api/auth/register', { name: 'QE Signup', email, password: '' });
    const same = await post('/api/auth/register', { name: 'QE Signup', email, password: email });
    record('empty password and password = email refused', weak.status === 400 && same.status === 400, `${weak.body.error} | ${same.body.error}`);

    const shortOne = await post('/api/auth/register', { name: 'Short', email: 'qe.short' + Date.now() + '@adobe.com', password: 'abc' });
    record('short passwords are allowed (no length rule)', shortOne.status === 201, JSON.stringify(shortOne.body));
    const ok = await post('/api/auth/register', { name: 'QE Signup', email: email.toUpperCase(), password: 'my-own-password-1' });
    const me = await get('/api/auth/me', ok.cookie);
    record('sign-up creates a member and signs them in', ok.status === 201 && me.status === 200 && me.body.user.email === email && !me.body.user.isAdmin
        && !me.body.user.mustChangePassword, JSON.stringify(ok.body));
    const hist = await get('/api/upload/history', ok.cookie);
    record('new account can use the app right away', hist.status === 200, `history=${hist.status}`);

    const dup = await post('/api/auth/register', { name: 'Again', email, password: 'another-password-2' });
    record('same email cannot sign up twice', dup.status === 409, JSON.stringify(dup.body));

    const gLogin = await post('/api/auth/login', { email: 'someone@gmail.com', password: 'whatever-123' });
    record('sign-in with a non-Adobe email is refused with a clear message', gLogin.status === 400 && /Adobe email/.test(gLogin.body.error), JSON.stringify(gLogin.body));
    const login = await post('/api/auth/login', { email, password: 'my-own-password-1' });
    record('the new account signs in with its own password', login.status === 200, JSON.stringify(login.body));

    const fGmail = await post('/api/auth/forgot', { email: 'x@gmail.com' });
    record('forgot password refuses non-Adobe emails', fGmail.status === 400 && /Adobe email/.test(fGmail.body.error), JSON.stringify(fGmail.body));
    const fUnknown = await post('/api/auth/forgot', { email: 'nobody.here@adobe.com' });
    const fReal = await post('/api/auth/forgot', { email });
    record('forgot password gives the same answer for unknown and real accounts', fUnknown.status === 200 && fReal.status === 200
        && fUnknown.body.message === fReal.body.message, `${fUnknown.body.message} | ${fReal.body.message}`);

    const admin = await post('/api/auth/login', { email: 'admin@test.local', password: 'admin-pass-123' });
    const team = await get('/api/admin/users', admin.cookie);
    const row = (team.body.users || []).find((u) => u.email === email);
    record('admin sees the reset request on Team members', !!(row && row.resetRequestedAt), JSON.stringify(row));
    const reset = await post(`/api/admin/users/${encodeURIComponent(email)}`, { resetPassword: true }, admin.cookie);
    const team2 = await get('/api/admin/users', admin.cookie);
    const row2 = (team2.body.users || []).find((u) => u.email === email);
    const temp = await post('/api/auth/login', { email, password: reset.body.temporaryPassword });
    record('admin reset gives a temporary password and clears the request', reset.status === 200 && !row2.resetRequestedAt
        && temp.status === 200 && temp.body.user.mustChangePassword, JSON.stringify(row2));

    // Emailed-link path, on a throwaway user file
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'gus-auth-'));
    process.env.USERS_FILE = path.join(dir, 'users.json');
    process.env.ALLOWED_EMAIL_DOMAINS = 'adobe.com';
    const auth = require(path.join(__dirname, '..', '..', 'auth.js'));
    const first = auth.registerUser({ name: 'First', email: 'first@adobe.com', password: 'first-password-1' });
    const second = auth.registerUser({ name: 'Second', email: 'second@adobe.com', password: 'second-password-1' });
    record('with no ADMIN_EMAILS the very first account becomes admin, later ones members', first.role === 'admin' && second.role === 'member', `${first.role}/${second.role}`);
    const req = auth.requestPasswordReset('second@adobe.com');
    const changed = auth.resetPasswordWithToken(req.token, 'brand-new-password-9');
    let reused = '';
    try { auth.resetPasswordWithToken(req.token, 'another-password-9'); } catch (e) { reused = e.message; }
    record('reset link sets a new password once, then stops working', changed.email === 'second@adobe.com'
        && !!auth.checkPassword('second@adobe.com', 'brand-new-password-9') && /expired or was already used/.test(reused), reused);
    const stored = fs.readFileSync(process.env.USERS_FILE, 'utf8');
    record('reset tokens and passwords are not stored in plain text', !stored.includes(req.token) && !stored.includes('brand-new-password-9'), '');
    fs.rmSync(dir, { recursive: true, force: true });

    console.log(`\n${results.filter(Boolean).length}/${results.length} passed`);
    process.exit(results.every(Boolean) ? 0 : 1);
})();
