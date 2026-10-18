// Team-account checks against the running preview (3930) and mock AEM (4620).
const BASE = 'http://127.0.0.1:3930';
const AEM = 'http://127.0.0.1:4620';
const results = [];
const record = (name, ok, detail) => { results.push(ok); console.log(`[${ok ? 'PASS' : 'FAIL'}] ${name}: ${detail}`); };
const H = { 'Content-Type': 'application/json', Origin: BASE };

async function login(email, password) {
    const r = await fetch(BASE + '/api/auth/login', { method: 'POST', headers: H, body: JSON.stringify({ email, password }) });
    return { status: r.status, body: await r.json().catch(() => ({})), cookie: (r.headers.get('set-cookie') || '').split(';')[0] };
}
const call = (path, cookie, method = 'GET', body) => fetch(BASE + path, { method, headers: { ...H, Cookie: cookie }, body: body ? JSON.stringify(body) : undefined });
const json = async (r) => ({ status: r.status, body: await r.json().catch(() => ({})) });

(async () => {
    const bad = await login('admin@test.local', 'nope');
    record('wrong password', bad.status === 401 && bad.body.error === 'Wrong email or password.', `${bad.status} ${bad.body.error}`);
    const unknown = await login('nobody@test.local', 'whatever-123');
    record('unknown email gives the same answer', unknown.status === 401 && unknown.body.error === bad.body.error, `${unknown.status} ${unknown.body.error}`);

    const admin = await login('ADMIN@test.local', 'admin-pass-123');
    record('admin signs in (email case-insensitive)', admin.status === 200 && admin.body.user.isAdmin && !('passwordHash' in admin.body.user), JSON.stringify(admin.body.user));

    const member = await login('qe@test.local', 'qe-pass-12345');
    const mAdmin = await json(await call('/api/admin/users', member.cookie));
    record('member cannot open Team members', mAdmin.status === 403, `status=${mAdmin.status}`);

    const email = `new${Date.now()}@test.local`;
    const add = await json(await call('/api/admin/users', admin.cookie, 'POST', { email, name: 'New Person', role: 'member' }));
    const temp = add.body.temporaryPassword;
    const list = await json(await call('/api/admin/users', admin.cookie));
    const stored = list.body.users.find((u) => u.email === email);
    record('admin adds a member with a temporary password', add.status === 201 && /^[A-Za-z2-9]{16}$/.test(temp || '') && stored && stored.mustChangePassword && !('passwordHash' in stored),
        `temp length=${(temp || '').length}, listed=${!!stored}`);
    const dup = await json(await call('/api/admin/users', admin.cookie, 'POST', { email: email.toUpperCase(), name: 'x' }));
    record('duplicate email refused', dup.status === 409, `${dup.status} ${dup.body.error}`);

    const first = await login(email, temp);
    const blocked = await json(await call('/api/upload/history', first.cookie));
    record('temporary password: everything blocked until changed', first.body.user.mustChangePassword && blocked.status === 403 && blocked.body.code === 'PASSWORD_CHANGE_REQUIRED', `history=${blocked.status} ${blocked.body.code}`);
    const short = await json(await call('/api/auth/change-password', first.cookie, 'POST', { currentPassword: temp, newPassword: '' }));
    const changed = await json(await call('/api/auth/change-password', first.cookie, 'POST', { currentPassword: temp, newPassword: 'my-own-password-1' }));
    const after = await json(await call('/api/upload/history', first.cookie));
    const oldTemp = await login(email, temp);
    record('change password: rules enforced, then access granted, temp no longer works',
        short.status === 400 && changed.status === 200 && after.status === 200 && oldTemp.status === 401, `short=${short.status}, change=${changed.status}, history=${after.status}, temp again=${oldTemp.status}`);

    const reset = await json(await call(`/api/admin/users/${encodeURIComponent(email)}`, admin.cookie, 'POST', { resetPassword: true }));
    const killed = await json(await call('/api/upload/history', first.cookie));
    record('reset password ends their sessions and issues a new temporary password', reset.status === 200 && !!reset.body.temporaryPassword && killed.status === 401, `reset=${reset.status}, old session=${killed.status}`);

    const dis = await json(await call(`/api/admin/users/${encodeURIComponent(email)}`, admin.cookie, 'POST', { disabled: true }));
    const disLogin = await login(email, reset.body.temporaryPassword);
    record('disabled account cannot sign in', dis.status === 200 && disLogin.status === 403 && disLogin.body.code === 'ACCOUNT_DISABLED', `${disLogin.status} ${disLogin.body.error}`);

    const selfDemote = await json(await call('/api/admin/users/admin%40test.local', admin.cookie, 'POST', { role: 'member' }));
    record('admin cannot demote or disable themselves', selfDemote.status === 400, `${selfDemote.status} ${selfDemote.body.error}`);

    const badAem = await json(await call('/api/upload/browser/init', member.cookie, 'POST', { baseUrl: AEM, username: 'admin', password: 'wrong', damPath: 'content/dam/T', totalBytes: 1, fileCount: 1 }));
    const goodAem = await json(await call('/api/upload/browser/init', member.cookie, 'POST', { baseUrl: AEM, username: 'admin', password: 'admin', damPath: 'content/dam/T', totalBytes: 1, fileCount: 1 }));
    record('AEM credentials are checked per upload', badAem.status === 400 && goodAem.status === 200, `wrong AEM password=${badAem.status} (${badAem.body.code}), right=${goodAem.status}`);
    if (goodAem.body.sessionId) await call('/api/upload/browser/cancel/' + goodAem.body.sessionId, member.cookie, 'POST');

    const xsite = await fetch(BASE + '/api/admin/users', { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: 'http://evil.example', Cookie: admin.cookie }, body: '{"email":"x@y.z"}' });
    record('cross-origin admin write rejected', xsite.status === 403, `status=${xsite.status}`);

    const proxied = await fetch(BASE + '/api/auth/login', { method: 'POST', headers: { ...H, 'X-Forwarded-Proto': 'https' }, body: JSON.stringify({ email: 'qe@test.local', password: 'qe-pass-12345' }) });
    const proxiedCookie = proxied.headers.get('set-cookie') || '';
    record('over plain HTTP the session cookie is never Secure (else the browser drops it and a reload signs out)', proxied.status === 200 && !/;\s*Secure/i.test(proxiedCookie), proxiedCookie.replace(/=[0-9a-f]{20,}/, '=…'));

    const promote = await json(await call('/api/admin/users/qe%40test.local', admin.cookie, 'POST', { role: 'admin' }));
    const meNow = await json(await call('/api/auth/me', member.cookie));
    await call('/api/admin/users/qe%40test.local', admin.cookie, 'POST', { role: 'member' });
    const meBack = await json(await call('/api/auth/me', member.cookie));
    record('a role change by an admin applies to the open session at once', promote.status === 200 && meNow.body.user && meNow.body.user.isAdmin && meBack.body.user && !meBack.body.user.isAdmin,
        `admin now=${meNow.body.user && meNow.body.user.isAdmin}, after revert=${meBack.body.user && meBack.body.user.isAdmin}`);

    const st = await json(await call('/api/admin/status', admin.cookie));
    const stMember = await call('/api/admin/status', member.cookie);
    record('server status for admins: disk, memory, backups, version; not for members', st.status === 200 && st.body.disk && st.body.disk.temp.free > 0
        && st.body.memory && st.body.backup && st.body.version && Array.isArray(st.body.warnings) && stMember.status === 403,
        `admin=${st.status} warnings=${JSON.stringify(st.body.warnings)} member=${stMember.status}`);

    const open = await json(await call('/api/upload/browser/init', member.cookie, 'POST', { baseUrl: AEM, username: 'admin', password: 'admin', damPath: 'content/dam/T', totalBytes: 1, fileCount: 1 }));
    const memberDrain = await call('/api/admin/drain', member.cookie, 'POST', { on: true });
    const drainOn = await json(await call('/api/admin/drain', admin.cookie, 'POST', { on: true }));
    try {
        const health = await json(await fetch(BASE + '/api/health'));
        const refused = await json(await call('/api/upload/browser/init', member.cookie, 'POST', { baseUrl: AEM, username: 'admin', password: 'admin', damPath: 'content/dam/T', totalBytes: 1, fileCount: 1 }));
        const pkgRefused = await call('/api/packages/init', member.cookie, 'POST', { damPath: 'content/dam/T', packageName: 'p', version: '1.0', totalBytes: 1, fileCount: 1 });
        const stDrain = await json(await call('/api/admin/status', admin.cookie));
        record('drain mode: only admins; new uploads and packages refused with a clear message; health and status show it',
            memberDrain.status === 403 && drainOn.status === 200 && drainOn.body.draining.by === 'admin@test.local' && health.body.draining === true
            && refused.status === 503 && refused.body.code === 'DRAINING' && /paused/.test(refused.body.error) && pkgRefused.status === 503 && stDrain.body.draining,
            `member=${memberDrain.status} on=${drainOn.status} health=${health.body.draining} init=${refused.status} ${refused.body.code} package=${pkgRefused.status}`);
        record('drain mode: an upload that already started counts as running (not safe to deploy yet)',
            open.status === 200 && stDrain.body.busy.receiving >= 1 && health.body.busy >= 1, JSON.stringify(stDrain.body.busy));
        if (open.body.sessionId) await call('/api/upload/browser/cancel/' + open.body.sessionId, member.cookie, 'POST');
    } finally {
        const off = await json(await call('/api/admin/drain', admin.cookie, 'POST', { on: false }));
        const again = await json(await call('/api/upload/browser/init', member.cookie, 'POST', { baseUrl: AEM, username: 'admin', password: 'admin', damPath: 'content/dam/T', totalBytes: 1, fileCount: 1 }));
        if (again.body.sessionId) await call('/api/upload/browser/cancel/' + again.body.sessionId, member.cookie, 'POST');
        record('resume uploads opens them again', off.status === 200 && off.body.draining === null && again.status === 200, `off=${off.status} init=${again.status}`);
    }

    const logMember = await call('/api/admin/copy-log', member.cookie);
    const logAdmin = await json(await call('/api/admin/copy-log', admin.cookie));
    const csv = await call('/api/admin/copy-log?format=csv', admin.cookie);
    const csvText = await csv.text();
    record('copy log: admins only; JSON and CSV download', logMember.status === 403 && logAdmin.status === 200 && Array.isArray(logAdmin.body.entries)
        && /text\/csv/.test(csv.headers.get('content-type')) && /attachment/.test(csv.headers.get('content-disposition')) && csvText.replace(/^﻿/, '').startsWith('at,event,copyId,by,'),
        `member=${logMember.status} admin=${logAdmin.status} csv=${csv.headers.get('content-type')}`);

    const out = await call('/api/auth/logout', member.cookie, 'POST');
    const gone = await call('/api/upload/history', member.cookie);
    record('sign-out ends the session', out.status === 200 && gone.status === 401, `after=${gone.status}`);

    console.log(`\n${results.filter(Boolean).length}/${results.length} passed`);
    process.exit(results.every(Boolean) ? 0 : 1);
})();
