#!/usr/bin/env node
/**
 * Manage team member accounts from the server's shell (e.g. to create the first admin).
 *
 *   node tools/manage-users.js list
 *   node tools/manage-users.js add --email jdoe@adobe.com --name "Jane Doe" [--admin]
 *   node tools/manage-users.js reset --email jdoe@adobe.com
 *   node tools/manage-users.js disable --email jdoe@adobe.com
 *   node tools/manage-users.js enable --email jdoe@adobe.com
 *   node tools/manage-users.js role --email jdoe@adobe.com --admin|--member
 *
 * "add" and "reset" print a temporary password once; the person must change it at first sign-in.
 * Uses the same users.json as the server (USERS_FILE overrides the location).
 */
const auth = require('../auth');

function parse(argv) {
    const out = { _: [] };
    for (let i = 0; i < argv.length; i += 1) {
        const a = argv[i];
        if (a === '--admin' || a === '--member') out.role = a.slice(2);
        else if (a.startsWith('--')) { out[a.slice(2)] = argv[i + 1]; i += 1; }
        else out._.push(a);
    }
    return out;
}

function showTemp(email, temporaryPassword) {
    console.log(`\nTemporary password for ${email}: ${temporaryPassword}`);
    console.log('Share it privately. It must be changed at first sign-in.\n');
}

try {
    const args = parse(process.argv.slice(2));
    const cmd = args._[0];
    if (cmd === 'list') {
        const users = auth.listUsers();
        if (!users.length) console.log('No team members yet. Create the first admin with: add --email ... --admin');
        users.forEach((u) => console.log(`${u.email.padEnd(36)} ${u.role.padEnd(7)} ${u.disabled ? 'DISABLED' : 'active  '} ${u.mustChangePassword ? 'temp-password' : ''}`));
    } else if (cmd === 'add') {
        const { user, temporaryPassword } = auth.addUser({ email: args.email, name: args.name, role: args.role || 'member', createdBy: 'manage-users.js' });
        console.log(`Added ${user.email} (${user.role})`);
        showTemp(user.email, temporaryPassword);
    } else if (cmd === 'reset') {
        const { user, temporaryPassword } = auth.updateUser(args.email, { resetPassword: true });
        showTemp(user.email, temporaryPassword);
    } else if (cmd === 'disable' || cmd === 'enable') {
        const { user } = auth.updateUser(args.email, { disabled: cmd === 'disable' });
        console.log(`${user.email} is now ${user.disabled ? 'disabled' : 'active'}`);
    } else if (cmd === 'role') {
        if (!args.role) throw new Error('Pass --admin or --member');
        const { user } = auth.updateUser(args.email, { role: args.role });
        console.log(`${user.email} is now ${user.role}`);
    } else {
        console.log('Usage: node tools/manage-users.js list | add --email E [--name N] [--admin] | reset --email E | disable --email E | enable --email E | role --email E --admin|--member');
        process.exit(cmd ? 1 : 0);
    }
    console.log(`(accounts file: ${auth.USERS_FILE})`);
    // auth.js starts a session-cleanup timer; it is unref'd, so the process exits normally
} catch (e) {
    console.error(`Error: ${e.message}`);
    process.exit(1);
}
