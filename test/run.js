// Regression runner: mock AEM + the app on a throwaway data folder, then the API and browser suites.
//   node test/run.js                 all suites
//   node test/run.js team high       only these
//   node test/run.js --list          names
// Needs Python 3 with Playwright (pip install -r test/requirements.txt && python -m playwright install chromium).
const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

const HERE = __dirname;
const APP = 'http://127.0.0.1:3930';
const AEM = 'http://127.0.0.1:4620';
const PYTHON = process.env.PYTHON || (process.platform === 'win32' ? 'python' : 'python3');
const SUITE_TIMEOUT_MS = 10 * 60 * 1000;
const WORK = process.env.TEST_WORK_DIR || fs.mkdtempSync(path.join(os.tmpdir(), 'aem-upload-test-'));
const FIX = path.join(WORK, 'fixtures');
const LOGS = path.join(WORK, 'logs');

const ui = (file, ...args) => ({ cmd: PYTHON, args: [path.join(HERE, 'ui', file), ...args] });
const api = (file) => ({ cmd: process.execPath, args: [path.join(HERE, 'api', file)] });
const fix = (name) => path.join(FIX, name);
const SUITES = {
    unit: { cmd: process.execPath, args: [path.join(HERE, '..', 'tools', 'test-package-builder.js')] },
    backup: { cmd: process.execPath, args: [path.join(HERE, '..', 'tools', 'test-backup.js')] },
    high: ui('high_test.py', APP, AEM, fix('uifiles')),
    'high-7-8': ui('high_7_8_test.py', APP, AEM, fix('uifiles')),
    medium: ui('medium_test.py', APP, AEM, fix('medsmall'), fix('medbig')),
    low: ui('low_test.py', APP),
    'skip-top': ui('skip_top_test.py', APP, AEM),
    'two-users': ui('two_users_test.py', APP, AEM, fix('uifiles')),
    review: ui('review_test.py', APP, AEM, fix('revfiles')),
    progress: ui('progress_test.py', APP, AEM, fix('revfiles')),
    folders: ui('folder_cases_test.py', APP, AEM),
    signup: ui('signup_test.py', APP),
    package: ui('package_test.py', APP, AEM),
    'skip-unchanged': ui('skip_unchanged_test.py', APP, AEM),
    copy: ui('copy_test.py', APP, AEM),
    'keep-names': ui('keep_names_test.py', APP, AEM),
    resume: ui('resume_test.py', AEM),
    queue: ui('queue_test.py'),
    // last: they try wrong passwords on purpose, and the sign-in limit (10 failures a minute) would block the next suites
    team: api('team.js'),
    'signup-api': api('signup.js'),
};

function writeFixtures() {
    const make = (dir, files) => {
        fs.mkdirSync(fix(dir), { recursive: true });
        for (const [name, body] of files) fs.writeFileSync(path.join(fix(dir), name), body);
    };
    const pad = (n, w) => String(n).padStart(w, '0');
    const range = (from, to) => Array.from({ length: to - from + 1 }, (_, i) => from + i);
    make('uifiles', range(1, 400).map((i) => [`t${pad(i, 3)}.dita`, `<topic id="t${pad(i, 3)}"><title>t${pad(i, 3)}</title></topic>\n`]));
    make('medsmall', [...range(1, 400).map((i) => [`t${pad(i, 3)}.dita`, `<topic id="t${pad(i, 3)}"/>`]),
        ...range(1, 50).map((i) => [`x${pad(i, 2)}.xml`, '<x/>'])]);
    make('medbig', range(0, 5999).map((i) => [`b${pad(i, 5)}.dita`, '<t/>']));
    make('revfiles', [...range(1, 300).map((i) => [`t${pad(i, 3)}.dita`, '<t/>\n']),
        ...range(1, 5).map((i) => [`bad${i}.dita`, '<t/>\n']), ['Thumbs.db', 'x\n']]);
}

function seedUsers(file) {
    const hash = (pw) => {
        const salt = crypto.randomBytes(16);
        return `scrypt$${salt.toString('hex')}$${crypto.scryptSync(pw, salt, 64, { N: 16384, r: 8, p: 1 }).toString('hex')}`;
    };
    const now = new Date().toISOString();
    const user = (email, name, role, pw) => ({ email, name, role, passwordHash: hash(pw), mustChangePassword: false, disabled: false, createdAt: now, createdBy: 'seed' });
    fs.writeFileSync(file, JSON.stringify([
        user('admin@test.local', 'Admin User', 'admin', 'admin-pass-123'),
        user('qe@test.local', 'QE User', 'member', 'qe-pass-12345'),
    ], null, 2));
}

function appEnv(name, port) {
    const d = path.join(WORK, name);
    const tmp = path.join(d, 'tmp');
    fs.mkdirSync(tmp, { recursive: true });
    return {
        PORT: String(port), TMPDIR: tmp, TEMP: tmp, TMP: tmp,
        HISTORY_FILE: path.join(d, 'upload-history.json'), PACKAGES_DIR: path.join(d, 'packages'), BACKUP_DIR: path.join(d, 'backups'),
    };
}

function start(name, cmd, args, env) {
    const log = fs.openSync(path.join(LOGS, name + '.log'), 'a');
    return spawn(cmd, args, { env: { ...process.env, ...env }, stdio: ['ignore', log, log] });
}

async function waitFor(url) {
    for (let i = 0; i < 120; i++) {
        try {
            await fetch(url);
            return;
        } catch (_) {
            await new Promise((r) => setTimeout(r, 500));
        }
    }
    throw new Error(`${url} did not start`);
}

function runSuite(name, { cmd, args }) {
    return new Promise((resolve) => {
        const started = Date.now();
        const logFile = path.join(LOGS, `suite-${name}.log`);
        const out = fs.createWriteStream(logFile);
        const child = spawn(cmd, args, { env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONUNBUFFERED: '1' } });
        let text = '';
        const take = (d) => { text += d; out.write(d); };
        child.stdout.on('data', take);
        child.stderr.on('data', take);
        const timer = setTimeout(() => { take(`\nTIMEOUT after ${SUITE_TIMEOUT_MS / 60000} min\n`); child.kill('SIGKILL'); }, SUITE_TIMEOUT_MS);
        child.on('close', (code) => {
            clearTimeout(timer);
            out.end();
            const lines = text.split(/\r?\n/);
            const failed = lines.filter((l) => /^\[FAIL\]|^TIMEOUT|Traceback|Error:/.test(l));
            const summary = lines.filter((l) => /\d+\/\d+ passed/.test(l)).pop() || '';
            resolve({ name, ok: code === 0, secs: Math.round((Date.now() - started) / 1000), summary, failed, logFile, text });
        });
    });
}

async function main() {
    if (process.argv.includes('--list')) return console.log(Object.keys(SUITES).join('\n'));
    const wanted = process.argv.slice(2);
    const unknown = wanted.filter((n) => !SUITES[n]);
    if (unknown.length) throw new Error(`unknown suite(s): ${unknown.join(', ')}; see --list`);
    const names = wanted.length ? wanted : Object.keys(SUITES);

    fs.mkdirSync(LOGS, { recursive: true });
    writeFixtures();
    const usersFile = path.join(WORK, 'users.json');
    seedUsers(usersFile);
    process.env.TEST_WORK_DIR = WORK;
    process.env.USERS_FILE = usersFile;
    process.env.ALLOWED_EMAIL_DOMAINS = 'adobe.com,test.local';
    process.env.LOG_LEVEL = process.env.LOG_LEVEL || 'info';
    console.log(`work folder: ${WORK}`);

    const procs = [
        start('mock-aem', process.execPath, [path.join(HERE, 'mock-aem.js'), '4620']),
        start('app', process.execPath, [path.join(HERE, 'launch.js')], { ...appEnv('app', 3930), ADMIN_EMAILS: 'admin@test.local', COPY_TARGETS: AEM }),
    ];
    const stopAll = () => procs.forEach((p) => { try { p.kill('SIGKILL'); } catch (_) { /* gone */ } });
    process.on('exit', stopAll);
    process.on('SIGINT', () => process.exit(130));
    await waitFor(AEM + '/__completed');
    await waitFor(APP + '/api/health');

    const results = [];
    for (const name of names) {
        const r = await runSuite(name, SUITES[name]);
        results.push(r);
        console.log(`${r.ok ? 'PASS' : 'FAIL'}  ${name.padEnd(15)} ${String(r.secs).padStart(4)}s  ${r.summary}`);
        if (!r.ok) {
            r.failed.slice(0, 10).forEach((l) => console.log('      ' + l.slice(0, 300)));
            if (process.env.CI) console.log(`::group::${name} output\n${r.text}\n::endgroup::`);
        }
    }
    const bad = results.filter((r) => !r.ok);
    console.log(`\n${results.length - bad.length}/${results.length} suites passed; logs in ${LOGS}`);
    stopAll();
    process.exit(bad.length ? 1 : 0);
}

main().catch((e) => { console.error(e.message); process.exit(1); });
