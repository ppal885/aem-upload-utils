#!/usr/bin/env node
'use strict';

// Checks lib/backup.js against a throwaway folder: node tools/test-backup.js
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const work = fs.mkdtempSync(path.join(os.tmpdir(), 'gus-backup-'));
process.env.BACKUP_DIR = path.join(work, 'backups');
process.env.BACKUP_KEEP_DAYS = '14';
const { backupIfDue, BACKUP_DIR } = require('../lib/backup');

const quiet = { info() {}, warn() {}, error() {} };
const users = path.join(work, 'users.json');
const history = path.join(work, 'upload-history.json');
fs.writeFileSync(users, '[{"email":"a@adobe.com"}]');
fs.writeFileSync(history, '[]');

(async () => {
    let passed = 0;
    const test = async (name, fn) => { await fn(); passed++; console.log(`PASS  ${name}`); };

    for (let d = 1; d <= 20; d++) fs.mkdirSync(path.join(BACKUP_DIR, `2020-01-${String(d).padStart(2, '0')}`), { recursive: true });

    await test('first run copies the files that exist and skips missing ones', async () => {
        const dir = await backupIfDue({ 'users.json': users, 'upload-history.json': history, 'packages.json': path.join(work, 'none.json') }, quiet);
        assert.ok(dir);
        assert.equal(fs.readFileSync(path.join(dir, 'users.json'), 'utf8'), '[{"email":"a@adobe.com"}]');
        assert.ok(fs.existsSync(path.join(dir, 'upload-history.json')));
        assert.ok(!fs.existsSync(path.join(dir, 'packages.json')));
    });
    await test('second run on the same day does nothing', async () => {
        assert.equal(await backupIfDue({ 'users.json': users }, quiet), null);
    });
    await test('only the newest 14 days are kept', async () => {
        const days = fs.readdirSync(BACKUP_DIR).filter((n) => /^\d{4}-\d{2}-\d{2}$/.test(n)).sort();
        assert.equal(days.length, 14);
        assert.equal(days[days.length - 1], new Date().toISOString().slice(0, 10));
        assert.ok(!days.includes('2020-01-01'));
    });
    await test('no half-written folder is left behind', async () => {
        assert.ok(!fs.readdirSync(BACKUP_DIR).some((n) => n.endsWith('.partial')));
    });
    fs.rmSync(work, { recursive: true, force: true });
    console.log(`\n${passed} passed`);
})().catch((e) => { console.error('FAIL', e.message); process.exit(1); });
