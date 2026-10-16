/**
 * Daily copies of the data that exists only on this server: team accounts, upload history and the
 * package list. One folder per day in BACKUP_DIR (default ./backups), BACKUP_KEEP_DAYS (default 14) kept.
 * The files are written with write-then-rename, so a copy is always a complete file.
 */
const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');

const BACKUP_DIR = path.resolve(process.env.BACKUP_DIR || path.join(__dirname, '..', 'backups'));
const KEEP_DAYS = parseInt(process.env.BACKUP_KEEP_DAYS, 10) || 14;
const DAY_FOLDER = /^\d{4}-\d{2}-\d{2}$/;

function today() {
    return new Date().toISOString().slice(0, 10);
}

/** files: { name: absolutePath }. Returns the folder written, or null when today's backup exists. */
async function backupIfDue(files, log) {
    const dir = path.join(BACKUP_DIR, today());
    if (fs.existsSync(dir)) return null;
    const tmp = `${dir}.partial`;
    await fsp.rm(tmp, { recursive: true, force: true });
    await fsp.mkdir(tmp, { recursive: true, mode: 0o700 });
    const copied = [];
    for (const [name, file] of Object.entries(files)) {
        try {
            await fsp.copyFile(file, path.join(tmp, name));
            copied.push(name);
        } catch (e) {
            if (e.code !== 'ENOENT') log.warn(`Backup of ${name} failed: ${e.message}`);
        }
    }
    await fsp.rename(tmp, dir);
    await prune(log);
    log.info(`Backup written: ${dir} (${copied.join(', ') || 'nothing to copy'})`);
    return dir;
}

async function prune(log) {
    let entries = [];
    try { entries = await fsp.readdir(BACKUP_DIR); } catch (_) { return; }
    const days = entries.filter((n) => DAY_FOLDER.test(n)).sort();
    for (const old of days.slice(0, Math.max(0, days.length - KEEP_DAYS))) {
        await fsp.rm(path.join(BACKUP_DIR, old), { recursive: true, force: true });
        log.info(`Old backup removed: ${old}`);
    }
}

/** Backs up now if today's copy is missing, then checks every hour (so a new day gets its copy). */
function scheduleBackups(files, log) {
    const run = () => backupIfDue(files, log).catch((e) => log.error(`Backup failed: ${e.message}`));
    run();
    setInterval(run, 60 * 60 * 1000).unref();
}

module.exports = { BACKUP_DIR, KEEP_DAYS, backupIfDue, scheduleBackups };
