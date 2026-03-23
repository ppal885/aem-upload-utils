#!/usr/bin/env node
/**
 * CLI upload script — uses upload-service.js.
 *
 * Usage:
 *   node migrate-to-aem.js <baseUrl> <damPath> <localPath> [username] [password]
 *
 * Or set environment variables:
 *   AEM_URL, AEM_DAM_PATH, AEM_LOCAL_PATH, AEM_USERNAME, AEM_PASSWORD
 *
 * Example:
 *   node migrate-to-aem.js https://author-p1234-e5678.adobeaemcloud.com content/dam/my-folder C:\content admin admin
 */
const { runUpload, validateUploadConfig, scanDirectory } = require('./upload-service');

const args = process.argv.slice(2);

const baseUrl   = args[0] || process.env.AEM_URL;
const damPath   = args[1] || process.env.AEM_DAM_PATH;
const localPath = args[2] || process.env.AEM_LOCAL_PATH;
const username  = args[3] || process.env.AEM_USERNAME;
const password  = args[4] || process.env.AEM_PASSWORD;

if (!baseUrl || !damPath || !localPath || !username || !password) {
    console.error('Usage: node migrate-to-aem.js <baseUrl> <damPath> <localPath> [username] [password]');
    console.error('  or set AEM_URL, AEM_DAM_PATH, AEM_LOCAL_PATH, AEM_USERNAME, AEM_PASSWORD env vars');
    process.exit(1);
}

const config = {
    baseUrl,
    damPath,
    localPaths: [localPath],
    username,
    password,
    maxConcurrent: parseInt(process.env.AEM_MAX_CONCURRENT) || 20,
    maxUploadFiles: parseInt(process.env.AEM_MAX_FILES) || 50000,
};

const configErr = validateUploadConfig(config);
if (configErr) { console.error('Configuration error:', configErr); process.exit(1); }

(async () => {
    console.log('Scanning', localPath, '...');
    const scan = await scanDirectory(localPath);
    console.log(`Found ${scan.fileCount} files (${scan.folderCount} folders, ${(scan.totalSize / 1048576).toFixed(1)} MB)`);
    console.log(`Uploading to ${baseUrl}/${damPath}...\n`);

    const { promise, cancel } = runUpload(config, (event, data) => {
        if (event === 'filestart')  process.stdout.write(`  START  ${data.fileName} (${data.fileSize} bytes)\n`);
        if (event === 'fileend')    process.stdout.write(`  DONE   ${data.fileName}\n`);
        if (event === 'fileerror')  process.stdout.write(`  ERROR  ${data.fileName}: ${(data.errors && data.errors[0]) || 'unknown'}\n`);
    });

    // Allow Ctrl+C to request cancel
    process.on('SIGINT', () => { console.log('\nCancel requested...'); cancel(); });

    promise.then((result) => {
        console.log(`\nUpload finished: ${result.totalFiles} files, ${result.errors.length} error(s)`);
        if (result.cancelled) console.log('Upload was cancelled.');
        if (!result.success) {
            result.errors.forEach(e => console.error('  -', e));
            process.exit(1);
        }
    }).catch((err) => {
        console.error('Fatal error:', err);
        process.exit(1);
    });
})();
