#!/usr/bin/env node
'use strict';

// Builds an AEM content package (.zip) from a local folder.
// Usage:
//   node tools/build-package.js --source <dir> --dam /content/dam/en --out pkg.zip
//        [--name x] [--group y] [--version 1.0] [--description d] [--created-by u] [--skip-top-folder true]

const path = require('node:path');
const { buildPackage } = require('../package-builder');

function parseArgs(argv) {
    const args = {};
    for (let i = 0; i < argv.length; i++) {
        const a = argv[i];
        if (!a.startsWith('--')) throw new Error(`unexpected argument: ${a}`);
        const eq = a.indexOf('=');
        if (eq > 0) {
            args[a.slice(2, eq)] = a.slice(eq + 1);
        } else {
            const next = argv[i + 1];
            if (next === undefined || next.startsWith('--')) args[a.slice(2)] = 'true';
            else { args[a.slice(2)] = next; i++; }
        }
    }
    return args;
}

function usage() {
    console.error('Usage: node tools/build-package.js --source <dir> --dam /content/dam/<path> --out <pkg.zip>\n'
        + '       [--name x] [--group y] [--version 1.0] [--description d] [--created-by u] [--skip-top-folder true]');
}

async function main() {
    let args;
    try { args = parseArgs(process.argv.slice(2)); } catch (e) { console.error(e.message); usage(); process.exit(2); }
    if (args.help || !args.source || !args.dam || !args.out) { usage(); process.exit(args.help ? 0 : 2); }

    const started = Date.now();
    let lastLine = 0;
    const result = await buildPackage({
        sourceDir: args.source,
        damRoot: args.dam,
        packageName: args.name,
        group: args.group,
        version: args.version,
        description: args.description,
        createdBy: args['created-by'],
        skipTopFolder: String(args['skip-top-folder'] || '').toLowerCase() === 'true',
        outFile: path.resolve(args.out),
        onProgress: ({ files, totalFiles, bytes }) => {
            if (Date.now() - lastLine < 1000) return;
            lastLine = Date.now();
            process.stderr.write(`  ${files}/${totalFiles} files, ${(bytes / 1048576).toFixed(1)} MB\n`);
        },
    });
    const secs = ((Date.now() - started) / 1000).toFixed(1);
    console.log(`Package written: ${result.outFile}`);
    console.log(`  files=${result.files} folders=${result.folders} assetBytes=${result.bytes} zipBytes=${result.zipBytes} `
        + `entries=${result.entries}${result.zip64 ? ' (zip64)' : ''} time=${secs}s`);
    console.log(`  filter: ${result.filterRoots.join(', ')}`);
    if (result.skipped.length) {
        console.log(`  skipped ${result.skipped.length}:`);
        for (const s of result.skipped.slice(0, 50)) console.log(`    ${s.path} - ${s.reason}`);
        if (result.skipped.length > 50) console.log(`    ... ${result.skipped.length - 50} more`);
    }
}

main().catch((e) => {
    console.error(`build-package failed: ${e.message}`);
    if (/"[A-Za-z]:[\\/][^"]*[\\/]content[\\/]dam/.test(String(e.message))) {
        console.error('  (Git Bash rewrote the /content/... argument into a Windows path; run with MSYS_NO_PATHCONV=1)');
    }
    process.exit(1);
});
