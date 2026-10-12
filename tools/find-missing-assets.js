#!/usr/bin/env node
/**
 * Lists the files of a local folder that are not yet in an AEM DAM folder, and can copy just those
 * files into a new folder (same structure) so the rest of an interrupted upload can be re-uploaded.
 *
 * Usage:
 *   node find-missing-assets.js --aem <author url> --dam <DAM folder the upload targeted>
 *        --local <the local folder that was uploaded> [--copy-to <empty folder>] [--out missing-files.txt]
 *
 * Credentials come from AEM_USER / AEM_PASSWORD, or are asked for (the password is not echoed).
 * Requires Node.js 18+ (no npm install needed).
 */
const fs = require('fs');
const fsp = require('fs').promises;
const path = require('path');
const readline = require('readline');

// Same renaming the upload utility applies (server.js rearrange + @adobe/aem-upload node-name cleaning)
const REARRANGE_UNSAFE = /[<>"|?*]/g;
const INVALID_FOLDER_CHARACTERS = /[.%;#+?^{}\s"&]/g;
const INVALID_ASSET_CHARACTERS = /[#%{}?&]/g;
const INVALID_CHARACTERS = /[/:[\]|*\\]/g;
const PAGE_SIZE = 5000;

function parseArgs(argv) {
    const args = {};
    for (let i = 0; i < argv.length; i += 1) {
        const key = argv[i];
        if (!key.startsWith('--')) throw new Error(`Unexpected argument: ${key}`);
        args[key.slice(2)] = argv[i + 1];
        i += 1;
    }
    for (const required of ['aem', 'dam', 'local']) {
        if (!args[required]) throw new Error(`Missing --${required}. See the usage at the top of this file.`);
    }
    return args;
}

function rearrangeSegment(segment) {
    return segment.replace(/\.\./g, '_').replace(REARRANGE_UNSAFE, '_');
}

function remoteFolderName(segment) {
    return rearrangeSegment(segment).replace(INVALID_FOLDER_CHARACTERS, '-').replace(INVALID_CHARACTERS, '-');
}

function remoteAssetName(fileName) {
    const { name, ext } = path.parse(rearrangeSegment(fileName));
    return name.replace(INVALID_ASSET_CHARACTERS, '-').replace(INVALID_CHARACTERS, '-') + ext;
}

async function walk(dir, base, out) {
    const entries = await fsp.readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) await walk(full, base, out);
        else if (entry.isFile()) out.push(path.relative(base, full).split(path.sep));
    }
    return out;
}

function ask(question, hidden) {
    return new Promise((resolve) => {
        const rl = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: true });
        if (hidden) {
            rl._writeToOutput = (text) => { if (text.includes(question)) rl.output.write(text); };
        }
        rl.question(question, (answer) => {
            rl.close();
            if (hidden) process.stdout.write('\n');
            resolve(answer);
        });
    });
}

async function listAemAssets(aemUrl, damFolder, auth) {
    const paths = new Set();
    for (let offset = 0; ; offset += PAGE_SIZE) {
        const query = new URLSearchParams({
            path: damFolder, type: 'dam:Asset', 'p.limit': String(PAGE_SIZE), 'p.offset': String(offset),
            'p.hits': 'selective', 'p.properties': 'jcr:path', orderby: 'path', 'p.guessTotal': 'false',
        });
        const res = await fetch(`${aemUrl}/bin/querybuilder.json?${query}`, { headers: { Authorization: auth } });
        if (res.status === 401 || res.status === 403) throw new Error(`AEM rejected the username or password (HTTP ${res.status}).`);
        if (!res.ok) throw new Error(`AEM query failed with HTTP ${res.status}.`);
        const body = await res.json();
        const hits = body.hits || [];
        hits.forEach((hit) => paths.add(hit['jcr:path']));
        process.stdout.write(`\r  AEM assets listed: ${paths.size}`);
        if (hits.length < PAGE_SIZE) break;
    }
    process.stdout.write('\n');
    return paths;
}

async function main() {
    const args = parseArgs(process.argv.slice(2));
    const aemUrl = args.aem.replace(/\/+$/, '');
    const damFolder = '/' + args.dam.replace(/^\/+|\/+$/g, '');
    const localRoot = path.resolve(args.local);
    const outFile = path.resolve(args.out || 'missing-files.txt');
    if (!fs.statSync(localRoot).isDirectory()) throw new Error(`--local is not a folder: ${localRoot}`);

    const user = process.env.AEM_USER || await ask('AEM username: ', false);
    const password = process.env.AEM_PASSWORD || await ask('AEM password: ', true);
    const auth = 'Basic ' + Buffer.from(`${user}:${password}`).toString('base64');

    console.log(`Reading local folder ${localRoot} ...`);
    const localFiles = await walk(localRoot, localRoot, []);
    console.log(`  local files: ${localFiles.length}`);

    console.log(`Listing assets under ${damFolder} on ${aemUrl} ...`);
    const aemPaths = await listAemAssets(aemUrl, damFolder, auth);

    // The uploaded folder itself becomes a folder under the DAM target
    const topFolder = remoteFolderName(path.basename(localRoot));
    const missing = [];
    const expected = new Set();
    for (const segments of localFiles) {
        const folders = segments.slice(0, -1).map(remoteFolderName);
        const remote = [damFolder, topFolder, ...folders, remoteAssetName(segments[segments.length - 1])].join('/');
        expected.add(remote);
        if (!aemPaths.has(remote)) missing.push(segments.join('/'));
    }
    const notLocal = [...aemPaths].filter((p) => p.startsWith(`${damFolder}/${topFolder}/`) && !expected.has(p)).length;

    fs.writeFileSync(outFile, missing.join('\n') + (missing.length ? '\n' : ''));
    console.log('');
    console.log(`Local files:            ${localFiles.length}`);
    console.log(`Already in AEM:         ${localFiles.length - missing.length}`);
    console.log(`Missing from AEM:       ${missing.length}  -> ${outFile}`);
    if (notLocal > 0) console.log(`In AEM but not local:   ${notLocal}  (left alone)`);

    if (args['copy-to'] && missing.length > 0) {
        const copyRoot = path.join(path.resolve(args['copy-to']), path.basename(localRoot));
        if (fs.existsSync(copyRoot) && fs.readdirSync(copyRoot).length > 0) {
            throw new Error(`${copyRoot} already exists and is not empty; choose another --copy-to folder.`);
        }
        for (const rel of missing) {
            const dest = path.join(copyRoot, ...rel.split('/'));
            await fsp.mkdir(path.dirname(dest), { recursive: true });
            await fsp.copyFile(path.join(localRoot, ...rel.split('/')), dest);
        }
        console.log('');
        console.log(`Copied the ${missing.length} missing files to: ${copyRoot}`);
        console.log(`Upload that folder ("${path.basename(localRoot)}") to the same DAM path (${damFolder.replace(/^\//, '')}).`);
    }
}

main().catch((e) => {
    console.error(`Error: ${e.message}`);
    process.exit(1);
});
