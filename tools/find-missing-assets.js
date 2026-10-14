#!/usr/bin/env node
/**
 * Lists the files of a local folder that are not yet in an AEM DAM folder, and can copy just those
 * files into a new folder (same structure) so the rest of an interrupted upload can be re-uploaded.
 *
 * Usage:
 *   node find-missing-assets.js --aem <author url> --dam <DAM folder the upload targeted>
 *        --local <the local folder that was uploaded> [--copy-to <empty folder>] [--out missing-files.txt]
 *        [--source browser|server] [--skip-top-folder true]
 *
 * --skip-top-folder true: the upload used "Upload the folder's contents, not the folder itself".
 * --source: "browser" (default) for uploads made from the web page, "server" for a server path or the CLI
 * (only browser uploads rename ".." and <>"|?* in paths).
 * Credentials come from AEM_USER (or AEM_USERNAME) / AEM_PASSWORD, or are asked for (the password is not echoed).
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
// Files and folders @adobe/aem-upload never uploads (src/utils.js isTempPath)
const TEMP_NAME_PATTERNS = [/^[.~]/i, /^TestFile/, /\.tmp$/i, /\.~tmp$/i, /^desktop\.ini/i, /^Thumbs\.db/i,
    /^Adobe Bridge Cache\.bc$/i, /^Adobe Bridge Cache\.bct$/i];
const isSkippedName = (name) => TEMP_NAME_PATTERNS.some((p) => p.test(name));
let rearrangeEnabled = true;

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
    if (!rearrangeEnabled) return segment;
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
        if (isSkippedName(entry.name)) continue;
        const full = path.join(dir, entry.name);
        // The upload follows symlinks, so count what they point to
        const kind = entry.isSymbolicLink() ? await fsp.stat(full).catch(() => null) : entry;
        if (!kind) continue;
        if (kind.isDirectory()) await walk(full, base, out);
        else if (kind.isFile()) out.push(path.relative(base, full).split(path.sep));
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
        // AEM may return fewer hits per page than asked for; stop on the reported total, not the page size
        const total = typeof body.total === 'number' ? body.total : null;
        if (hits.length === 0 || (total !== null ? offset + hits.length >= total : hits.length < PAGE_SIZE)) break;
        if (total !== null && hits.length < PAGE_SIZE) offset -= PAGE_SIZE - hits.length;
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
    const source = args.source || 'browser';
    if (!['browser', 'server'].includes(source)) throw new Error('--source must be "browser" or "server"');
    rearrangeEnabled = source === 'browser';
    if (!fs.statSync(localRoot).isDirectory()) throw new Error(`--local is not a folder: ${localRoot}`);

    const user = process.env.AEM_USER || process.env.AEM_USERNAME || await ask('AEM username: ', false);
    const password = process.env.AEM_PASSWORD || await ask('AEM password: ', true);
    const auth = 'Basic ' + Buffer.from(`${user}:${password}`).toString('base64');

    console.log(`Reading local folder ${localRoot} ...`);
    const localFiles = await walk(localRoot, localRoot, []);
    console.log(`  local files: ${localFiles.length}`);

    console.log(`Listing assets under ${damFolder} on ${aemUrl} ...`);
    const aemPaths = await listAemAssets(aemUrl, damFolder, auth);

    // The uploaded folder itself becomes a folder under the DAM target, except that a browser upload
    // whose folder has the same name as the DAM folder is uploaded straight into it (server.js unwrap)
    const damLastSegment = damFolder.split('/').pop();
    const skipTop = String(args['skip-top-folder'] || '').toLowerCase() === 'true';
    const unwrapped = skipTop || (rearrangeEnabled && rearrangeSegment(path.basename(localRoot)) === damLastSegment);
    const topFolder = unwrapped ? null : remoteFolderName(path.basename(localRoot));
    const remoteRoot = topFolder ? `${damFolder}/${topFolder}` : damFolder;
    const missing = [];
    const expected = new Set();
    for (const segments of localFiles) {
        const folders = segments.slice(0, -1).map(remoteFolderName);
        const remote = [remoteRoot, ...folders, remoteAssetName(segments[segments.length - 1])].join('/');
        expected.add(remote);
        if (!aemPaths.has(remote)) missing.push(segments.join('/'));
    }
    const notLocal = [...aemPaths].filter((p) => p.startsWith(`${remoteRoot}/`) && !expected.has(p)).length;

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
