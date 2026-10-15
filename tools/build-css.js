#!/usr/bin/env node
'use strict';

// Builds public/app.css from the Tailwind classes used in public/, then stamps content hashes into
// index.html (app.css?v=…, aem-names.js?v=…) so browsers can cache those files for a year and still
// get a new version right after a deploy. Run after changing classes in public/: npm run build:css

const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const ROOT = path.join(__dirname, '..');
const PUBLIC = path.join(ROOT, 'public');
const INDEX = path.join(PUBLIC, 'index.html');

execFileSync(process.platform === 'win32' ? 'npx.cmd' : 'npx',
    ['--yes', 'tailwindcss@3.4.17', '-c', 'tailwind.config.js', '-i', 'src/tailwind.css', '-o', 'public/app.css', '--minify'],
    { cwd: ROOT, stdio: 'inherit', shell: process.platform === 'win32' });

const hashOf = (file) => crypto.createHash('sha256').update(fs.readFileSync(path.join(PUBLIC, file))).digest('hex').slice(0, 10);
let html = fs.readFileSync(INDEX, 'utf8');
for (const file of ['app.css', 'aem-names.js', 'favicon.svg']) {
    const re = new RegExp(`${file.replace('.', '\\.')}(\\?v=[0-9a-f]+)?"`, 'g');
    html = html.replace(re, `${file}?v=${hashOf(file)}"`);
}
fs.writeFileSync(INDEX, html);
console.log(`app.css ${(fs.statSync(path.join(PUBLIC, 'app.css')).size / 1024).toFixed(1)} KB; hashes stamped into index.html`);
