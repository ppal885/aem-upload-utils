# GUIdes Upload Utility

Bulk asset upload tool for Adobe Experience Manager, powered by `@adobe/aem-upload`.

## Features

- Web UI for configuring and monitoring uploads
- Support for AEM Cloud and On-Premise instances
- Real-time progress tracking with SSE (Server-Sent Events)
- Background upload support with browser notifications
- File system browser for selecting local folders
- Upload history with persistence
- Drag-and-drop column reordering in history
- Dry run mode to preview before uploading
- File-type filter for the selected files
- CLI script for headless uploads
- Optional API key authentication (API clients only; see Security Notes)
- Rate limiting and CORS support

## Requirements

- Node.js 18 or later
- npm

## Quick Start

```bash
# Install dependencies
npm install

# Start the server
npm start

# Open in browser
# http://localhost:3000
```

## Sign-in and users

Team members sign in with their **Adobe email** (`ALLOWED_EMAIL_DOMAINS`, default `adobe.com`) and a password
that belongs to this tool only (never their Adobe password).

- **Create account**: anyone with an allowed email creates their own account on the sign-in page.
  Emails in `ADMIN_EMAILS` become admins when they sign up; with no `ADMIN_EMAILS`, the very first account is the admin.
  Set `SIGNUP_ENABLED=false` to allow only admin-created accounts.
- **Forgot password**: asks for the Adobe email. With SMTP configured (`SMTP_HOST`, ...) the person gets a reset link
  (valid 1 hour). Without SMTP the request shows up on **Team members** as "Asked for password reset"; the admin
  clicks *Reset password* and passes on the temporary password, which must be changed at the next sign-in.
- Admins manage people on **Team members** (add, reset password, make admin, disable) or on the server with
  `node tools/manage-users.js list | add --email ... --name ... [--admin] | reset | disable | enable | role --admin|--member`.
- Passwords are stored only as scrypt hashes in `users.json` (git-ignored, back it up). Sessions: 8 h idle, 24 h maximum;
  they survive a restart or deploy (`sessions.json`, or `SESSIONS_FILE`: only a SHA-256 of each session id, readable by the server user only).
  An admin's role change applies to open sessions at once; disabling an account or resetting its password ends them.
- AEM credentials are entered for each upload and checked against AEM before any file is sent. The AEM password is
  never stored; the AEM server URL and username are remembered per person in the browser.
- Members see only their own uploads, history and packages; admins see everyone's. History keeps the last
  200 uploads per person.
- Scripts can still use `API_KEY` (`X-API-Key` header) and pass AEM credentials in the request body.

## Configuration

Settings are read from environment variables. The server and the CLI do **not** read a `.env` file by
themselves: set the variables in the shell, in `ecosystem.config.cjs` (`env`) for PM2, or through
`EnvironmentFile=` for systemd. `.env.example` lists the available names.

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | Server port |
| `API_KEY` | _(empty)_ | API key for authentication. Leave empty to disable (dev mode). |
| `CORS_ORIGIN` | `*` | Allowed CORS origins |
| `LOG_LEVEL` | `info` | Logging verbosity: `error`, `warn`, `info`, `debug` |
| `BROWSE_ROOTS` | _(empty)_ | Restrict file browsing to these directories (comma-separated) |
| `AEM_ALLOW_SELF_SIGNED` | `0` | Set to `1` to allow self-signed certs for `https://localhost:4502` (dev only) |
| `ALLOWED_EMAIL_DOMAINS` | `adobe.com` | Email domains that may sign up and sign in (comma-separated) |
| `ADMIN_EMAILS` | _(empty)_ | Emails that become admins when they create their account |
| `SIGNUP_ENABLED` | `true` | `false` = only admins create accounts |
| `MIN_PASSWORD_LENGTH` | `1` | Minimum password length (no rule by default; a password cannot be empty or the email) |
| `USERS_FILE` | `<app>/users.json` | Team accounts (scrypt password hashes) |
| `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `SMTP_SECURE`, `MAIL_FROM` | _(empty)_ | Mail server for password-reset links; without it admins reset passwords |
| `PUBLIC_URL` | _(request host)_ | Address used in reset links, e.g. `https://guides-upload.example.com` |
| `PACKAGES_DIR` / `PACKAGE_KEEP_DAYS` | `<app>/packages` / `7` | Where built content packages are kept, and for how long |
| `MAX_HISTORY_PER_USER` / `MAX_HISTORY` | `200` / `5000` | History rows kept per person / in total |
| `SESSION_IDLE_HOURS` / `SESSION_MAX_HOURS` | `8` / `24` | Sign-in session lifetime |
| `UPLOAD_PIPELINES` | `8` | Parallel upload pipelines (1-16). Each runs `maxConcurrent` (default 4) transfers. |
| `UPLOAD_TMPDIR` | `<app>/.upload-tmp` | Staging folder for browser uploads when started with `pm2 start ecosystem.config.cjs` |

### Content packages

**New upload -> Build package** turns the selected folder into a CRX content package (`.zip`, FileVault format)
for the chosen DAM folder, using the same folder skipping and name rules as an upload. AEM sign-in is not needed
to build. The **Packages** page lists your packages for download or **Install on AEM** (CRX Package Manager,
`force` + `install`). The filter is `mode="update"` on the packaged folder only: installing adds or overwrites
assets and never deletes anything else. Folders are `sling:OrderedFolder` with the original name as title.

Verified on AEM as a Cloud Service: the package installs, but Cloud does **not** run asset processing for
content that arrives in a package (no renditions, no Guides post-processing). After installing on Cloud, run
*Reprocess Assets* on the folder, or use Upload, which processes every file. CLI: `node tools/build-package.js`.

### Housekeeping

- **Backups:** once a day the server copies `users.json`, `upload-history.json` and the package list into
  `BACKUP_DIR/<date>/` (default `<app>/backups`, git-ignored) and keeps `BACKUP_KEEP_DAYS` (default 14) days.
  To restore, stop the app and copy the files back. The copies are on the same disk; copy the folder elsewhere
  as well if the VM itself could be lost.

- Staging folders are removed after every upload; abandoned sessions are reaped, and orphaned staging folders
  older than a day are swept every hour (and at start-up).
- An upload or package build that would not fit on the staging disk is refused up front.
- Packages expire after `PACKAGE_KEEP_DAYS`. PM2 logs rotate with `pm2-logrotate` (50 MB, 5 files, compressed);
  the upload library's per-file debug output is only logged with `LOG_LEVEL=debug`.

### HTTPS

The server speaks HTTPS when `TLS_CERT_FILE` and `TLS_KEY_FILE` (PEM) are set; it listens on `HTTPS_PORT`
(default 3443; use 443 for a plain `https://host` address). Set `HTTPS_PORT` equal to `PORT` when the network
lets only one port through (the VM 10.42.75.146 only allows 3000): the server then tells TLS from plain HTTP on
the first byte of each connection and serves both on that port. With `HTTPS_ONLY=true` the normal `PORT` only
redirects to HTTPS (except `/api/health`), and the session cookie is marked `Secure`.

Without a DNS name and a certificate from IT, `tools/make-cert.sh <ip> [host names]` creates a private CA and a
server certificate in `certs/` (git-ignored). Browsers show a warning until the CA is trusted; it can be
downloaded from `/guides-upload-ca.crt` (Windows: open it → Install Certificate → Local Machine →
"Trusted Root Certification Authorities"). A certificate from IT later just replaces `certs/server-chain.crt`
and `certs/server.key`.

### Page performance

The page loads no frameworks from CDNs: buttons and toasts are plain HTML/CSS, and Tailwind is built ahead of
time into `public/app.css` (`npm run build:css` after changing classes in `public/`; it also stamps content
hashes into `index.html`). Hashed files are cached for a year; `index.html` is revalidated on every visit.
Measured on a simulated VPN link (150 ms, 10 Mbit/s): 702 requests / 806 KB / 3.1 s fully loaded before,
9 requests / 100 KB / 1.7 s after; repeat visits transfer about 2 KB.

### Skip files already in AEM

**Skip files already in AEM** (summary panel) uploads only new or changed files. Before the transfer one
QueryBuilder search lists the assets under the DAM folder with `dam:sha1` (computed by AEM asset processing);
a staged file is skipped when its AEM path and SHA-1 match. Assets AEM has not processed yet (no `dam:sha1`,
e.g. uploaded minutes ago or installed from a package) are sent again, so a changed file is never skipped. If
the search fails the upload sends everything and says so. History shows "+N skipped". Verified on an AEM Cloud
stage author: re-uploading 50 files with 2 changed sent 2 and skipped 48.

### DTD and DITA-OT folders: keep names exactly

AEM does not allow some characters in folder names, so an upload normally cleans them (`wd-1.3` becomes `wd-1-3`, spaces become `-`). For a DTD or DITA-OT folder that breaks the paths in `catalog.xml`, and AEM Guides cannot find the DTDs. Turn on **Keep folder and file names exactly** (Summary panel, for upload and package): only the characters JCR can never store (`/ : [ ] | * \`) are replaced.

When the selection contains a DTD catalog (`*catalog*.xml` in the OASIS catalog namespace), the page reads it and warns before uploading: references (`uri`, `nextCatalog`, `rewritePrefix`, …) whose folder or file name would change, with a button to keep names exactly, and referenced files missing from the selection. After the upload, add the uploaded `catalog.xml` to a DITA profile (Tools > Guides > DITA Profiles) whose paths cover the content, save it, and only then upload the topics.

### Upload speed for many small files

AEM confirms every uploaded file with its own request, and each upload pipeline confirms one file at a time, so for small files the speed is about pipelines divided by AEM's answer time (measured on AEM as a Cloud Service: about 6 files/s with 8 pipelines). Uploads start with 8 pipelines (`UPLOAD_PIPELINES`) and, while an upload is the only one to its AEM server, add 4 every 30 seconds as long as that makes it faster, up to 24 (`UPLOAD_MAX_PIPELINES`); when more no longer helps they go back, on failed files they halve, and when a second upload to the same server starts they return to 8. Changes are logged ("Pipelines 8 -> 12") and shown on Server status. `UPLOAD_ADAPTIVE=0` turns this off.

For thousands of small files the confirm dialog suggests **Build package** with an estimate: one package is copied and installed in one go (filter mode update, so files already in the folder stay).

### Ready in AEM? and Server status

**Ready in AEM?** appears under every finished upload: the same check as Check in AEM for the uploaded folder (AEM Guides processed / failed, links, DTD), refreshed every 30 seconds until Guides is done (up to an hour). It uses the AEM sign-in from New upload; for another server it offers Check in AEM.

**Server status** (admins, sidebar) shows free disk for uploads and for the app, memory, running and waiting uploads per AEM server, signed-in sessions, the last backup, packages and the version, with warnings for low disk (under 10 GB or 10 %), no backup in the last day or high memory. "No uploads running" there means a deploy now interrupts nobody. Data: GET `/api/admin/status`.

### Copy log and pausing uploads for a deploy

**Copy log** (admins, sidebar) lists every Copy from AEM: who copied which folder from which server (and as which AEM user), how it ended, and where the content went: uploaded to an allowed server, built into a package, package installed or downloaded. **Download CSV** exports it. Passwords and file contents are never logged. The log is `copy-log.jsonl` next to the app (`COPY_LOG_FILE`), readable by the server user only, included in the daily backup; above 10 MB it moves to `copy-log.jsonl.1`.

**Pause new uploads** (Server status, "Before a deploy") refuses new uploads, copies, packages and installs with a short note for everyone, and lets the running ones finish. When it says **Safe to deploy now**, deploy; the restart opens uploads again (the pause is not saved). `/api/health` shows `draining` and `busy` (work a restart would break).

### Check in AEM

**Check in AEM** (History rows, and Packages after an install) shows how far AEM and AEM Guides got with a folder: DITA files processed / waiting / failed by Guides (failed ones by name), AEM processing (renditions, SHA-1), whether the links in up to 100 processed maps point at files in the folder, and whether the editor can load the DTD of a few topics. It refreshes every 30 seconds until Guides is done. On AEM as a Cloud Service, files installed from a package have no AEM processing: the dialog offers **Reprocess Assets** (Full Process + Run Post Process, as in Assets). Only reads, except Reprocess; the credentials are the ones on New upload for the same server, or typed in the dialog, and are not stored.

### Create a DITA profile

**Create DITA profile** (in the "DTD catalog found" box after uploading a DTD folder, and in a not-ready Copy from AEM result) creates a new profile on the AEM server from its Global/Default profile, like Tools > Guides > DITA Profiles > Create Profile: the topics folder in Assigned Paths, the uploaded `catalog.xml` added to the built-in catalogs, optionally a customer DITA-OT zip. **Check** first shows what changes (which profile a folder uses now, or that it moves out of another profile) and refuses a duplicate name or a catalog / zip that is not on the server yet; **Create profile** saves it and reads it back to confirm there are no errors. It never edits or deletes an existing profile, works only on the `COPY_TARGETS` servers, and every change is logged. After creating, a Copy from AEM result runs **Check again** by itself.

### Allowed copy targets

Team page, admins: the servers Copy from AEM may send customer content to (upload, package install, Create DITA profile). Add a server (https only) or remove one; a removed server is refused at once, also for copies already made and waiting for their upload. Each change is in the Copy log (who, added, removed). The list is saved in `copy-targets.json` (readable by the server user only, in the daily backup) and replaces `COPY_TARGETS` until **Reset to the server default**.

### Copy these too

After a copy, the result lists the folders outside it that its content needs: the folder of each DTD catalog and of the DITA-OT zip in the source DITA profile, and the folders of files the content links to. Each says whether the target already has it; missing ones are ticked. **Copy these too** (enter the source password again, it is never stored) copies the ticked folders from the same source to the **same path** on the target and uploads them (skipping files already there unchanged), then checks the setup again. Upload the topics after that. Each such copy is in the Copy log with the copy it belongs to.

History has **Download CSV**: when, who, AEM server, folder, source, result, files, skipped, errors, size, duration, first error (your own uploads; admins get everyone's).

### DTD catalogs and DITA-OT zips: what AEM Guides needs

Checked in the Guides code and on AEM as a Cloud Service; the tool checks these on the selection and before creating a DITA profile:

- **DITA-OT zip in DAM**: Guides reads the zip from `<path>/jcr:content/jcr:data`, which a DAM asset does not have, so the asset path gives "Failed to extract DITA-OT … jcr:data not found" (AEM still answers 200; only the alert icon in DITA Profiles shows it). Use `<zip>/jcr:content/renditions/original` as **AEM DITA-OT Zip Path**; **Create DITA profile** sets it for you.
- **The zip needs a top folder named exactly `DITA-OT`** (with `DITA-OT/plugins/org.dita.base/catalog-dita.xml` for Integrate Catalogs). With `dita-ot-3.7.4/` or files at the root the profile saves without an error and publishing fails later. The tool reads only the zip's table of contents (in the browser for a selected zip, with an HTTP range request in AEM) and blocks such a zip.
- **Catalogs**: point at the catalog **file**; Guides copies that file's folder (with subfolders) and resolves the catalog there, so a reference like `uri="../shared/x.dtd"` that leaves the folder is not found unless that folder has its own catalog in the same profile, and two catalogs in the same or nested folders collide (one is dropped). Folder and file names must match the catalog exactly: use **Keep folder and file names exactly** for DTD folders.

### Copy from AEM

**Copy from AEM** copies a folder from another AEM Guides server (for example a customer's stage) to one of our servers, by upload or as a package. The source is only read: one QueryBuilder search, then the original renditions (8 at a time by default; Gentle 4 or Fast 16). Nothing is created, changed or locked there, and its password is used only during the copy.

- **Links keep working.** On UUID-based Guides servers files point at each other by GUID (`href="GUID-….dita"`), and GUIDs are given out per server. The copy turns every GUID link back into a relative path; the target's Guides converts those to its own GUIDs. Links to files outside the folder are resolved on the source and reported; GUIDs that no file has (already broken on the source) are reported and left as they are.
- **Names are kept exactly** (a folder `v1.2` stays `v1.2`), and the target folder defaults to the same path as on the source: Guides DITA profiles point at absolute paths.
- **Every file is checked**: against AEM's `dam:sha1` when it matches, otherwise against the size AEM sent (Guides rewrites DITA files after AEM computed the SHA-1, so an older fingerprint is normal there).
- **Setup check before uploading.** Guides finds DTDs and DITA-OT through the DITA profile (Tools > Guides > DITA Profiles) that covers the folder. The tool reads the source profile and its DTD catalogs, lists the DOCTYPEs the files use, and checks that the target's profile for the target folder resolves them. It says what to copy too (a DTD folder or DITA-OT zip outside the folder, linked folders) and what to set up. Topics uploaded before their DTDs and profile are in place do not open in the editor, so the Upload button turns into **Upload anyway** until the target is ready; **Check again** re-reads the target. Output presets stored on maps are not copied.

Rules for copied content (often a customer's):

| Variable | Default | Effect |
|---|---|---|
| `COPY_TARGETS` | (none: copying is off) | Comma-separated server URLs copied content may go to, by upload or package install. The default list: once an admin saves a list on the Team page (**Allowed copy targets**, stored in `copy-targets.json` / `COPY_TARGETS_FILE`), that list applies until **Reset to the server default**. |
| `COPY_IDLE_MINUTES` | `60` | Copied files nobody uploads or packages are removed after this. |
| `COPY_PACKAGE_KEEP_HOURS` | `24` | Packages built from copied content are removed after this. |
| `COPY_ALLOW_DOWNLOAD` | off | Packages of copied content can only be installed on a `COPY_TARGETS` server, not downloaded, unless set to `true`. |

Every copy is recorded in History ("Copied from host/folder").

### Fair queue per AEM server

At most `MAX_UPLOADS_PER_SERVER` (default 3) uploads run against the same AEM host at a time; each already
sends 32 files at once (8 pipelines x 4). Further uploads to that host wait in line: the progress panel shows
*Waiting for a free slot on <host>: #2 in line*, the background banner shows the place too, and the upload
starts by itself when a slot frees up. Waiting uploads send nothing to AEM; Cancel just leaves the line.
Uploads to other AEM servers are not held up. Resumed uploads join the same line.

### Resume after a restart

Every running upload keeps a `job.json` (AEM server, DAM folder, AEM user, options; never the AEM password) in
its staging folder and deletes each staged file as soon as AEM confirms it. If the server stops, crashes or is
redeployed, the staging folder therefore holds exactly the unfinished files. After the restart the person who
started it (and admins) see **Interrupted by a server restart** on New upload with *N of M files reached AEM*;
**Resume** asks only for the AEM password and uploads the rest from the server, nothing is sent again from the
browser. **Discard** deletes the leftover files. Interrupted uploads are kept 3 days.

Tested by hard-killing the server at 909 of 3,000 files: after Resume all 3,000 were in AEM, 8 sent twice
(those in flight at the kill).

### Deploying a new version with PM2

Prefer a moment with nothing uploading (`/api/health` shows `"activeUploads":0`). A restart during an upload
is no longer fatal (see Resume after a restart), but the person then has to click Resume.

```bash
git pull && pm2 delete aem-upload-utils && pm2 start ecosystem.config.cjs && pm2 save
```

`pm2 restart` keeps the old process settings, so changes to `ecosystem.config.cjs` (staging folder, fork mode)
only take effect with `delete` + `start`. An upload running during the restart is recorded as interrupted and can be resumed.

## CLI Usage

```bash
node migrate-to-aem.js <baseUrl> <damPath> <localPath> <username> <password>
```

Example:

```bash
node migrate-to-aem.js https://author-p1234-e5678.adobeaemcloud.com content/dam/my-folder C:\content admin admin
```

Or set the environment variables `AEM_URL`, `AEM_DAM_PATH`, `AEM_LOCAL_PATH`, `AEM_USERNAME`, `AEM_PASSWORD`
(optional: `AEM_MAX_CONCURRENT`, default 2, and `UPLOAD_PIPELINES`, default 4).

### Resuming an interrupted upload

`tools/find-missing-assets.js` lists the files of a local folder that are not yet in the DAM folder and can copy
only those into a new folder to upload:

```bash
node tools/find-missing-assets.js --aem <author url> --dam /content/dam/<folder> --local <uploaded folder> --copy-to <empty folder>
```

Add `--source server` when the original upload used a server path or the CLI instead of the web page.

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/health` | Health check |
| `GET` | `/api/targets` | List preset targets |
| `GET` | `/api/scan?localPath=...` | Scan local directory |
| `POST` | `/api/upload` | Start upload |
| `POST` | `/api/upload/cancel/:id` | Cancel upload |
| `GET` | `/api/upload/stream/:id` | SSE progress stream |
| `GET` | `/api/upload/active` | List active uploads |
| `GET` | `/api/upload/status/:id` | Single upload status |
| `GET` | `/api/upload/history` | Upload history |
| `DELETE` | `/api/upload/history/:uploadId` | Delete history entry |
| `DELETE` | `/api/upload/history` | Clear all history |
| `GET` | `/api/browse?path=...` | Browse filesystem |
| `GET` | `/api/validate-path?localPath=...` | Validate path |
| `POST` | `/api/open-path` | Open path in OS file explorer |
| `POST` | `/api/auth/register`, `/api/auth/login`, `/api/auth/logout` | Create account, sign in, sign out |
| `POST` | `/api/auth/forgot`, `/api/auth/reset` | Forgot password, reset with emailed link |
| `GET`/`POST` | `/api/admin/users`, `/api/admin/users/:email` | Team members (admins) |
| `POST` | `/api/packages/init`, `/api/packages/build/:sessionId` | Build a content package from sent files |
| `GET` | `/api/packages`, `/api/packages/:id`, `/api/packages/:id/download` | List, status, download |
| `POST` | `/api/packages/:id/install` | Install on AEM via CRX Package Manager |

## Asset Upload Tips

- **Local AEM (https://localhost:4502)**: If your local AEM uses HTTPS with a self-signed cert, set `AEM_ALLOW_SELF_SIGNED=1` in `.env` and restart the server. Run the app locally (`npm start`) so both the app and AEM are on the same machine.
- **App on VM, AEM on your machine**: Use your machine's IP (e.g. `http://192.168.1.100:4502`) instead of `localhost`, since `localhost` from the VM refers to the VM itself.
- **Supported formats**: AEM accepts most file types (images, PDFs, DITA/XML, videos, etc.). The tool does not restrict file types; AEM handles validation.
- **Folder structure**: The full directory hierarchy from your local folder is preserved in AEM DAM. If you upload `my-folder/images/photo.jpg` to `content/dam`, it creates `content/dam/my-folder/images/photo.jpg`.
- **Speed**: Files are uploaded by `UPLOAD_PIPELINES` (default 8) parallel pipelines of 4 transfers (32 at a time). Measured on an AEM Cloud stage author from the VM, 2,000 small DITA files: 4x2 = 5.3 files/s, 8x4 = 9.1 files/s, 16x4 = 12.6 files/s, no errors. AEM slows down per request as parallelism grows, and the team shares the same servers, so 8x4 is the default; *Advanced options* can raise it.
- **Folder levels**: *Start the upload from* lets you skip any number of the selected folder's parent levels, e.g. upload `c/f1/f2` as just `f2` into an existing DAM folder. A folder named like the DAM folder's last part is not doubled (`en` into `.../en`).
- **File/folder names**: Invalid characters (`#%{}?&` and `/:[]|*\` in file names; also `.%;#+?^{}"&` and spaces in folder names) become `-`, like the AEM Assets UI. Folders keep their original name as title. The confirm dialog lists every renamed name, names that would collide after renaming, and names over 255 bytes. Unicode names are normalized (NFC).
- **DAM folder**: `/content/dam/x/`, `content\dam\x` and a pasted Assets URL are all accepted; missing parent folders are created. `#`, `?`, `%` and `..` are refused.
- **Error handling**: If individual files fail, the upload continues. Failed files are collected and available via the "Download Error Report" button (CSV with file name, error message, and error code).
- **Large uploads (up to 50K files)**: Files are chunked (1000 per batch) and sent in parallel to the server, then uploaded to AEM. Session timeouts are set to 8 hours to accommodate very long uploads at low concurrency. Browser uploads are staged on the server first: with `pm2 start ecosystem.config.cjs` in `<app>/.upload-tmp`, otherwise in the system temp folder (often a small `/tmp`). The server refuses an upload that does not fit and says so.
- **Skipped files**: Only system files are left out: names starting with `.` (incl. `.DS_Store`, `.git`), Office lock files (`~$...`), `Thumbs.db`, `ehthumbs.db`, `desktop.ini`, `__MACOSX` and Adobe Bridge caches. (`@adobe/aem-upload` on its own would also drop real content named `TestFile*`, `*.tmp` or `~...`; this tool uploads those.)

### Known Limitations

| Feature | Status |
|---|---|
| Skip existing assets | Supported for unchanged files (see Skip files already in AEM). |
| Metadata mapping (title, tags, XMP) | Not supported. AEM auto-extracts basic metadata (dimensions, MIME type). |
| Duplicate detection | Optional: **Skip files already in AEM** leaves out files whose path and content (SHA-1) match an asset already in AEM. Without it, existing assets are replaced. |
| DAM workflow triggers | Not supported. AEM runs its default DAM Update Asset workflow. |
| Asset processing profiles | Not configurable. Uses AEM server defaults. |

## Tests

`node test/run.js` starts a mock AEM (port 4620) and the app (port 3930) on a throwaway data folder, then runs every suite: package builder and backups, the account API, and the browser suites (Playwright for Python) for uploads, review, progress, folders, sign-up, packages, skip-unchanged, resume after a hard kill and the per-server queue. Name suites to run only those (`node test/run.js team high`, list them with `--list`). Logs go to `<work folder>/logs`; set `TEST_WORK_DIR` to choose the folder.

```bash
pip install -r test/requirements.txt
python -m playwright install chromium
node test/run.js
```

GitHub Actions runs the same suites on every pull request and every push to main, in three parallel groups (`.github/workflows/tests.yml`). A failed group keeps its logs as a build artifact for 7 days.

## Security Notes

- **API Key**: Setting `API_KEY` makes every API request require an `X-API-Key` header (or `apiKey` query parameter). The web page does not send one yet, so with `API_KEY` set only API clients work; leave it empty when people use the web page.
- **BROWSE_ROOTS**: Restrict filesystem browsing to specific directories in production.
- **HTTPS**: Use a reverse proxy (nginx, Caddy) for TLS termination in production; sign-in passwords travel in the request body.
- Credentials are sent as Basic auth over HTTP. Always use HTTPS in production.

## License

ISC
