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
  a restart signs everyone out.
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

- Staging folders are removed after every upload; abandoned sessions are reaped, and orphaned staging folders
  older than a day are swept every hour (and at start-up).
- An upload or package build that would not fit on the staging disk is refused up front.
- Packages expire after `PACKAGE_KEEP_DAYS`. PM2 logs rotate with `pm2-logrotate` (50 MB, 5 files, compressed);
  the upload library's per-file debug output is only logged with `LOG_LEVEL=debug`.

### Deploying a new version with PM2

Check that nothing is uploading (`curl -s http://<host>:3000/api/upload/active` prints `[]`), then:

```bash
git pull && pm2 delete aem-upload-utils && pm2 start ecosystem.config.cjs && pm2 save
```

`pm2 restart` keeps the old process settings, so changes to `ecosystem.config.cjs` (staging folder, fork mode)
only take effect with `delete` + `start`. A restart during an upload stops it; it is recorded in history as interrupted.

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
| Skip existing assets | Not supported. Existing DAM assets are overwritten. |
| Metadata mapping (title, tags, XMP) | Not supported. AEM auto-extracts basic metadata (dimensions, MIME type). |
| Duplicate detection | Not supported. No pre-upload hash or AEM API check. |
| DAM workflow triggers | Not supported. AEM runs its default DAM Update Asset workflow. |
| Asset processing profiles | Not configurable. Uses AEM server defaults. |

## Security Notes

- **API Key**: Setting `API_KEY` makes every API request require an `X-API-Key` header (or `apiKey` query parameter). The web page does not send one yet, so with `API_KEY` set only API clients work; leave it empty when people use the web page.
- **BROWSE_ROOTS**: Restrict filesystem browsing to specific directories in production.
- **HTTPS**: Use a reverse proxy (nginx, Caddy) for TLS termination in production; sign-in passwords travel in the request body.
- Credentials are sent as Basic auth over HTTP. Always use HTTPS in production.

## License

ISC
