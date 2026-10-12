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
| `UPLOAD_PIPELINES` | `4` | Parallel upload pipelines (1-8). Each runs `maxConcurrent` transfers. |
| `UPLOAD_TMPDIR` | `<app>/.upload-tmp` | Staging folder for browser uploads when started with `pm2 start ecosystem.config.cjs` |

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

## Asset Upload Tips

- **Local AEM (https://localhost:4502)**: If your local AEM uses HTTPS with a self-signed cert, set `AEM_ALLOW_SELF_SIGNED=1` in `.env` and restart the server. Run the app locally (`npm start`) so both the app and AEM are on the same machine.
- **App on VM, AEM on your machine**: Use your machine's IP (e.g. `http://192.168.1.100:4502`) instead of `localhost`, since `localhost` from the VM refers to the VM itself.
- **Supported formats**: AEM accepts most file types (images, PDFs, DITA/XML, videos, etc.). The tool does not restrict file types; AEM handles validation.
- **Folder structure**: The full directory hierarchy from your local folder is preserved in AEM DAM. If you upload `my-folder/images/photo.jpg` to `content/dam`, it creates `content/dam/my-folder/images/photo.jpg`.
- **Concurrency**: Files are uploaded by `UPLOAD_PIPELINES` (default 4) parallel pipelines. For browser uploads over 5,000 files each pipeline is capped to 2 transfers.
- **File/folder name sanitization**: Invalid characters (`#%{}?&` in filenames, `.%;#+?^{}"&` and spaces in folder names) are automatically replaced with `-` before upload.
- **Error handling**: If individual files fail, the upload continues. Failed files are collected and available via the "Download Error Report" button (CSV with file name, error message, and error code).
- **Large uploads (up to 50K files)**: Files are chunked (1000 per batch) and sent in parallel to the server, then uploaded to AEM. Session timeouts are set to 8 hours to accommodate very long uploads at low concurrency. Browser uploads are staged on the server first: with `pm2 start ecosystem.config.cjs` in `<app>/.upload-tmp`, otherwise in the system temp folder (often a small `/tmp`). The server refuses an upload that does not fit and says so.
- **Skipped files**: Like `@adobe/aem-upload`, the tool never uploads names starting with `.` or `~`, `*.tmp`, `TestFile*`, `Thumbs.db`, `desktop.ini` and Adobe Bridge caches.

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
- **HTTPS**: Use a reverse proxy (nginx, Caddy) for TLS termination in production.
- Credentials are sent as Basic auth over HTTP. Always use HTTPS in production.

## License

ISC
