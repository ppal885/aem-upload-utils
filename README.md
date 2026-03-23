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
- File/folder exclude patterns
- CLI script for headless uploads
- Optional API key authentication
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

Copy `.env.example` to `.env` and customize:

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | Server port |
| `API_KEY` | _(empty)_ | API key for authentication. Leave empty to disable (dev mode). |
| `CORS_ORIGIN` | `*` | Allowed CORS origins |
| `LOG_LEVEL` | `info` | Logging verbosity: `error`, `warn`, `info`, `debug` |
| `BROWSE_ROOTS` | _(empty)_ | Restrict file browsing to these directories (comma-separated) |
| `AEM_ALLOW_SELF_SIGNED` | `0` | Set to `1` to allow self-signed certs for `https://localhost:4502` (dev only) |

## CLI Usage

```bash
node migrate-to-aem.js <baseUrl> <damPath> <localPath> <username> <password>
```

Example:

```bash
node migrate-to-aem.js https://author-p1234-e5678.adobeaemcloud.com content/dam/my-folder C:\content admin admin
```

Or use environment variables (see `.env.example`).

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
- **Concurrency**: For large uploads (5K+ files), AEM concurrency is auto-capped to 2 threads. This prevents AEM overload and ensures stability.
- **File/folder name sanitization**: Invalid characters (`#%{}?&` in filenames, `.%;#+?^{}"&` and spaces in folder names) are automatically replaced with `-` before upload.
- **Error handling**: If individual files fail, the upload continues. Failed files are collected and available via the "Download Error Report" button (CSV with file name, error message, and error code).
- **Large uploads (up to 50K files)**: Files are chunked (1000 per batch) and sent in parallel to the server, then uploaded to AEM. Session timeouts are set to 8 hours to accommodate very long uploads at low concurrency. For 50K uploads, ensure `/tmp` has at least 5–10 GB (or use appropriate tmpfs sizing) to avoid rearrange failures from insufficient disk space.

### Known Limitations

| Feature | Status |
|---|---|
| Skip existing assets | Not supported. Existing DAM assets are overwritten. |
| Metadata mapping (title, tags, XMP) | Not supported. AEM auto-extracts basic metadata (dimensions, MIME type). |
| Duplicate detection | Not supported. No pre-upload hash or AEM API check. |
| DAM workflow triggers | Not supported. AEM runs its default DAM Update Asset workflow. |
| Asset processing profiles | Not configurable. Uses AEM server defaults. |

## Security Notes

- **API Key**: Set `API_KEY` environment variable before deploying to a shared network. All API requests must include `X-API-Key` header.
- **BROWSE_ROOTS**: Restrict filesystem browsing to specific directories in production.
- **HTTPS**: Use a reverse proxy (nginx, Caddy) for TLS termination in production.
- Credentials are sent as Basic auth over HTTP. Always use HTTPS in production.

## License

ISC
