# Run GUIdes Upload Utility on VM (forever)

## 1. One-time setup on VM

```bash
# Go to app folder (after copying code to VM)
cd ~/aem-upload-utils

# Install dependencies (if not done)
npm install

# Install PM2 globally (run once)
npm install -g pm2
```

## 2. Start the app (runs forever, restarts on crash)

```bash
cd ~/aem-upload-utils
pm2 start ecosystem.config.cjs
```

Or without the config file:

```bash
pm2 start server.js --name aem-upload-utils --node-args="--max-old-space-size=4096"
```

## 3. Make it start on VM reboot

```bash
pm2 startup
# Run the command it prints (usually with sudo)
pm2 save
```

## 4. Useful PM2 commands

| Command | Description |
|--------|-------------|
| `pm2 status` | List apps and status |
| `pm2 logs aem-upload-utils` | View logs (Ctrl+C to exit) |
| `pm2 restart aem-upload-utils` | Restart the app |
| `pm2 stop aem-upload-utils` | Stop the app |
| `pm2 delete aem-upload-utils` | Remove from PM2 |
| `pm2 monit` | Live dashboard |

## 5. Access the app

- From VM: `http://localhost:3000`
- From your machine: `http://<VM_IP>:3000` (ensure port 3000 is open on the VM firewall)

## 6. Large uploads (50K files)

For 50K file uploads, ensure `/tmp` has at least 5–10 GB (or use appropriate tmpfs sizing). This avoids rearrange failures from insufficient disk space during the finalization phase.

## 7. Optional: run on a specific port

```bash
PORT=8080 pm2 start ecosystem.config.cjs
# or
pm2 start server.js --name aem-upload-utils --node-args="--max-old-space-size=4096" --update-env -- PORT=8080
```

Better: set in environment before starting:

```bash
export PORT=8080
pm2 start ecosystem.config.cjs
```
