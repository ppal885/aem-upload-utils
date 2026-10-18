/**
 * PM2 config for GUIdes Upload Utility.
 * Usage on VM: pm2 start ecosystem.config.cjs
 */
module.exports = {
  apps: [{
    name: 'aem-upload-utils',
    script: 'server.js',
    cwd: __dirname,
    node_args: '--max-old-space-size=4096',
    exec_mode: 'fork',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '4500M',
    // Many VMs mount /tmp as a small RAM-backed tmpfs; stage uploads on the app's disk instead
    // Account and mail settings are taken from the shell that runs `pm2 start` (see README, Configuration)
    env: {
      NODE_ENV: 'production',
      TMPDIR: process.env.UPLOAD_TMPDIR || require('path').join(__dirname, '.upload-tmp'),
      ...Object.fromEntries(['ADMIN_EMAILS', 'ALLOWED_EMAIL_DOMAINS', 'SIGNUP_ENABLED', 'PUBLIC_URL', 'SMTP_HOST', 'SMTP_PORT',
        'SMTP_USER', 'SMTP_PASS', 'SMTP_SECURE', 'MAIL_FROM', 'PACKAGES_DIR', 'PACKAGE_KEEP_DAYS', 'UPLOAD_PIPELINES',
        'TLS_CERT_FILE', 'TLS_KEY_FILE', 'HTTPS_PORT', 'HTTPS_ONLY', 'MIN_PASSWORD_LENGTH',
        'BACKUP_DIR', 'BACKUP_KEEP_DAYS', 'MAX_UPLOADS_PER_SERVER',
        'COPY_TARGETS', 'COPY_ALLOW_DOWNLOAD', 'COPY_IDLE_MINUTES', 'COPY_PACKAGE_KEEP_HOURS', 'SESSIONS_FILE']
        .filter((k) => process.env[k] !== undefined).map((k) => [k, process.env[k]])),
    },
    merge_logs: true,
    time: true,
  }],
};
