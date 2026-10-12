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
    env: { NODE_ENV: 'production', TMPDIR: process.env.UPLOAD_TMPDIR || require('path').join(__dirname, '.upload-tmp') },
    merge_logs: true,
    time: true,
  }],
};
