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
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '4500M',
    env: { NODE_ENV: 'production' },
    merge_logs: true,
    time: true,
  }],
};
