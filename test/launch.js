// Starts the app for the tests. All settings (PORT, USERS_FILE, HISTORY_FILE, TMPDIR, ...) come from the environment.
const path = require('path');

const root = path.join(__dirname, '..');
process.chdir(root);
require(path.join(root, 'server.js'));
