const { spawnSync } = require('node:child_process');

function logout() {
  if (!process.env.STATE_docker_login) {
    return;
  }
  const { registry, configDir } = JSON.parse(process.env.STATE_docker_login);
  const result = spawnSync('docker', ['logout', registry], {
    encoding: 'utf8',
    env: { ...process.env, DOCKER_CONFIG: configDir },
  });
  if (result.error || result.status !== 0) {
    // Cleanup failure should not replace the job's result.
    console.warn(`Docker logout from ${registry} failed: ${result.error?.message || result.stderr.trim() || result.status}`);
  } else {
    console.log(`Logged out of ${registry}`);
  }
}

if (require.main === module) {
  logout();
}

module.exports = { logout };
