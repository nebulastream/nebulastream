const { spawnSync } = require('node:child_process');
const { appendFileSync } = require('node:fs');
const { homedir } = require('node:os');
const { join, resolve } = require('node:path');
const { setTimeout: sleep } = require('node:timers/promises');

async function login() {
  const registry = process.env.INPUT_REGISTRY || 'docker.io';
  const username = process.env.INPUT_USERNAME;
  const password = process.env.INPUT_PASSWORD;
  if (!username || !password) {
    throw new Error('Docker registry username and password are required');
  }

  const configDir = resolve(process.env.DOCKER_CONFIG || join(homedir(), '.docker'));
  // Save the original config location before attempting login so post-job cleanup
  // also runs after a failed login and is unaffected by later DOCKER_CONFIG changes.
  appendFileSync(process.env.GITHUB_STATE, `docker_login=${JSON.stringify({ registry, configDir })}\n`);

  const maxAttempts = 5;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const result = spawnSync('docker', ['login', '--username', username, '--password-stdin', registry], {
      input: password,
      encoding: 'utf8',
      env: { ...process.env, DOCKER_CONFIG: configDir },
    });
    if (result.error) {
      throw result.error;
    }
    if (result.status === 0) {
      console.log(`Logged in to ${registry}`);
      return;
    }
    console.error(result.stderr.trim() || `docker login exited with status ${result.status}`);
    if (attempt === maxAttempts) {
      throw new Error(`Docker login to ${registry} failed after ${maxAttempts} attempts`);
    }
    const delay = 5 * 2 ** (attempt - 1);
    console.log(`Docker login attempt ${attempt}/${maxAttempts} failed. Retrying in ${delay}s`);
    await sleep(delay * 1000);
  }
}

if (require.main === module) {
  login().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}

module.exports = { login };
