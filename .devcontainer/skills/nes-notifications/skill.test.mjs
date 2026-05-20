import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync, realpathSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

const executable = realpathSync(execFileSync('which', ['pi'], { encoding: 'utf8' }).trim());
const { loadSkillsFromDir } = await import(resolve(dirname(executable), '../../dist/core/skills.js'));
const root = fileURLToPath(new URL('../../', import.meta.url));

test('project-specific NES notification skill is discoverable and installed separately from Pi setup', () => {
    const result = loadSkillsFromDir({ dir: resolve(root, 'skills'), source: 'test' });
    assert.deepEqual(result.diagnostics, []);
    const skills = result.skills.filter(skill => skill.name === 'nes-notifications');
    assert.equal(skills.length, 1);
    assert.equal(result.skills.length, 1);
    const text = readFileSync(skills[0].filePath, 'utf8');
    for (const required of ['relay.py', 'PI_NOTIFY_SOCKET', '@NES_NOTIFY_SOCKET@', 'yield_after: 1',
        'acknowledgement', 'SIGINT/SIGTERM', 'untrusted query output']) assert.ok(text.includes(required), required);
    assert.ok(!text.includes('/workspaces/'));
    const installer = readFileSync(resolve(root, 'install-pi-skills.sh'), 'utf8');
    assert.match(installer, /for file in SKILL\.md relay\.py/);
    assert.ok(!installer.includes('test_relay.py'));
    const setup = readFileSync(resolve(root, 'setup-pi.sh'), 'utf8');
    assert.match(setup, /bash "\$script_dir\/install-pi-skills\.sh"/);
});
