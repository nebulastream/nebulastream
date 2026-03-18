import { execSync } from 'child_process';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const buildDir = join(__dirname, '..', 'build');
const spikeJs = join(buildDir, 'spike-cpp23', 'spike-cpp23.cjs');

try {
    const output = execSync(`node ${spikeJs}`, { encoding: 'utf-8' });
    if (output.includes('ALL C++23 TESTS PASSED')) {
        console.log('PASS: C++23 spike');
    } else {
        console.error('FAIL: C++23 spike - unexpected output:', output);
        process.exit(1);
    }
} catch (e) {
    console.error('FAIL: C++23 spike');
    console.error(e.stderr || e.message);
    process.exit(1);
}
