import { execSync } from 'child_process';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

const tests = [
    { name: 'C++23 Features', file: 'test-cpp23.mjs' },
    { name: 'Embind Interop', file: 'test-embind.mjs' },
    { name: 'ANTLR4 WASM', file: 'test-antlr4.mjs' },
];

let allPassed = true;
for (const test of tests) {
    try {
        execSync(`node ${join(__dirname, test.file)}`, { stdio: 'inherit' });
        console.log(`PASS: ${test.name}\n`);
    } catch {
        console.error(`FAIL: ${test.name}\n`);
        allPassed = false;
    }
}

console.log(allPassed ? 'ALL SPIKES PASSED' : 'SOME SPIKES FAILED');
process.exit(allPassed ? 0 : 1);
