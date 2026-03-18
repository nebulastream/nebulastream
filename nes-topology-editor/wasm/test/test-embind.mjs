import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const createModule = require('../build/spike-embind/spike-embind.cjs');
const Module = await createModule();
const result = Module.greet("NebulaStream");
console.assert(result === "Hello from WASM, NebulaStream!", `Unexpected: ${result}`);
console.log("PASS: Embind spike");
