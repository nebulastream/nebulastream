import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const createModule = require('../build/spike-antlr4/spike-antlr4.cjs');

const Module = await createModule();

// Test 1: Valid NES SQL parses without error
const validSql = "SELECT * FROM default_logical";
const validResult = Module.parseSql(validSql);
console.assert(!validResult.includes("PARSE_ERROR"),
    `Valid SQL should parse: ${validResult}`);
console.log(`Valid SQL result: ${validResult}`);

// Test 2: Invalid SQL returns PARSE_ERROR
const invalidSql = "SELECTFROM broken!!!";
const invalidResult = Module.parseSql(invalidSql);
console.assert(invalidResult === "PARSE_ERROR",
    `Invalid SQL should fail: ${invalidResult}`);
console.log(`Invalid SQL result: ${invalidResult}`);

// Test 3: NES-specific SQL with WINDOW clause
const nesSql = "SELECT key, value FROM default_logical WINDOW TUMBLING(SIZE 10 SEC)";
const nesResult = Module.parseSql(nesSql);
console.assert(!nesResult.includes("PARSE_ERROR"),
    `NES SQL should parse: ${nesResult}`);
console.log(`NES SQL result: ${nesResult}`);

console.log("PASS: ANTLR4 WASM spike");
