---
phase: 6
slug: validation-library-extraction
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-03-15
---

# Phase 6 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | C++ native tests via ctest / Node.js WASM tests via node --test |
| **Config file** | nes-validator/tests/CMakeLists.txt (native) / nes-topology-editor/wasm/test/ (WASM) |
| **Quick run command** | `cd build && ctest -R nes-validator --output-on-failure -j$(nproc)` |
| **Full suite command** | `cd build && ctest -R nes-validator --output-on-failure -j$(nproc) && node nes-topology-editor/wasm/test/test-validator.mjs` |
| **Estimated runtime** | ~15 seconds (native) + ~10 seconds (WASM) |

---

## Sampling Rate

- **After every task commit:** Run `cd build && ctest -R nes-validator --output-on-failure -j$(nproc)`
- **After every plan wave:** Run full suite (native + WASM)
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 25 seconds (applies to incremental ctest runs; cold cmake --build commands for initial compilation will take longer and are excluded from this latency target)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| TBD | TBD | TBD | VLIB-01 | build | cmake --build build --target nes-validator | W0 | pending |
| TBD | TBD | TBD | VLIB-02 | build | cmake --build build --target nes-validator (with stubs) | W0 | pending |
| TBD | TBD | TBD | VLIB-03 | unit | ctest -R nes-validator-yaml-test | W0 | pending |
| TBD | TBD | TBD | VLIB-04 | unit | ctest -R nes-validator-catalog-test | W0 | pending |
| TBD | TBD | TBD | VLIB-05 | unit | ctest -R nes-validator-sql-test | W0 | pending |
| TBD | TBD | TBD | VLIB-06 | unit | ctest -R nes-validator-inference-test | W0 | pending |
| TBD | TBD | TBD | WASM-03 | integration | node nes-topology-editor/wasm/test/test-validator.mjs | W0 | pending |
| TBD | TBD | TBD | WASM-04 | size check | gzip -c build/nes-validator.wasm \| wc -c | W0 | pending |
| TBD | TBD | TBD | TEST-01 | unit | ctest -R nes-validator --output-on-failure | W0 | pending |

*Status: pending / green / red / flaky*

---

## Wave 0 Requirements

- [ ] `nes-validator/tests/CMakeLists.txt` -- test target registration
- [ ] `nes-validator/tests/ValidatorTest.cpp` -- stubs for VLIB-03..06, TEST-01
- [ ] `nes-topology-editor/wasm/test/test-validator.mjs` -- WASM integration test stubs for WASM-03

*Existing Phase 5 test infrastructure (test-antlr4.mjs, test-embind.mjs) provides patterns to follow.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| WASM binary size < 1MB gzipped | WASM-04 | Size check depends on build output | `gzip -c build/nes-validator.wasm \| wc -c` -- verify < 1048576 |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 25s (for incremental ctest; initial cmake --build excluded)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved
