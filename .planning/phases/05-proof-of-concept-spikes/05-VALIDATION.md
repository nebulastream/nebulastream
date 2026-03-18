---
phase: 5
slug: proof-of-concept-spikes
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-15
---

# Phase 5 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Node.js scripts (spike validation); vitest for TS integration tests |
| **Config file** | None needed for spike Node.js tests |
| **Quick run command** | `node test/run-spikes.mjs` |
| **Full suite command** | `cd nes-topology-editor/wasm && cmake --build build && node test/run-spikes.mjs` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Build affected spike + run its Node.js test
- **After every plan wave:** Full rebuild + all spike tests
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 05-01-01 | 01 | 1 | WASM-01 | smoke | `cmake --build build --target spike-cpp23` | No -- Wave 0 | ⬜ pending |
| 05-01-02 | 01 | 1 | WASM-01 | smoke | `node test/test-cpp23.mjs` | No -- Wave 0 | ⬜ pending |
| 05-02-01 | 02 | 1 | WASM-02 | smoke | `cmake --build build --target spike-antlr4` | No -- Wave 0 | ⬜ pending |
| 05-02-02 | 02 | 1 | WASM-02 | smoke | `node test/test-antlr4.mjs` | No -- Wave 0 | ⬜ pending |
| 05-03-01 | 03 | 1 | WASM-01 | smoke | `cmake --build build --target spike-embind` | No -- Wave 0 | ⬜ pending |
| 05-03-02 | 03 | 1 | WASM-01 | smoke | `node test/test-embind.mjs` | No -- Wave 0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `nes-topology-editor/wasm/CMakeLists.txt` -- top-level CMake for spikes
- [ ] `nes-topology-editor/wasm/triplets/wasm32-emscripten.cmake` -- vcpkg triplet
- [ ] `nes-topology-editor/wasm/test/test-cpp23.mjs` -- Node.js test for spike 1
- [ ] `nes-topology-editor/wasm/test/test-antlr4.mjs` -- Node.js test for spike 2
- [ ] `nes-topology-editor/wasm/test/test-embind.mjs` -- Node.js test for spike 3
- [ ] Emscripten SDK installation (emsdk)
- [ ] Stub headers for NES-specific ANTLR4 postinclude macros

*Wave 0 creates all test infrastructure before spike implementation begins.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| WASM binary size credibility assessment | WASM-02 | Requires judgement on path to 1MB budget | Measure .wasm size after gzip, compare to budget, document reasoning |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
