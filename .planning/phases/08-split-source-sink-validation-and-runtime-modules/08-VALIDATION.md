---
phase: 8
slug: split-source-sink-validation-and-runtime-modules
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-16
---

# Phase 8 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | CTest (C++ via GTest), Node.js integration tests |
| **Config file** | `CMakeLists.txt` per module, `nes-topology-editor/wasm/nes-validator/CMakeLists.txt` |
| **Quick run command** | `cd /tmp/build && ctest --test-dir . -R "SourceCatalogTest\|SinkDescriptorTest\|TopologyValidatorTest" --output-on-failure -j$(nproc)` |
| **Full suite command** | `cd /tmp/build && ctest --test-dir . --output-on-failure -j$(nproc)` |
| **Estimated runtime** | ~60 seconds (quick), ~300 seconds (full) |

---

## Sampling Rate

- **After every task commit:** Run quick run command (source/sink/validator tests)
- **After every plan wave:** Run full suite command
- **Before `/gsd:verify-work`:** Full suite must be green + WASM build succeeds
- **Max feedback latency:** 60 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 08-01-01 | 01 | 1 | VLIB-01 | build | `cmake --build /tmp/build --target nes-sources-validation` | N/A (new target) | ⬜ pending |
| 08-01-02 | 01 | 1 | VLIB-01 | build | `cmake --build /tmp/build --target nes-sinks-validation` | N/A (new target) | ⬜ pending |
| 08-01-03 | 01 | 1 | VLIB-01 | unit | `ctest --test-dir /tmp/build -R SourceCatalogTest --output-on-failure` | ✅ | ⬜ pending |
| 08-02-01 | 02 | 2 | VLIB-01 | integration | `cd nes-topology-editor/wasm && cmake --build build` | ✅ | ⬜ pending |
| 08-02-02 | 02 | 2 | VLIB-01 | integration | `node nes-topology-editor/wasm/build/nes-validator/test-validation.cjs` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

*Existing infrastructure covers all phase requirements.* Existing C++ tests (SourceCatalogTest, SinkDescriptorTest, TopologyValidatorTest) and WASM integration tests already exist from Phase 6.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| validateTopology() rejects File sources missing file_path | VLIB-01 | Can be automated via Node.js test | Run `node test-validation.cjs` with YAML missing file_path, verify error message |
| validateTopology() rejects TCP sources missing socket_host/socket_port | VLIB-01 | Can be automated via Node.js test | Run `node test-validation.cjs` with YAML missing socket_host, verify error message |

*Both can be automated in integration tests during implementation.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
