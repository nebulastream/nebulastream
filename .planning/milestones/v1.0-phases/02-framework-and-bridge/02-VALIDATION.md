---
phase: 02
slug: framework-and-bridge
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-15
---

# Phase 02 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test (Rust) + ctest (C++ if needed) |
| **Config file** | nes-sources/rust/Cargo.toml (workspace) |
| **Quick run command** | `cd nes-sources/rust && cargo test --workspace -- --test-threads=1` |
| **Full suite command** | `cd nes-sources/rust && cargo test --workspace -- --test-threads=1` |
| **Estimated runtime** | ~5 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cd nes-sources/rust && cargo test --workspace -- --test-threads=1`
- **After every plan wave:** Run `cd nes-sources/rust && cargo test --workspace -- --test-threads=1`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 10 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 02-01-01 | 01 | 1 | FWK-02, FWK-04 | unit | `cargo test --workspace` | ❌ W0 | ⬜ pending |
| 02-01-02 | 01 | 1 | FWK-05 | unit | `cargo test --workspace` | ❌ W0 | ⬜ pending |
| 02-01-03 | 01 | 1 | BRG-01, BRG-02, BRG-03 | unit | `cargo test --workspace` | ❌ W0 | ⬜ pending |
| 02-02-01 | 02 | 2 | FWK-06, FWK-07 | unit | `cargo test --workspace` | ❌ W0 | ⬜ pending |
| 02-02-02 | 02 | 2 | BRG-04 | unit | `cargo test --workspace` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] Test modules within existing source files — BackpressureFuture tests, bridge channel tests, semaphore lifecycle tests
- [ ] Existing cargo test infrastructure covers all phase requirements

*If none: "Existing infrastructure covers all phase requirements."*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| C++ TokioSource plugs into SourceHandle variant | BRG-04 | Requires full cmake build + C++ compilation | Build with cmake, verify TokioSource compiles and links |
| BackpressureController calls Rust FFI callback | FWK-05 | Requires C++ integration test | Trigger backpressure from C++ side, verify Rust future wakes |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
