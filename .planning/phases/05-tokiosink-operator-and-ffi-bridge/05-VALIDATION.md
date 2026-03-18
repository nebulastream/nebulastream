---
phase: 05
slug: tokiosink-operator-and-ffi-bridge
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-16
---

# Phase 05 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust: cargo test (tokio::test), C++: GTest |
| **Config file** | Cargo.toml (nes-source-runtime), CMakeLists.txt (nes-sources/tests) |
| **Quick run command** | `cargo test -p nes-source-runtime --lib` |
| **Full suite command** | `cargo test -p nes-source-runtime && cargo test -p nes-io-bindings` |
| **Estimated runtime** | ~15 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test -p nes-source-runtime --lib`
- **After every plan wave:** Run `cargo test -p nes-source-runtime && cargo test -p nes-io-bindings`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 05-01-01 | 01 | 1 | PLN-01 (partial) | build | `cargo build -p nes-io-bindings` | ❌ W0 | ⬜ pending |
| 05-01-02 | 01 | 1 | PLN-01 (partial) | unit | `cargo test -p nes-io-bindings` | ❌ W0 | ⬜ pending |
| 05-02-01 | 02 | 2 | PLN-01, PLN-02 | unit+integration | `cargo test -p nes-source-runtime sink` | ❌ W0 | ⬜ pending |
| 05-02-02 | 02 | 2 | PLN-03 | unit | `cargo test -p nes-source-runtime backpressure` | ❌ W0 | ⬜ pending |
| 05-02-03 | 02 | 2 | PLN-04 | unit | `cargo test -p nes-source-runtime flush` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Existing Rust test infrastructure covers all phase requirements (tokio::test, async_channel test utilities)
- C++ GTest infrastructure exists for TokioSource tests — extend for TokioSink
- No new frameworks needed

*Existing infrastructure covers all phase requirements.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Buffer pool not exhausted under sustained load | PLN-04 (refcount) | Requires sustained load test beyond unit scope | Run generator source -> TokioSink pipeline with 1000+ buffers, verify no pool exhaustion |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
