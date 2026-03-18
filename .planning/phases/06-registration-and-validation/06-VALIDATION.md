---
phase: 06
slug: registration-and-validation
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-16
---

# Phase 06 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust: cargo test (tokio::test), C++: GTest, Systest: NES systest runner |
| **Config file** | Cargo.toml (nes-source-runtime), CMakeLists.txt (nes-sinks, nes-sources) |
| **Quick run command** | `cargo test -p nes-source-runtime --lib` |
| **Full suite command** | `cargo test -p nes-source-runtime && cargo test -p nes-source-lib` |
| **Estimated runtime** | ~15 seconds (Rust), ~60 seconds (C++ build + test) |

---

## Sampling Rate

- **After every task commit:** Run `cargo test -p nes-source-runtime --lib`
- **After every plan wave:** Run full Rust suite + C++ build check
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds (Rust), 60 seconds (C++)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 06-01-01 | 01 | 1 | VAL-01 (partial) | unit | `cargo test -p nes-source-runtime sink_context` | ✅ | ⬜ pending |
| 06-01-02 | 01 | 1 | VAL-01 | unit | `cargo test -p nes-source-runtime file_sink` | ❌ W0 | ⬜ pending |
| 06-02-01 | 02 | 2 | REG-01 | build | `cmake --build ... --target nes-sinks` | ❌ W0 | ⬜ pending |
| 06-02-02 | 02 | 2 | REG-01, VAL-01 | build+unit | `cargo test -p nes-source-lib` | ❌ W0 | ⬜ pending |
| 06-02-03 | 02 | 2 | VAL-02, VAL-03 | systest | systest runner | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- Existing Rust test infrastructure covers SinkMessage extension
- C++ GTest infrastructure exists for sink tests
- Systest runner exists for end-to-end tests

*Existing infrastructure covers all phase requirements.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Systest output file contains expected data | VAL-02 | Raw binary verification at scale | Run systest, inspect output file size matches expected tuple count x tuple size |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s (Rust) / 60s (C++)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
