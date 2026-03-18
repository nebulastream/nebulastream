---
phase: 4
slug: rust-sink-framework
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-16
---

# Phase 4 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test (Rust unit tests) |
| **Config file** | nes-sources/rust/nes-source-runtime/Cargo.toml |
| **Quick run command** | `cargo test -p nes_source_runtime --lib` |
| **Full suite command** | `cargo test -p nes_source_runtime` |
| **Estimated runtime** | ~5 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test -p nes_source_runtime --lib`
- **After every plan wave:** Run `cargo test -p nes_source_runtime`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 5 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 04-01-01 | 01 | 1 | SNK-01 | unit | `cargo test -p nes_source_runtime sink` | ❌ W0 | ⬜ pending |
| 04-01-02 | 01 | 1 | SNK-02 | unit | `cargo test -p nes_source_runtime sink_context` | ❌ W0 | ⬜ pending |
| 04-02-01 | 02 | 1 | SNK-03, SNK-04 | unit | `cargo test -p nes_source_runtime spawn_sink` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `nes-sources/rust/nes-source-runtime/src/sink.rs` — AsyncSink trait, SinkMessage enum, SinkError type
- [ ] `nes-sources/rust/nes-source-runtime/src/sink_context.rs` — SinkContext with recv() and CancellationToken

*Existing test infrastructure (cargo test) covers all phase requirements.*

---

## Manual-Only Verifications

*All phase behaviors have automated verification.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 5s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
