---
phase: 3
slug: validation
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-15
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test (Rust unit) + GTest (C++ integration via CMake) |
| **Config file** | nes-sources/rust/Cargo.toml + nes-sources/CMakeLists.txt |
| **Quick run command** | `cd nes-sources/rust && cargo test --workspace --lib` |
| **Full suite command** | `cd nes-sources/rust && cargo test --workspace --lib && cd /tmp/tokio-sources && cmake --build build --target nes-source-tests && ctest --test-dir build -R TokioSource` |
| **Estimated runtime** | ~30 seconds (Rust) + ~60 seconds (C++ build+test) |

---

## Sampling Rate

- **After every task commit:** Run `cd nes-sources/rust && cargo test --workspace --lib`
- **After every plan wave:** Run full suite command
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 90 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 03-01-01 | 01 | 1 | VAL-01 | unit | `cargo test --workspace --lib` | ❌ W0 | ⬜ pending |
| 03-01-02 | 01 | 1 | VAL-02 | integration | `ctest -R TokioSource` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `nes-sources/rust/nes-source-runtime/src/generator.rs` — GeneratorSource implementation
- [ ] `nes-sources/tests/TokioSourceTest.cpp` — GTest integration test file

*Existing Rust test infrastructure covers unit tests. C++ GTest framework already configured in CMakeLists.txt.*

---

## Manual-Only Verifications

*All phase behaviors have automated verification.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 90s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
