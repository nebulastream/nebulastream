---
phase: 1
slug: ffi-foundation
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-15
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust `#[cfg(test)]` + `#[tokio::test]` for async, Google Test for C++ integration |
| **Config file** | Cargo.toml `[dev-dependencies]` section in each crate |
| **Quick run command** | `cargo test -p nes_source_runtime --lib` |
| **Full suite command** | `cargo test --workspace` (from nes-sources/rust/) |
| **Estimated runtime** | ~10 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test -p nes_source_runtime --lib`
- **After every plan wave:** Run `cargo test --workspace`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 1-01-01 | 01 | 1 | FFI-01 | unit | `cargo test -p nes_source_runtime buffer::tests::test_clone_drop_refcount` | ❌ W0 | ⬜ pending |
| 1-01-02 | 01 | 1 | FFI-04 | build | `cargo build -p nes_source_bindings` | ❌ W0 | ⬜ pending |
| 1-01-03 | 01 | 1 | FFI-02 | integration | Requires C++ linkage — test via CMake/ctest | ❌ W0 | ⬜ pending |
| 1-01-04 | 01 | 1 | FFI-03 | unit | `cargo test -p nes_source_bindings tests::test_panic_handling` | ❌ W0 | ⬜ pending |
| 1-01-05 | 01 | 1 | FWK-01 | unit | `cargo test -p nes_source_runtime runtime::tests::test_runtime_init_once` | ❌ W0 | ⬜ pending |
| 1-01-06 | 01 | 1 | FWK-03 | unit | `cargo test -p nes_source_runtime context::tests::test_source_context_api` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `nes-sources/rust/` workspace — entire directory structure needs creation
- [ ] `nes-source-runtime/src/buffer.rs` — TupleBufferHandle tests (FFI-01)
- [ ] `nes-source-runtime/src/runtime.rs` — runtime init tests (FWK-01)
- [ ] `nes-source-runtime/src/context.rs` — SourceContext API tests (FWK-03)
- [ ] `nes-source-bindings/lib.rs` — cxx bridge declarations (FFI-04)

*Note: FFI-01 refcount assertion test requires C++ linkage. Pure Rust unit test can verify Clone/Drop call sites but actual refcount verification needs a mock or integration test via CMake.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| FFI-02 buffer allocation via C++ | FFI-02 | Requires linked C++ BufferManager | Build with CMake, run integration test that allocates and returns a buffer |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
