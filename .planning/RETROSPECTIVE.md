# Project Retrospective

*A living document updated after each milestone. Lessons feed forward into future planning.*

## Milestone: v1.0 — Tokio-Based Rust Sources

**Shipped:** 2026-03-16
**Phases:** 3 | **Plans:** 9

### What Was Built
- Complete Rust ↔ C++ FFI layer via cxx with safe wrappers for TupleBuffer and BufferProvider
- AsyncSource trait and SourceContext providing a minimal `allocate_buffer()` + `emit().await` interface
- Bridge thread architecture with per-source emit registry, backpressure, and inflight semaphore
- TokioSource C++ class integrating into the existing SourceHandle via std::variant
- GeneratorSource reference implementation
- End-to-end integration tests (buffer ordering, backpressure, shutdown safety)

### What Worked
- Coarse 3-phase structure (FFI → Framework → Validation) kept dependencies clean and phases independently completable
- TDD approach in Phase 2 (failing tests first) caught bridge thread dispatch issues early
- cfg(test) substitution pattern for SourceContext allowed pure-Rust unit tests without C++ linker
- PIMPL pattern for rust::Box types kept CXX types out of public C++ headers

### What Was Inefficient
- Phase 3 required significant build infrastructure fixes (namespace issues, duplicate linker symbols) that should have been caught in Phase 2
- Several architectural decisions evolved during stress testing after Phase 3 (EOS through bridge channel, async buffer allocation, semaphore moved to sources) — these were improvements but weren't captured in plans
- Buffer leak (double-retain) was subtle and required extensive debugging to identify

### Patterns Established
- `Arc::into_raw/from_raw` for passing Rust reference-counted types through C++ callbacks
- `BridgeMessage` enum through bounded `async_channel` for async→sync bridging
- Static `LazyLock<Notify>` for C++→Rust async notifications (buffer recycle)
- Per-buffer `onComplete` callback on `SourceReturnType::Data` for end-to-end lifecycle tracking
- `#[unsafe(no_mangle)] extern "C"` for Rust→C++ callbacks bypassing CXX limitations

### Key Lessons
1. Copy constructors that call retain() are easy to miss — never add explicit retain() in bridge code without verifying the constructor doesn't already do it
2. Inflight semaphore permits must be released when the pipeline finishes processing, not when the bridge dispatches — OnComplete callback is the right hook
3. EOS must travel the same path as data (through the bridge channel) to guarantee ordering
4. tryGetBuffer + Notify is simpler and more correct than spawn_blocking for async buffer allocation

### Cost Observations
- Model mix: 100% opus (quality profile)
- Total execution time: ~92min across 9 plans
- Notable: Phase 1 averaged 3.7min/plan, Phase 2 averaged 11.8min/plan, Phase 3 averaged 34min/plan (build infrastructure complexity)

---

## Cross-Milestone Trends

### Process Evolution

| Milestone | Phases | Plans | Key Change |
|-----------|--------|-------|------------|
| v1.0 | 3 | 9 | Initial — coarse phases, TDD, cfg(test) isolation |

### Top Lessons (Verified Across Milestones)

1. FFI boundary code needs extra scrutiny for ownership semantics (retain/release, Arc lifecycle)
2. Stress testing after feature completion reveals architectural issues that unit tests miss
