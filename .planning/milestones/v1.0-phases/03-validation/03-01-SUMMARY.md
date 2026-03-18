---
phase: 03-validation
plan: 01
subsystem: runtime
tags: [generator-source, async-source, cxx-bridge, ffi, tokio, tdd]

# Dependency graph
requires:
  - phase: 02-framework-and-bridge
    provides: "AsyncSource trait, SourceContext, spawn_source, CXX bridge"
provides:
  - "GeneratorSource implementing AsyncSource (allocate_buffer + emit + cancellation)"
  - "spawn_generator_source FFI entry point in CXX bridge"
  - "Test-only TupleBufferHandle (Vec<u8> backing, no C++ linker)"
  - "Test-only allocate_buffer() returning scratch buffer"
affects: [03-02-integration-tests]

# Tech tracking
tech-stack:
  added: []
  patterns: [cfg-test-buffer-substitution, source-context-sync]

key-files:
  created:
    - nes-sources/rust/nes-source-runtime/src/generator.rs
  modified:
    - nes-sources/rust/nes-source-runtime/src/lib.rs
    - nes-sources/rust/nes-source-runtime/src/buffer.rs
    - nes-sources/rust/nes-source-runtime/src/context.rs
    - nes-sources/rust/nes-source-lib/src/lib.rs

key-decisions:
  - "Test-only TupleBufferHandle uses Vec<u8> instead of cxx::UniquePtr to avoid C++ linker symbols in pure Rust tests"
  - "SourceContext impl Sync (BufferManager is thread-safe) so &SourceContext is Send in async futures"
  - "GeneratorSource checks cancellation before each emit and during sleep interval (not tokio::select! around allocate_buffer)"

patterns-established:
  - "cfg(test) TupleBufferHandle: Vec<u8> scratch buffer with no-op set_number_of_tuples, avoids all CXX linker deps"
  - "cfg(test) allocate_buffer: returns TupleBufferHandle::new_test() directly, no spawn_blocking"
  - "Source test pattern: bounded async_channel receiver to count emit() calls"

requirements-completed: [VAL-01, VAL-02]

# Metrics
duration: 34min
completed: 2026-03-15
---

# Phase 3 Plan 01: GeneratorSource and CXX Bridge Entry Point Summary

**GeneratorSource emitting N u64 values via allocate_buffer/emit with cancellation support, plus spawn_generator_source FFI entry point for C++ integration**

## Performance

- **Duration:** 34 min
- **Started:** 2026-03-15T15:11:11Z
- **Completed:** 2026-03-15T15:44:49Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- GeneratorSource implements AsyncSource: loops 0..count, writes u64 LE bytes, emits via SourceContext
- 4 unit tests: exact emit count (5), zero-count no-op, cancellation exits early, trait compile check
- spawn_generator_source declared in CXX bridge ffi_source module for C++ callers
- All 48 workspace tests pass (Phase 1 + 2 + 3)

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement GeneratorSource with Rust unit tests** - `f20e312474` (feat)
2. **Task 2: Add spawn_generator_source CXX bridge entry point** - `fcfadf22d5` (feat)

## Files Created/Modified
- `nes-sources/rust/nes-source-runtime/src/generator.rs` - GeneratorSource struct + AsyncSource impl + 4 unit tests
- `nes-sources/rust/nes-source-runtime/src/lib.rs` - Added pub mod generator and pub use GeneratorSource
- `nes-sources/rust/nes-source-runtime/src/buffer.rs` - Split into cfg(test)/cfg(not(test)) TupleBufferHandle (Vec<u8> vs UniquePtr)
- `nes-sources/rust/nes-source-runtime/src/context.rs` - Added cfg(test) allocate_buffer() + unsafe impl Sync
- `nes-sources/rust/nes-source-lib/src/lib.rs` - Added spawn_generator_source FFI declaration + implementation

## Decisions Made
- **Test-only TupleBufferHandle backed by Vec<u8>:** cxx::UniquePtr<TupleBuffer> pulls C++ linker symbols even when null. Replaced entire struct definition with cfg(test) variant using Vec<u8> scratch buffer. All buffer methods (as_mut_slice, set_number_of_tuples, etc) have test-only implementations.
- **SourceContext unsafe impl Sync:** BufferProviderHandle contains raw pointer preventing auto-Sync. BufferManager is thread-safe (atomics + folly::MPMCQueue). Adding Sync allows &SourceContext to be Send, required for async fns producing Send futures.
- **Cancellation pattern:** Check is_cancelled() before each emit + tokio::select! during sleep interval. Avoids borrowing &ctx across select! async block (which would require Sync for the inner async block reference).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Test-only TupleBufferHandle with Vec<u8> backing**
- **Found during:** Task 1 (GeneratorSource tests)
- **Issue:** cxx::UniquePtr<TupleBuffer>::null() and its Drop/Clone impls require C++ linker symbols. Cannot create/drop TupleBufferHandle in pure Rust tests.
- **Fix:** Split TupleBufferHandle into cfg(test) (Vec<u8>) and cfg(not(test)) (UniquePtr) variants. Test variant has no-op stubs for all FFI methods.
- **Files modified:** nes-source-runtime/src/buffer.rs
- **Verification:** All 48 tests pass, cargo check on nes_source_lib succeeds
- **Committed in:** f20e312474 (Task 1 commit)

**2. [Rule 3 - Blocking] cfg(test) allocate_buffer() returning scratch buffer**
- **Found during:** Task 1 (GeneratorSource tests)
- **Issue:** Production allocate_buffer() calls spawn_blocking + ffi::getBufferBlocking. Cannot call in test builds.
- **Fix:** Added cfg(test) allocate_buffer() returning TupleBufferHandle::new_test()
- **Files modified:** nes-source-runtime/src/context.rs
- **Verification:** GeneratorSource tests call allocate_buffer without C++ linker
- **Committed in:** f20e312474 (Task 1 commit)

**3. [Rule 3 - Blocking] SourceContext unsafe impl Sync**
- **Found during:** Task 1 (GeneratorSource compile error)
- **Issue:** AsyncSource::run() returns impl Future + Send. GeneratorSource borrows &ctx in async body. &SourceContext must be Send, requiring Sync. Raw pointer in BufferProviderHandle prevents auto-Sync.
- **Fix:** Added unsafe impl Sync for SourceContext with safety justification (BufferManager is thread-safe)
- **Files modified:** nes-source-runtime/src/context.rs
- **Verification:** GeneratorSource compiles and all tests pass
- **Committed in:** f20e312474 (Task 1 commit)

---

**Total deviations:** 3 auto-fixed (3 blocking)
**Impact on plan:** All auto-fixes necessary to enable pure Rust testing of sources that call allocate_buffer/emit. Establishes reusable test infrastructure pattern for future source implementations. No scope creep.

## Issues Encountered
None beyond the auto-fixed deviations above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- GeneratorSource ready for C++ integration tests (Plan 03-02)
- spawn_generator_source CXX entry point ready for TokioSource C++ wrapper to call
- Test infrastructure (cfg(test) TupleBufferHandle) available for any future source implementations

---
*Phase: 03-validation*
*Completed: 2026-03-15*
