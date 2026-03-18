---
phase: 01-ffi-foundation
plan: 02
subsystem: ffi
tags: [rust, tokio, buffer-wrapper, source-context, async-channel, cancellation-token, spawn-blocking]

# Dependency graph
requires:
  - phase: 01-ffi-foundation/01
    provides: "cxx bridge declarations for TupleBuffer, AbstractBufferProvider, BackpressureListener"
provides:
  - "TupleBufferHandle safe wrapper with Clone (retain) and Drop (release)"
  - "BufferProviderHandle with unsafe Send and get_buffer_blocking"
  - "OnceLock Tokio runtime initialization with Result<()> for FFI-03"
  - "SourceContext with allocate_buffer().await, emit(buf).await, cancellation_token()"
  - "SourceResult/SourceError error types"
affects: [02-tokio-runtime, 03-source-api]

# Tech tracking
tech-stack:
  added: [cxx 1.0 (runtime dep)]
  patterns: [send-ptr-wrapper-for-spawn-blocking, oncelock-singleton-runtime, unsafe-send-for-ffi-types]

key-files:
  created: []
  modified:
    - nes-sources/rust/nes-source-runtime/src/buffer.rs
    - nes-sources/rust/nes-source-runtime/src/error.rs
    - nes-sources/rust/nes-source-runtime/src/runtime.rs
    - nes-sources/rust/nes-source-runtime/src/context.rs
    - nes-sources/rust/nes-source-runtime/src/lib.rs
    - nes-sources/rust/nes-source-runtime/Cargo.toml

key-decisions:
  - "SendPtr newtype wrapper for raw pointer Send across spawn_blocking boundary (edition 2024 captures individual fields)"
  - "init_source_runtime stub remains in bindings crate; runtime.rs has real logic; wiring deferred to link time via nes-source-lib"

patterns-established:
  - "SendPtr pattern: wrap raw *mut in newtype with unsafe Send for spawn_blocking closures"
  - "OnceLock runtime: process-global Tokio runtime with atomic call counter for duplicate detection"
  - "SourceContext API: allocate_buffer().await + emit(buf).await + cancellation_token()"

requirements-completed: [FFI-01, FFI-02, FWK-01, FWK-03]

# Metrics
duration: 5min
completed: 2026-03-15
---

# Phase 1 Plan 02: Safe Rust Wrappers and SourceContext Summary

**TupleBufferHandle/BufferProviderHandle safe wrappers with OnceLock Tokio runtime and SourceContext async API (allocate_buffer, emit, cancellation_token)**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-15T01:21:08Z
- **Completed:** 2026-03-15T01:26:15Z
- **Tasks:** 3
- **Files modified:** 6

## Accomplishments
- TupleBufferHandle wraps UniquePtr<TupleBuffer> with Clone (cloneTupleBuffer/retain) and Drop (release), exposes as_mut_slice/as_slice/set_number_of_tuples/capacity
- BufferProviderHandle wraps raw *mut AbstractBufferProvider with unsafe Send, provides get_buffer_blocking and buffer_size
- OnceLock Tokio runtime with configurable thread count, duplicate-call warning, Result<()> return for FFI-03 panic safety
- SourceContext provides allocate_buffer().await (via spawn_blocking), emit(buf).await (via async_channel), cancellation_token()
- SourceResult/SourceError types with Display, Error, and From impls
- 13 unit tests covering Send/Sync bounds, API signatures, runtime init, and error types

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement TupleBufferHandle and BufferProviderHandle safe wrappers** - `c32e988dd4` (feat)
2. **Task 2: Implement Tokio runtime init and error types** - `02f0ca1b4b` (feat)
3. **Task 3: Implement SourceContext with allocate_buffer and emit** - `a7a03d0254` (feat)

## Files Created/Modified
- `nes-sources/rust/nes-source-runtime/src/buffer.rs` - TupleBufferHandle and BufferProviderHandle safe wrappers
- `nes-sources/rust/nes-source-runtime/src/runtime.rs` - OnceLock Tokio runtime init with source_runtime() accessor
- `nes-sources/rust/nes-source-runtime/src/error.rs` - SourceResult enum and SourceError type
- `nes-sources/rust/nes-source-runtime/src/context.rs` - SourceContext with allocate_buffer, emit, cancellation_token
- `nes-sources/rust/nes-source-runtime/src/lib.rs` - Module declarations and public re-exports
- `nes-sources/rust/nes-source-runtime/Cargo.toml` - Added cxx dependency for UniquePtr usage

## Decisions Made
- **SendPtr newtype for spawn_blocking:** Rust 2024 edition captures individual closure fields, so a raw `*mut` extracted from SendPtr would lose the Send impl. Created SendPtr with `into_raw()` method to ensure the closure captures the full Send wrapper, not the inner non-Send pointer.
- **Bindings stub unchanged:** The `init_source_runtime` stub in nes-source-bindings remains as-is. The real implementation lives in runtime.rs. At link time via nes-source-lib (staticlib), the real implementation will be used. Updating the stub to delegate would require a circular dependency (bindings -> runtime -> bindings).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added cxx dependency to nes-source-runtime Cargo.toml**
- **Found during:** Task 1 (buffer.rs implementation)
- **Issue:** buffer.rs uses `cxx::UniquePtr` directly but nes-source-runtime didn't have cxx as a dependency
- **Fix:** Added `cxx = "1.0"` to [dependencies] in Cargo.toml
- **Files modified:** nes-sources/rust/nes-source-runtime/Cargo.toml
- **Verification:** `cargo check -p nes_source_runtime` passes
- **Committed in:** c32e988dd4 (Task 1 commit)

**2. [Rule 3 - Blocking] Created SendPtr newtype wrapper for spawn_blocking Send bound**
- **Found during:** Task 3 (SourceContext allocate_buffer)
- **Issue:** Raw `*mut AbstractBufferProvider` is not Send; Rust 2024 edition closure field capture means wrapping in SendPtr and accessing `.0` inside the closure captures the inner non-Send field
- **Fix:** Added `SendPtr` newtype with `into_raw()` method; closure captures `SendPtr` (which is Send) then extracts raw pointer inside the closure body
- **Files modified:** nes-sources/rust/nes-source-runtime/src/context.rs
- **Verification:** `cargo check -p nes_source_runtime` passes, SourceContext Send assertion passes
- **Committed in:** a7a03d0254 (Task 3 commit)

---

**Total deviations:** 2 auto-fixed (2 blocking)
**Impact on plan:** Both auto-fixes necessary for compilation. No scope creep.

## Issues Encountered
None beyond the deviations documented above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Complete nes-source-runtime crate with buffer wrappers, runtime init, error types, and SourceContext
- All Phase 1 API surface exists: `ctx.allocate_buffer().await`, `ctx.emit(buf).await`, `ctx.cancellation_token()`
- Ready for Phase 2 (Tokio runtime integration, BackpressureFuture, source lifecycle)
- Full C++ integration testing requires cmake build (not done in this plan)

## Self-Check: PASSED

All 7 files verified present. All 3 task commits verified in git log.

---
*Phase: 01-ffi-foundation*
*Completed: 2026-03-15*
