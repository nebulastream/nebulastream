---
phase: 06-registration-and-validation
plan: 01
subsystem: sinks
tags: [tokio, async-file-sink, cxx-bridge, flush, rust-sinks]

# Dependency graph
requires:
  - phase: 04-rust-sink-framework
    provides: AsyncSink trait, SinkContext, SinkMessage, spawn_sink lifecycle
  - phase: 05-tokio-sink-operator
    provides: ffi_sink CXX bridge, SinkHandle wrapper, DevNullSink
provides:
  - SinkMessage::Flush variant for advisory flush signaling
  - AsyncFileSink reference sink writing raw binary via tokio::fs
  - spawn_file_sink CXX FFI entry point for C++ factory integration
affects: [06-02-PLAN, 06-03-PLAN]

# Tech tracking
tech-stack:
  added: [tokio-fs, tokio-io-util]
  patterns: [file-sink-pattern, flush-message-pattern]

key-files:
  created:
    - nes-sources/rust/nes-source-runtime/src/file_sink.rs
  modified:
    - nes-sources/rust/nes-source-runtime/src/sink_context.rs
    - nes-sources/rust/nes-source-runtime/src/sink.rs
    - nes-sources/rust/nes-source-runtime/src/sink_handle.rs
    - nes-sources/rust/nes-source-runtime/src/lib.rs
    - nes-sources/rust/nes-source-runtime/Cargo.toml
    - nes-sources/rust/nes-source-lib/src/lib.rs

key-decisions:
  - "Added tokio io-util feature alongside fs for AsyncWriteExt methods (write_all, flush)"

patterns-established:
  - "File sink pattern: open file in run(), loop on recv(), write_all on Data, flush on Flush/Close"
  - "Flush message pattern: advisory non-blocking flush, sinks handle as best-effort"

requirements-completed: [VAL-01]

# Metrics
duration: 3min
completed: 2026-03-16
---

# Phase 6 Plan 01: Rust Sink Extensions Summary

**SinkMessage::Flush variant, AsyncFileSink writing raw binary via tokio::fs, and spawn_file_sink CXX FFI entry point**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-16T18:07:17Z
- **Completed:** 2026-03-16T18:10:26Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Extended SinkMessage enum with Flush variant for advisory flush signaling
- Implemented AsyncFileSink as reference sink writing raw binary data from TupleBufferHandle to file
- Added spawn_file_sink CXX bridge function for C++ factory integration
- 4 new unit tests covering write, flush, close, and error cases
- All 77 Rust tests pass

## Task Commits

Each task was committed atomically:

1. **Task 1: Add SinkMessage::Flush variant and update all match arms** - `a9dbf4d509` (feat)
2. **Task 2: Implement AsyncFileSink and spawn_file_sink FFI** - `102b4d0bb2` (feat)

## Files Created/Modified
- `nes-sources/rust/nes-source-runtime/src/file_sink.rs` - AsyncFileSink implementation with 4 unit tests
- `nes-sources/rust/nes-source-runtime/src/sink_context.rs` - SinkMessage::Flush variant added
- `nes-sources/rust/nes-source-runtime/src/sink.rs` - MockCollectorSink updated for Flush
- `nes-sources/rust/nes-source-runtime/src/sink_handle.rs` - Test match arm updated for Flush
- `nes-sources/rust/nes-source-runtime/src/lib.rs` - file_sink module and AsyncFileSink re-export
- `nes-sources/rust/nes-source-runtime/Cargo.toml` - tokio fs and io-util features
- `nes-sources/rust/nes-source-lib/src/lib.rs` - spawn_file_sink FFI function and DevNullSink Flush handling

## Decisions Made
- Added tokio `io-util` feature alongside `fs` because `AsyncWriteExt` methods (write_all, flush) require it

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added tokio io-util feature for AsyncWriteExt**
- **Found during:** Task 2 (AsyncFileSink implementation)
- **Issue:** tokio::fs::File requires io-util feature for write_all/flush methods
- **Fix:** Added "io-util" to tokio features in Cargo.toml
- **Files modified:** nes-sources/rust/nes-source-runtime/Cargo.toml
- **Verification:** cargo test passes
- **Committed in:** 102b4d0bb2 (Task 2 commit)

**2. [Rule 3 - Blocking] Updated sink_handle.rs test match arm for Flush**
- **Found during:** Task 1 (Flush variant addition)
- **Issue:** sink_handle.rs had exhaustive match on SinkMessage without Flush, causing compile error
- **Fix:** Updated match arm to use catch-all pattern with discriminant debug
- **Files modified:** nes-sources/rust/nes-source-runtime/src/sink_handle.rs
- **Verification:** cargo test passes (77 tests)
- **Committed in:** a9dbf4d509 (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (2 blocking)
**Impact on plan:** Both auto-fixes necessary for compilation. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- AsyncFileSink and spawn_file_sink ready for C++ TokioSinkRegistry integration (Plan 02)
- SinkMessage::Flush ready for C++ send_flush_to_sink bridge function (Plan 02)
- All Rust sink infrastructure complete for registration and validation phases

---
*Phase: 06-registration-and-validation*
*Completed: 2026-03-16*
