---
phase: 05-tokiosink-operator-and-ffi-bridge
plan: 01
subsystem: ffi
tags: [cxx, rust, ffi, sink, rename, tokio]

# Dependency graph
requires:
  - phase: 04-rust-sink-framework
    provides: SinkTaskHandle, spawn_sink, AsyncSink trait, SinkContext, SinkMessage
provides:
  - "Renamed nes-io-bindings crate (was nes-source-bindings)"
  - "ffi_sink CXX bridge module with SinkHandle, SendResult, sink_send_buffer, stop_sink_bridge, is_sink_done_bridge, spawn_devnull_sink"
  - "on_sink_error_callback C++ implementation"
  - "DevNullSink for testing TokioSink operator mechanics"
affects: [05-02, 06-registry]

# Tech tracking
tech-stack:
  added: [async-channel (nes_source_lib)]
  patterns: [sink CXX bridge mirroring source pattern, std::mem::forget for buffer ownership on Full/Closed]

key-files:
  created:
    - nes-sources/rust/nes-source-bindings/include/IoBindings.hpp
    - nes-sources/rust/nes-source-bindings/src/IoBindings.cpp
  modified:
    - nes-sources/rust/nes-source-lib/src/lib.rs
    - nes-sources/rust/nes-source-lib/Cargo.toml
    - nes-sources/rust/nes-source-bindings/Cargo.toml
    - nes-sources/rust/nes-source-bindings/lib.rs
    - nes-sources/rust/nes-source-bindings/CMakeLists.txt
    - nes-sources/rust/nes-source-runtime/Cargo.toml
    - nes-sources/rust/nes-source-runtime/src/buffer.rs
    - nes-sources/rust/nes-source-runtime/src/runtime.rs
    - nes-sources/rust/nes-source-runtime/src/bridge.rs
    - nes-sources/rust/nes-source-runtime/src/source.rs
    - nes-sources/CMakeLists.txt
    - nes-sources/src/TokioSource.cpp
    - nes-sources/tests/TokioSourceTest.cpp

key-decisions:
  - "Used UniquePtr::from_raw for buffer ownership transfer (C++ retains, Rust wraps, forget on Full/Closed)"
  - "Added async-channel as direct dependency to nes_source_lib for TrySendError types"
  - "on_sink_error_callback reuses ErrorContext struct from source side"

patterns-established:
  - "Sink FFI pattern: SinkHandle wrapper + ffi_sink CXX bridge, mirroring source pattern"
  - "Buffer ownership: C++ retains before send, Rust forget on rejection, C++ releases on rejection"

requirements-completed: [PLN-01, PLN-02, PLN-04]

# Metrics
duration: 5min
completed: 2026-03-16
---

# Phase 5 Plan 01: Crate Rename and Sink FFI Bridge Summary

**Renamed nes-source-bindings to nes-io-bindings and added ffi_sink CXX bridge with SinkHandle, SendResult enum, and buffer ownership semantics using mem::forget**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-16T16:18:20Z
- **Completed:** 2026-03-16T16:23:10Z
- **Tasks:** 2
- **Files modified:** 16

## Accomplishments
- Renamed nes-source-bindings crate to nes-io-bindings across all Cargo.toml, CMakeLists.txt, Rust imports, and C++ includes
- Added ffi_sink CXX bridge module with SendResult enum, SinkHandle opaque type, and 4 FFI wrapper functions
- Implemented sink_send_buffer with correct buffer ownership (UniquePtr::from_raw + std::mem::forget on Full/Closed)
- Added DevNullSink and spawn_devnull_sink for Plan 02 TokioSink operator testing
- All 73 existing Rust tests pass unchanged

## Task Commits

Each task was committed atomically:

1. **Task 1: Rename nes-source-bindings crate to nes-io-bindings** - `0431cdf890` (refactor)
2. **Task 2: Add sink CXX bridge with SinkHandle, SendResult, and FFI wrappers** - `faa97deb31` (feat)

## Files Created/Modified
- `nes-sources/rust/nes-source-bindings/include/IoBindings.hpp` - Renamed from SourceBindings.hpp, added on_sink_error_callback
- `nes-sources/rust/nes-source-bindings/src/IoBindings.cpp` - Renamed from SourceBindings.cpp, added on_sink_error_callback impl
- `nes-sources/rust/nes-source-lib/src/lib.rs` - Added ffi_sink module, SinkHandle, DevNullSink, FFI wrappers
- `nes-sources/rust/nes-source-lib/Cargo.toml` - Added async-channel dependency
- `nes-sources/rust/nes-source-bindings/Cargo.toml` - Renamed crate to nes_io_bindings
- `nes-sources/rust/nes-source-bindings/CMakeLists.txt` - Updated target from nes-source-bindings to nes-io-bindings
- `nes-sources/CMakeLists.txt` - Updated link target to nes-io-bindings
- `nes-sources/src/TokioSource.cpp` - Updated include to IoBindings.hpp
- `nes-sources/tests/TokioSourceTest.cpp` - Updated includes to IoBindings.hpp and nes-io-bindings/lib.h

## Decisions Made
- Used UniquePtr::from_raw for buffer ownership transfer (matching locked CONTEXT.md decision: C++ retains, passes raw ptr, Rust wraps)
- std::mem::forget on Full/Closed prevents TupleBufferHandle::Drop from calling release(), leaving C++ to undo its own retain
- Added async-channel as direct dependency to nes_source_lib (needed for TrySendError types in sink_send_buffer)
- on_sink_error_callback reuses the same ErrorContext struct as sources (sourceId field serves as generic ID)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added async-channel dependency to nes_source_lib**
- **Found during:** Task 2 (sink CXX bridge implementation)
- **Issue:** async_channel::TrySendError used in sink_send_buffer but async-channel not in nes_source_lib Cargo.toml
- **Fix:** Added `async-channel = "2.3.1"` to nes-source-lib/Cargo.toml dependencies
- **Files modified:** nes-sources/rust/nes-source-lib/Cargo.toml
- **Verification:** cargo check -p nes_source_lib succeeds
- **Committed in:** faa97deb31 (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Missing dependency was a straightforward addition. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- ffi_sink CXX bridge surface is complete and ready for Plan 02 (C++ TokioSink operator)
- spawn_devnull_sink available for C++ integration tests
- IoBindings.hpp has on_sink_error_callback declaration ready for TokioSink to use

---
*Phase: 05-tokiosink-operator-and-ffi-bridge*
*Completed: 2026-03-16*
