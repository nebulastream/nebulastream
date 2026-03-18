---
phase: 02-framework-and-bridge
plan: 04
subsystem: bridge
tags: [ffi, raw-pointer, tuple-buffer, cxx, bridge-loop]

# Dependency graph
requires:
  - phase: 02-framework-and-bridge/01
    provides: "BridgeMessage, bridge_loop, dispatch_message, TupleBufferHandle"
provides:
  - "TupleBufferHandle::as_raw_ptr() for borrowing raw pointer extraction"
  - "bridge_loop passing real TupleBuffer pointer to C++ emit callback"
affects: [03-source-lifecycle]

# Tech tracking
tech-stack:
  added: []
  patterns: ["Pin::get_unchecked_mut for raw pointer extraction from UniquePtr"]

key-files:
  created: []
  modified:
    - nes-sources/rust/nes-source-runtime/src/buffer.rs
    - nes-sources/rust/nes-source-runtime/src/bridge.rs

key-decisions:
  - "as_raw_ptr uses pin_mut().get_unchecked_mut() to borrow pointer without consuming UniquePtr ownership"
  - "Retain/release ordering documented as invariant: C++ retain() synchronous in callback, Rust release() at end of loop body"

patterns-established:
  - "Borrowing raw pointer from cxx UniquePtr via Pin::get_unchecked_mut for FFI handoff"
  - "Compile-time signature tests using dead helper functions to avoid CXX linker in unit tests"

requirements-completed: [BRG-01, BRG-02]

# Metrics
duration: 2min
completed: 2026-03-15
---

# Phase 2 Plan 4: Fix Bridge Null Pointer Summary

**TupleBufferHandle::as_raw_ptr() borrowing method and bridge_loop fix to pass real TupleBuffer pointer to C++ emit callback**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-15T12:51:11Z
- **Completed:** 2026-03-15T12:53:01Z
- **Tasks:** 1
- **Files modified:** 2

## Accomplishments
- Added `pub(crate) as_raw_ptr(&mut self)` to TupleBufferHandle for borrowing raw pointer extraction without consuming UniquePtr ownership
- Fixed bridge_loop to extract real buffer pointer via `msg.buffer.as_raw_ptr()` instead of `std::ptr::null_mut()`
- Documented retain/release ordering invariant ensuring C++ retains buffer before Rust drops it
- Added compile-time signature assertion test (45 tests pass, up from 44)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add as_raw_ptr to TupleBufferHandle and fix bridge_loop null pointer** - `b2a5ccae9a` (feat)

## Files Created/Modified
- `nes-sources/rust/nes-source-runtime/src/buffer.rs` - Added as_raw_ptr method and compile-time test
- `nes-sources/rust/nes-source-runtime/src/bridge.rs` - Fixed bridge_loop to pass real buffer pointer with ordering invariant docs

## Decisions Made
- Used `pin_mut().get_unchecked_mut()` to extract raw pointer from cxx UniquePtr -- this borrows without consuming ownership so Drop still calls release()
- Compile-time test uses dead helper function pattern instead of function pointer coercion to avoid pulling CXX linker symbols into unit test binary

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed compile-time test pulling CXX linker symbols**
- **Found during:** Task 1 (verification step)
- **Issue:** Original test used function pointer coercion (`let _fn: fn(...) = TupleBufferHandle::as_raw_ptr`) which forced monomorphization of the function body, pulling in CXX linker symbols and failing to link in pure Rust unit tests
- **Fix:** Changed to dead helper function pattern that type-checks the signature without monomorphizing the body
- **Files modified:** nes-sources/rust/nes-source-runtime/src/buffer.rs
- **Verification:** All 45 tests pass, cargo check --workspace passes
- **Committed in:** b2a5ccae9a (part of task commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Test approach adjusted for CXX linker constraint. No scope creep.

## Issues Encountered
None beyond the test approach adjustment documented above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Buffer pipeline now functional: Rust source tasks produce TupleBufferHandle, bridge_loop extracts raw pointer, C++ bridge_emit receives valid pointer and calls retain()
- Ready for source lifecycle integration (Phase 3)

---
*Phase: 02-framework-and-bridge*
*Completed: 2026-03-15*
