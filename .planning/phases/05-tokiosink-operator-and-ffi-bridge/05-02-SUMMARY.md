---
phase: 05-tokiosink-operator-and-ffi-bridge
plan: 02
subsystem: sources
tags: [cxx, ffi, sink, backpressure, tokio, async, rust]

# Dependency graph
requires:
  - phase: 05-01
    provides: "CXX sink FFI bridge (sink_send_buffer, stop_sink_bridge, is_sink_done_bridge, spawn_devnull_sink)"
  - phase: 04
    provides: "Rust AsyncSink trait, SinkContext, SinkTaskHandle, spawn_sink"
provides:
  - "TokioSink C++ operator with start/execute/stop implementing Sink base class"
  - "SinkBackpressureHandler with hysteresis thresholds and popFront for drain"
  - "3-phase stop state machine: DRAINING -> CLOSING -> WAITING_DONE"
  - "makeDevNullSinkSpawnFn() factory for test sink creation"
  - "Integration tests covering PLN-01 through PLN-04"
affects: [06-registration-and-validation]

# Tech tracking
tech-stack:
  added: []
  patterns: [heap-allocated-buffer-ownership-transfer, 3-phase-stop-state-machine, sink-backpressure-hysteresis]

key-files:
  created:
    - nes-sources/include/Sources/TokioSink.hpp
    - nes-sources/src/TokioSink.cpp
    - nes-sources/tests/TokioSinkTest.cpp
  modified:
    - nes-sources/src/CMakeLists.txt
    - nes-sources/tests/CMakeLists.txt
    - nes-sources/rust/nes-source-bindings/src/IoBindings.cpp
    - nes-sources/tests/TokioSourceTest.cpp

key-decisions:
  - "Heap-allocate TupleBuffer copy for Rust ownership transfer instead of stack pointer with retain/release"
  - "SinkBackpressureHandler duplicated from NetworkSink to avoid cross-module dependency"
  - "StopPhase enum for 3-phase state machine: RUNNING -> DRAINING -> CLOSING -> WAITING_DONE"
  - "Static atomic counter for sinkId assignment (Phase 6 registry will provide real IDs)"
  - "CXX SendResult enum is at global scope (::SendResult) not in ffi_sink namespace"

patterns-established:
  - "Heap-allocated buffer for CXX FFI sink ownership: new TupleBuffer(buf) -> Rust UniquePtr::from_raw, delete on Full/Closed"
  - "3-phase stop with repeatTask retry: drain backlog -> send Close -> poll is_sink_done"
  - "makeDevNullSinkSpawnFn() factory pattern for test sinks (mirrors makeGeneratorSpawnFn for sources)"

requirements-completed: [PLN-01, PLN-02, PLN-03, PLN-04]

# Metrics
duration: 13min
completed: 2026-03-16
---

# Phase 5 Plan 02: TokioSink C++ Operator Summary

**TokioSink C++ operator with backpressure hysteresis, non-blocking CXX bridge execute(), 3-phase stop state machine, and 5 passing integration tests**

## Performance

- **Duration:** 13 min
- **Started:** 2026-03-16T16:25:47Z
- **Completed:** 2026-03-16T16:39:18Z
- **Tasks:** 3
- **Files modified:** 7

## Accomplishments
- TokioSink implements Sink (ExecutablePipelineStage) with complete start/execute/stop lifecycle
- execute() sends buffers via sink_send_buffer CXX bridge with backpressure hysteresis and repeatTask retry
- stop() uses 3-phase state machine (DRAINING -> CLOSING -> WAITING_DONE) for clean flush and completion
- All 5 integration tests pass: BasicLifecycle, BackpressureRetry, HysteresisThresholds, FlushOnStop, ExecuteAfterStopDropsBuffer
- All 73 Phase 4 Rust tests still pass

## Task Commits

Each task was committed atomically:

1. **Task 1: TokioSink header** - `ee381de655` (feat)
2. **Task 2: TokioSink.cpp implementation + CMake** - `68bdf7709f` (feat)
3. **Task 3: TokioSinkTest.cpp integration tests** - `40dfea0f08` (feat)

## Files Created/Modified
- `nes-sources/include/Sources/TokioSink.hpp` - TokioSink class with SinkBackpressureHandler, SpawnFn, StopPhase enum
- `nes-sources/src/TokioSink.cpp` - Full implementation: backpressure execute(), 3-phase stop(), makeDevNullSinkSpawnFn()
- `nes-sources/tests/TokioSinkTest.cpp` - 5 integration tests with MockPipelineContext covering PLN-01 through PLN-04
- `nes-sources/src/CMakeLists.txt` - Added TokioSink.cpp to source list
- `nes-sources/tests/CMakeLists.txt` - Added tokio-sink-test target, fixed link target name
- `nes-sources/rust/nes-source-bindings/src/IoBindings.cpp` - Fixed include path (nes-io-bindings -> nes-source-bindings)
- `nes-sources/tests/TokioSourceTest.cpp` - Fixed include path (nes-io-bindings -> nes-source-bindings)

## Decisions Made
- **Heap-allocated buffer ownership transfer:** Plan specified retain/release with stack pointer, but this causes use-after-free when Rust holds a UniquePtr to a destroyed stack-local TupleBuffer. Changed to `new TupleBuffer(inputBuffer)` heap allocation. On Success Rust owns the heap copy; on Full/Closed C++ deletes it (since Rust calls std::mem::forget).
- **CXX SendResult namespace:** Plan referenced `::ffi_sink::SendResult` but CXX generates the enum at global scope as `::SendResult`. Fixed in implementation.
- **CMake target name:** Plan 05-01 renamed the crate to nes-io-bindings but the CMake target is also `nes-io-bindings`, not `nes-source-bindings`. Tests link `nes-io-bindings`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed buffer ownership transfer from stack pointer to heap allocation**
- **Found during:** Task 2 (TokioSink.cpp implementation)
- **Issue:** Plan specified passing `&*currentBuffer` (stack pointer) to Rust with retain/release. Rust wraps in UniquePtr::from_raw which would try to `delete` a stack pointer when eventually dropped -- undefined behavior.
- **Fix:** Changed to `new TupleBuffer(*currentBuffer)` for heap allocation. On Success: Rust owns heap copy. On Full/Closed: C++ deletes heap copy (Rust called std::mem::forget).
- **Files modified:** nes-sources/src/TokioSink.cpp
- **Verification:** All 5 TokioSinkTest cases pass without memory errors
- **Committed in:** 68bdf7709f (Task 2 commit)

**2. [Rule 3 - Blocking] Fixed nes-io-bindings/lib.h include path**
- **Found during:** Task 3 (TokioSinkTest build)
- **Issue:** Plan 05-01 renamed the crate but CXX-generated header is still at `nes-source-bindings/lib.h`. Three files used the old path.
- **Fix:** Changed `#include <nes-io-bindings/lib.h>` to `#include <nes-source-bindings/lib.h>` in IoBindings.cpp, TokioSourceTest.cpp, TokioSinkTest.cpp
- **Files modified:** nes-sources/rust/nes-source-bindings/src/IoBindings.cpp, nes-sources/tests/TokioSourceTest.cpp, nes-sources/tests/TokioSinkTest.cpp
- **Verification:** Build succeeds, all TokioSinkTest cases pass
- **Committed in:** 40dfea0f08 (Task 3 commit)

**3. [Rule 3 - Blocking] Fixed CMake target name from nes-source-bindings to nes-io-bindings**
- **Found during:** Task 3 (TokioSinkTest link)
- **Issue:** Plan 05-01's CMake target is `nes-io-bindings`, test CMakeLists referenced `nes-source-bindings`
- **Fix:** Changed `target_link_libraries(tokio-sink-test nes-source-bindings)` to `nes-io-bindings`
- **Files modified:** nes-sources/tests/CMakeLists.txt
- **Verification:** Link succeeds, test executable produced
- **Committed in:** 40dfea0f08 (Task 3 commit)

**4. [Rule 1 - Bug] Fixed CXX SendResult namespace**
- **Found during:** Task 2 (TokioSink.cpp build)
- **Issue:** Plan referenced `::ffi_sink::SendResult::Success` etc. but CXX generates enum at global scope `::SendResult`
- **Fix:** Changed all `::ffi_sink::SendResult::X` to `::SendResult::X`
- **Files modified:** nes-sources/src/TokioSink.cpp
- **Verification:** Build succeeds
- **Committed in:** 40dfea0f08 (Task 3 commit, amendments during build iteration)

**5. [Rule 1 - Bug] Fixed HysteresisThresholds test assertion logic**
- **Found during:** Task 3 (test execution)
- **Issue:** Plan's test expected 3 onSuccess calls to drain, but with 3 buffered items plus clearing pending, 4 calls are needed
- **Fix:** Added 4th onSuccess call (returns nullopt) and corrected the assertion order
- **Files modified:** nes-sources/tests/TokioSinkTest.cpp
- **Verification:** HysteresisThresholds test passes
- **Committed in:** 40dfea0f08 (Task 3 commit)

---

**Total deviations:** 5 auto-fixed (2 bugs, 3 blocking)
**Impact on plan:** All fixes necessary for correctness and build. No scope creep. Core design unchanged.

## Issues Encountered
- Pre-existing TokioSourceTest failures (GeneratorEmitsIncrementingValues, BackpressureStability) caused by Plan 05-01 in-progress changes to generator source format. Out of scope for Plan 05-02.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- TokioSink operator complete with full lifecycle, backpressure, and flush protocol
- Phase 5 complete -- ready for Phase 6 (registration and validation)
- Pre-existing TokioSourceTest failures should be addressed in Phase 6 or as a follow-up

---
*Phase: 05-tokiosink-operator-and-ffi-bridge*
*Completed: 2026-03-16*
