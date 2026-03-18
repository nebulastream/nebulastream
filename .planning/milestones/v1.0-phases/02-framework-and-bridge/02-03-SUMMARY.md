---
phase: 02-framework-and-bridge
plan: 03
subsystem: runtime
tags: [tokio-source, source-handle, variant, ffi, bridge-emit, error-callback, cxx, pimpl]

# Dependency graph
requires:
  - phase: 02-framework-and-bridge
    plan: 02
    provides: AsyncSource trait, SourceTaskHandle, spawn_source, stop_source, Phase 2 CXX bridge in nes-source-lib
  - phase: 01-ffi-foundation
    provides: TupleBufferHandle, BufferProviderHandle, init_source_runtime, SourceBindings
provides:
  - TokioSource C++ class with start/stop/tryStop/getSourceId matching SourceThread interface
  - SourceHandle std::variant dispatch to either SourceThread or TokioSource
  - EmitContext and ErrorContext structs for C-style FFI callbacks
  - bridge_emit C-linkage callback (Rust bridge thread -> C++ emit dispatch)
  - on_source_error_callback C-linkage callback (Rust monitoring task -> C++ error logging)
  - PIMPL pattern hiding rust::Box from public C++ headers
affects: [phase-3, source-implementations, query-engine-integration]

# Tech tracking
tech-stack:
  added: []
  patterns: [PIMPL for rust::Box in C++ public headers, std::variant + std::visit for SourceHandle dispatch, Overloaded{} helper for multi-type visit, C-linkage callbacks for Rust->C++ emit/error dispatch]

key-files:
  created:
    - nes-sources/include/Sources/TokioSource.hpp
    - nes-sources/src/TokioSource.cpp
  modified:
    - nes-sources/include/Sources/SourceHandle.hpp
    - nes-sources/src/SourceHandle.cpp
    - nes-sources/rust/nes-source-bindings/include/SourceBindings.hpp
    - nes-sources/rust/nes-source-bindings/src/SourceBindings.cpp
    - nes-sources/src/CMakeLists.txt
    - nes-sources/rust/nes-source-bindings/CMakeLists.txt

key-decisions:
  - "PIMPL pattern for rust::Box<SourceHandle>: TokioSource.hpp forward-declares RustHandleImpl to avoid exposing CXX types in public C++ headers"
  - "Corrosion FILES list expanded to include nes-source-lib/src/lib.rs for Phase 2 CXX bridge header generation"
  - "bridge_emit retains TupleBuffer before creating C++ copy to take ownership from Rust bridge thread"
  - "tryStop returns SUCCESS immediately since Rust sources stop asynchronously via CancellationToken"

patterns-established:
  - "PIMPL for CXX types: wrap rust::Box<T> in a private struct to keep CXX headers out of public API"
  - "std::variant + Overloaded{} for SourceHandle: dispatch to SourceThread or TokioSource without modifying RunningSource/QueryEngine"
  - "C-linkage callback pattern: EmitContext/ErrorContext structs as void* context for Rust->C++ callbacks"

requirements-completed: [BRG-04]

# Metrics
duration: 9min
completed: 2026-03-15
---

# Phase 02 Plan 03: TokioSource C++ Class and SourceHandle Variant Summary

**TokioSource C++ wrapper with PIMPL for rust::Box, SourceHandle std::variant dispatch, and bridge_emit/error_callback C-linkage FFI**

## Performance

- **Duration:** 9 min
- **Started:** 2026-03-15T11:38:50Z
- **Completed:** 2026-03-15T11:47:50Z
- **Tasks:** 2
- **Files modified:** 8

## Accomplishments
- TokioSource class wraps Rust async source lifecycle (spawn_source/stop_source FFI) behind SourceThread-compatible interface
- SourceHandle uses std::variant to hold either SourceThread or TokioSource, dispatching via std::visit
- EmitContext and ErrorContext structs provide C-linkage callback contexts for Rust bridge thread and monitoring task
- bridge_emit callback retains buffer and calls C++ EmitFunction; on_source_error_callback logs errors via NES_ERROR
- PIMPL pattern hides rust::Box<SourceHandle> from public C++ headers
- RunningSource and QueryEngine remain completely unchanged
- All 44 Rust tests pass

## Task Commits

Each task was committed atomically:

1. **Task 1: Expand C++ SourceBindings with Phase 2 FFI wrapper functions** - `ff725063ce` (feat)
2. **Task 2: TokioSource C++ class and SourceHandle variant update** - `0e946ff27e` (feat)

## Files Created/Modified
- `nes-sources/include/Sources/TokioSource.hpp` - TokioSource class declaration with PIMPL for Rust handle
- `nes-sources/src/TokioSource.cpp` - TokioSource implementation calling spawn_source/stop_source FFI
- `nes-sources/include/Sources/SourceHandle.hpp` - Updated with std::variant and TokioSource constructor
- `nes-sources/src/SourceHandle.cpp` - std::visit dispatch with Overloaded{} pattern
- `nes-sources/rust/nes-source-bindings/include/SourceBindings.hpp` - EmitContext, ErrorContext, bridge_emit, on_source_error_callback declarations
- `nes-sources/rust/nes-source-bindings/src/SourceBindings.cpp` - bridge_emit and on_source_error_callback implementations
- `nes-sources/src/CMakeLists.txt` - Added TokioSource.cpp to build
- `nes-sources/rust/nes-source-bindings/CMakeLists.txt` - Added nes-source-lib/src/lib.rs to Corrosion FILES

## Decisions Made
- Used PIMPL pattern (RustHandleImpl) to hide rust::Box<SourceHandle> from public TokioSource.hpp header, avoiding CXX type exposure in nes-sources public API
- Expanded Corrosion FILES list to include nes-source-lib/src/lib.rs so Phase 2 CXX bridge generates C++ headers
- bridge_emit retains the raw TupleBuffer pointer before creating a C++ TupleBuffer copy, ensuring proper ownership transfer from Rust
- tryStop() returns SUCCESS immediately since Rust async sources stop cooperatively via CancellationToken (no blocking wait needed)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Corrosion CMake FILES list missing Phase 2 bridge**
- **Found during:** Task 2 (TokioSource.cpp needs CXX-generated header)
- **Issue:** Corrosion FILES only listed nes-source-bindings/lib.rs (Phase 1). Phase 2 CXX bridge in nes-source-lib/src/lib.rs had no generated C++ header.
- **Fix:** Added `../nes-source-lib/src/lib.rs` to the FILES parameter in nes-source-bindings/CMakeLists.txt
- **Files modified:** nes-sources/rust/nes-source-bindings/CMakeLists.txt
- **Committed in:** 0e946ff27e

**2. [Rule 3 - Blocking] rust::Box cannot be forward-declared in public C++ header**
- **Found during:** Task 2 (TokioSource.hpp design)
- **Issue:** Plan specified `rust::Box<SourceTaskHandle> rustHandle` as TokioSource member, but this requires including CXX headers (rust/cxx.h) in the public nes-sources API header, creating an undesirable build dependency.
- **Fix:** Used PIMPL pattern with `struct RustHandleImpl` that wraps `rust::Box<SourceHandle>` internally. TokioSource.hpp only exposes `std::unique_ptr<RustHandleImpl>`.
- **Files modified:** TokioSource.hpp, TokioSource.cpp
- **Committed in:** 0e946ff27e

---

**Total deviations:** 2 auto-fixed (2 blocking)
**Impact on plan:** Both deviations were necessary build system and API design adaptations. All planned functionality implemented. No scope creep.

## Issues Encountered
- SourceThread uses `getOriginId()` while TokioSource uses `getSourceId()` -- the Overloaded{} pattern in SourceHandle::getSourceId() dispatches to the correct method name for each variant.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- TokioSource class complete and ready for use in SourceProvider (Phase 3)
- SourceHandle variant dispatch transparent to RunningSource and QueryEngine
- PlaceholderSource in spawn_source ready to be replaced with real source dispatch
- Full C++ compilation requires CMake build (validated in Phase 3 integration)

---
*Phase: 02-framework-and-bridge*
*Completed: 2026-03-15*
