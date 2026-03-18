---
phase: 06-registration-and-validation
plan: 02
subsystem: sinks
tags: [registry, tokio, cxx, cmake, backpressure, factory-pattern]

# Dependency graph
requires:
  - phase: 06-01
    provides: "AsyncFileSink, spawn_file_sink CXX FFI, SinkMessage::Flush"
  - phase: 05-ffi-bridge-and-operator
    provides: "TokioSink C++ operator, CXX bridge FFI"
provides:
  - "TokioSinkRegistry BaseRegistry specialization for Rust async sinks"
  - "TokioFileSink factory registered as 'TokioFileSink' in TokioSinkRegistry"
  - "SinkProvider class with TokioSinkRegistry pre-check routing"
  - "makeFileSinkSpawnFn factory helper calling spawn_file_sink FFI"
  - "WorkerConfiguration.defaultChannelCapacity for Tokio sink channel sizing"
affects: [06-03, systests]

# Tech tracking
tech-stack:
  added: []
  patterns: [TokioSinkRegistry factory pattern, SinkProvider class with registry routing]

key-files:
  created:
    - nes-sinks/registry/include/TokioSinkRegistry.hpp
    - nes-sinks/registry/templates/TokioSinkGeneratedRegistrar.inc.in
    - nes-sinks/src/TokioFileSink.cpp
  modified:
    - nes-sinks/CMakeLists.txt
    - nes-sinks/src/CMakeLists.txt
    - nes-sinks/src/SinkProvider.cpp
    - nes-sinks/include/Sinks/SinkProvider.hpp
    - nes-sources/src/TokioSink.cpp
    - nes-sources/include/Sources/TokioSink.hpp
    - nes-runtime/interface/Configuration/WorkerConfiguration.hpp

key-decisions:
  - "TokioSinkRegistryReturnType is std::unique_ptr<Sink> (base class) to avoid cross-module dependency on TokioSink.hpp"
  - "TokioSinkRegistryArguments has exactly 2 fields: {SinkDescriptor, channelCapacity} -- no BackpressureController"
  - "Factory creates disconnected BackpressureController via createBackpressureChannel() since TokioSink manages own backpressure"
  - "Free function lower() kept as compatibility wrapper delegating to SinkProvider class"
  - "nes-sources added as PRIVATE dependency of nes-sinks for TokioFileSink.cpp compilation"

patterns-established:
  - "TokioSinkRegistry factory pattern: mirrors TokioSourceRegistry with add_plugin CMake macros"
  - "SinkProvider class pattern: mirrors SourceProvider with TokioSinkRegistry pre-check"

requirements-completed: [REG-01]

# Metrics
duration: 5min
completed: 2026-03-16
---

# Phase 6 Plan 2: Sink Registry and SinkProvider Summary

**TokioSinkRegistry with BaseRegistry pattern, TokioFileSink factory registration, and SinkProvider class routing Tokio sinks through spawn_file_sink FFI**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-16T18:12:43Z
- **Completed:** 2026-03-16T18:17:50Z
- **Tasks:** 2
- **Files modified:** 11

## Accomplishments
- TokioSinkRegistry created with BaseRegistry pattern, TokioSinkRegistryArguments has exactly {SinkDescriptor, channelCapacity}
- TokioFileSink factory registered in TokioSinkRegistry and SinkValidationRegistry via CMake add_plugin macros
- SinkProvider converted from free function to class with TokioSinkRegistry pre-check before standard SinkRegistry
- makeFileSinkSpawnFn factory helper creates SpawnFn that calls spawn_file_sink CXX FFI
- WorkerConfiguration gained defaultChannelCapacity (default 64) for Tokio async sink channel sizing
- Full C++ build and all TokioSink tests pass

## Task Commits

Each task was committed atomically:

1. **Task 1: Create TokioSinkRegistry, generated registrar template, and WorkerConfiguration field** - `9704d1dcaa` (feat)
2. **Task 2: TokioFileSink factory, makeFileSinkSpawnFn, SinkProvider integration** - `7706104394` (feat)

## Files Created/Modified
- `nes-sinks/registry/include/TokioSinkRegistry.hpp` - BaseRegistry specialization for Tokio sinks with {SinkDescriptor, channelCapacity} arguments
- `nes-sinks/registry/templates/TokioSinkGeneratedRegistrar.inc.in` - Code generation template for TokioSink registrations
- `nes-sinks/src/TokioFileSink.cpp` - TokioFileSink factory + validation registration, ConfigParametersTokioFileSink with FILE_PATH
- `nes-sinks/CMakeLists.txt` - Added TokioSink to create_registries_for_component, nes-sources as PRIVATE dep
- `nes-sinks/src/CMakeLists.txt` - add_plugin calls for TokioFileSink TokioSink and SinkValidation
- `nes-sinks/src/SinkProvider.cpp` - SinkProvider class with TokioSinkRegistry pre-check + compatibility wrapper
- `nes-sinks/include/Sinks/SinkProvider.hpp` - SinkProvider class declaration + free function compatibility wrapper
- `nes-sources/src/TokioSink.cpp` - makeFileSinkSpawnFn implementation calling spawn_file_sink FFI
- `nes-sources/include/Sources/TokioSink.hpp` - makeFileSinkSpawnFn declaration
- `nes-runtime/interface/Configuration/WorkerConfiguration.hpp` - defaultChannelCapacity UIntOption (default 64)

## Decisions Made
- TokioSinkRegistryReturnType uses `std::unique_ptr<Sink>` (not TokioSink) to avoid nes-sinks depending on TokioSink.hpp
- BackpressureController excluded from TokioSinkRegistryArguments per plan -- factory uses createBackpressureChannel() to create a disconnected controller since TokioSink manages own backpressure via Rust channel
- Kept free function `lower()` as compatibility wrapper to avoid refactoring all existing callers (ExecutableQueryPlan.cpp)
- Added nes-sources as PRIVATE dependency of nes-sinks -- no circular dependency since nes-sources does not depend on nes-sinks

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] BackpressureController has no default constructor**
- **Found during:** Task 2 (TokioFileSink factory)
- **Issue:** Plan specified `BackpressureController{}` but BackpressureController's constructor is private (only via createBackpressureChannel)
- **Fix:** Used `createBackpressureChannel()` to create a disconnected controller, discarding the listener
- **Files modified:** nes-sinks/src/TokioFileSink.cpp
- **Verification:** nes-sinks build succeeds
- **Committed in:** 7706104394 (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Minimal -- same semantics (unused backpressure controller), different construction method.

## Issues Encountered
None beyond the BackpressureController constructor issue documented above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- TokioSinkRegistry and TokioFileSink factory complete, ready for systest integration in 06-03
- SinkProvider routes "TokioFileSink" descriptors to Rust async sink path
- WorkerConfiguration provides defaultChannelCapacity for deployment tuning

---
*Phase: 06-registration-and-validation*
*Completed: 2026-03-16*
