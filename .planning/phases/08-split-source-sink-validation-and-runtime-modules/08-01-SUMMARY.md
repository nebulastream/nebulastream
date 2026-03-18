---
phase: 08-split-source-sink-validation-and-runtime-modules
plan: 01
subsystem: build
tags: [cmake, c++, validation, source-sink, wasm]

# Dependency graph
requires:
  - phase: 06-validation-extraction
    provides: nes-validator CMake target and WASM build with source_validation_stubs.cpp
provides:
  - nes-sources-validation CMake target (validation-only, no runtime deps)
  - nes-sinks-validation CMake target (validation-only, no runtime deps)
  - Validation code extracted for all 9 source/sink types
affects: [08-02-wasm-real-validation, nes-validator, wasm-build]

# Tech tracking
tech-stack:
  added: []
  patterns: [validation-file-split, plugin-registry-split, validation-only-cmake-target]

key-files:
  created:
    - nes-sources/include/Sources/FileSourceValidation.hpp
    - nes-sources/src/FileSourceValidation.cpp
    - nes-sinks/include/Sinks/FileSinkValidation.hpp
    - nes-sinks/include/Sinks/PrintSinkValidation.hpp
    - nes-sinks/src/FileSinkValidation.cpp
    - nes-sinks/src/PrintSinkValidation.cpp
    - nes-plugins/Sources/TCPSource/TCPSourceValidation.hpp
    - nes-plugins/Sources/TCPSource/TCPSourceValidation.cpp
    - nes-plugins/Sources/GeneratorSource/GeneratorSourceValidation.hpp
    - nes-plugins/Sources/GeneratorSource/GeneratorSourceValidation.cpp
    - nes-plugins/Sources/NetworkSource/NetworkSourceValidation.hpp
    - nes-plugins/Sources/NetworkSource/NetworkSourceValidation.cpp
    - nes-plugins/Sinks/NetworkSink/NetworkSinkValidation.hpp
    - nes-plugins/Sinks/NetworkSink/NetworkSinkValidation.cpp
    - nes-plugins/Sinks/ChecksumSink/ChecksumSinkValidation.hpp
    - nes-plugins/Sinks/ChecksumSink/ChecksumSinkValidation.cpp
    - nes-plugins/Sinks/VoidSink/VoidSinkValidation.hpp
    - nes-plugins/Sinks/VoidSink/VoidSinkValidation.cpp
  modified:
    - nes-sources/CMakeLists.txt
    - nes-sinks/CMakeLists.txt
    - nes-sources/src/FileSource.cpp
    - nes-sinks/src/FileSink.cpp
    - nes-sinks/src/PrintSink.cpp
    - nes-sinks/include/Sinks/FileSink.hpp
    - nes-sinks/include/Sinks/PrintSink.hpp
    - nes-plugins/Sources/TCPSource/TCPSource.hpp
    - nes-plugins/Sources/TCPSource/TCPSource.cpp
    - nes-plugins/Sources/TCPSource/CMakeLists.txt
    - nes-plugins/Sources/GeneratorSource/GeneratorSource.hpp
    - nes-plugins/Sources/GeneratorSource/GeneratorSource.cpp
    - nes-plugins/Sources/GeneratorSource/CMakeLists.txt
    - nes-plugins/Sources/NetworkSource/NetworkSource.hpp
    - nes-plugins/Sources/NetworkSource/NetworkSource.cpp
    - nes-plugins/Sources/NetworkSource/CMakeLists.txt
    - nes-plugins/Sinks/NetworkSink/NetworkSink.hpp
    - nes-plugins/Sinks/NetworkSink/NetworkSink.cpp
    - nes-plugins/Sinks/NetworkSink/CMakeLists.txt
    - nes-plugins/Sinks/ChecksumSink/ChecksumSink.hpp
    - nes-plugins/Sinks/ChecksumSink/ChecksumSink.cpp
    - nes-plugins/Sinks/ChecksumSink/CMakeLists.txt
    - nes-plugins/Sinks/VoidSink/VoidSink.hpp
    - nes-plugins/Sinks/VoidSink/VoidSink.cpp
    - nes-plugins/Sinks/VoidSink/CMakeLists.txt

key-decisions:
  - "Added nes-data-types to validation targets -- SourceDescriptor.hpp and SinkDescriptor.hpp include DataTypes/Schema.hpp transitively"
  - "Used create_plugin_registry_library + generate_plugin_registrars instead of create_registries_for_component for validation targets to avoid directory-name coupling"
  - "Deleted nes-sources/private/FileSource.hpp -- class definition moved inline to FileSource.cpp"

patterns-established:
  - "Validation file split: ConfigParameters struct + RegisterXxxValidation in *Validation.hpp/.cpp, runtime class stays in original file"
  - "Validation-only CMake target: add_library + create_plugin_registry_library + generate_plugin_registrars in parent CMakeLists.txt"
  - "Plugin validation registry wiring: add_plugin_as_library third param is nes-*-validation-registry for SinkValidation/SourceValidation plugins"

requirements-completed: [VLIB-01]

# Metrics
duration: 18min
completed: 2026-03-16
---

# Phase 8 Plan 1: Split Source/Sink Validation and Runtime Modules Summary

**Extracted ConfigParameters structs and RegisterXxxValidation functions into nes-sources-validation and nes-sinks-validation CMake targets covering all 9 source/sink types**

## Performance

- **Duration:** 18 min
- **Started:** 2026-03-16T15:35:54Z
- **Completed:** 2026-03-16T15:54:17Z
- **Tasks:** 2
- **Files modified:** 43

## Accomplishments
- Created nes-sources-validation CMake target compiling independently with nes-common + nes-configurations + nes-data-types
- Created nes-sinks-validation CMake target compiling independently with nes-common + nes-configurations + nes-data-types
- Extracted validation for all 4 source types (File, TCP, Generator, Network) and all 5 sink types (File, Print, Network, Checksum, Void)
- nes-sources and nes-sinks link validation targets PUBLIC, inheriting registrations
- Full project builds with no errors, all 14 relevant tests pass
- Deleted nes-sources/private/FileSource.hpp (class moved inline to FileSource.cpp)

## Task Commits

Each task was committed atomically:

1. **Task 1: Extract source validation files and create nes-sources-validation** - `d6627b0598` (feat)
2. **Task 2: Extract sink validation files and create nes-sinks-validation** - `38382544f7` (feat)

## Decisions Made

1. **Added nes-data-types dependency to validation targets** -- SourceDescriptor.hpp and SinkDescriptor.hpp both include DataTypes/Schema.hpp. Without nes-data-types, the validation targets cannot compile. This is a minimal additional dependency (nes-data-types links only nes-configurations + reflectcpp).

2. **Used lower-level CMake macros for validation targets** -- Used create_plugin_registry_library + generate_plugin_registrars instead of create_registries_for_component, because the latter derives COMPONENT_NAME from directory name which would give "nes-sources" for both targets.

3. **Deleted private FileSource.hpp** -- Per locked decision in CONTEXT.md. The class definition is only needed by the .cpp file's runtime methods.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added nes-data-types to validation target dependencies**
- **Found during:** Task 1 (nes-sources-validation compilation)
- **Issue:** SourceDescriptor.hpp includes DataTypes/Schema.hpp which requires nes-data-types; validation targets failed to compile with only nes-common + nes-configurations
- **Fix:** Added nes-data-types to target_link_libraries for both nes-sources-validation and nes-sinks-validation
- **Files modified:** nes-sources/CMakeLists.txt, nes-sinks/CMakeLists.txt
- **Verification:** Both validation targets compile successfully
- **Committed in:** d6627b0598 (Task 1), 38382544f7 (Task 2)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** nes-data-types is a lightweight dependency (Schema + DataType), necessary because SourceDescriptor.hpp and SinkDescriptor.hpp include Schema.hpp. The validation targets still avoid heavy runtime deps (nes-memory, nes-executable, folly, network).

## Issues Encountered
None -- build and tests passed on first attempt after adding nes-data-types.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- nes-sources-validation and nes-sinks-validation are ready for WASM build integration
- WASM CMakeLists.txt can replace source_validation_stubs.cpp with real validation .cpp files
- All plugin validation files are in separate .cpp files ready for cherry-picking into WASM build

## Self-Check: PASSED

- All 14 validation files created and exist on disk
- nes-sources/private/FileSource.hpp confirmed deleted
- Both task commits (d6627b0598, 38382544f7) exist in git log
- Full project build succeeds (405 targets)
- 14/14 relevant tests pass

---
*Phase: 08-split-source-sink-validation-and-runtime-modules*
*Completed: 2026-03-16*
