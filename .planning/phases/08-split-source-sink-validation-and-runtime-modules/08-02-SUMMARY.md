---
phase: 08-split-source-sink-validation-and-runtime-modules
plan: 02
subsystem: build
tags: [cmake, c++, validation, wasm, source-sink, unit-tests]

# Dependency graph
requires:
  - phase: 08-split-source-sink-validation-and-runtime-modules
    plan: 01
    provides: nes-sources-validation and nes-sinks-validation CMake targets with extracted validation .cpp files
provides:
  - WASM build using real validation code (File, TCP, Network sources; all 5 sink types)
  - Native nes-validator explicitly linking nes-sources-validation and nes-sinks-validation
  - Unit tests proving config field validation works for File and TCP sources
affects: [wasm-build, topology-editor, nes-validator]

# Tech tracking
tech-stack:
  added: []
  patterns: [wasm-real-validation, generator-stub-fallback]

key-files:
  created:
    - nes-topology-editor/wasm/nes-validator/generator_validation_stub.cpp
  modified:
    - nes-topology-editor/wasm/nes-validator/CMakeLists.txt
    - nes-topology-editor/wasm/nes-validator/generated/SourceValidationGeneratedRegistrar.inc
    - nes-topology-editor/wasm/nes-validator/generated/SinkValidationGeneratedRegistrar.inc
    - nes-validator/CMakeLists.txt
    - nes-validator/tests/UnitTests/TopologyValidatorTest.cpp
  deleted:
    - nes-topology-editor/wasm/nes-validator/source_validation_stubs.cpp

key-decisions:
  - "Generator validation kept as WASM stub -- GeneratorSourceValidation.hpp depends on Generator.hpp, GeneratorFields.hpp, FixedGeneratorRate.hpp, SinusGeneratorRate.hpp which have heavy runtime deps incompatible with WASM"
  - "All other source/sink validations use real code in WASM -- File, TCP, Network sources and all 5 sink types"

patterns-established:
  - "WASM validation stub fallback: when a validation .cpp has heavy runtime deps, keep a minimal stub that does pass-through validation"

requirements-completed: [VLIB-01]

# Metrics
duration: 4min
completed: 2026-03-16
---

# Phase 8 Plan 2: Wire WASM to Real Validation and Add Config Validation Tests Summary

**WASM build uses real File/TCP/Network source validation and all sink validation, with unit tests proving config field validation rejects missing required parameters**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-16T15:58:49Z
- **Completed:** 2026-03-16T16:02:42Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Deleted source_validation_stubs.cpp and wired WASM build to real validation .cpp files for 3 source types (File, TCP, Network) and all 5 sink types
- Updated registrar .inc files to include NetworkSource and NetworkSink entries (previously missing from WASM)
- Native nes-validator now explicitly links nes-sources-validation and nes-sinks-validation
- Added 3 new unit tests: FileSourceMissingFilePathReturnsError, TCPSourceMissingHostPortReturnsError, FileSourceWithFilePathPassesValidation
- All 12 TopologyValidatorTest cases pass (9 existing + 3 new)

## Task Commits

Each task was committed atomically:

1. **Task 1: Update WASM build to use real validation and update native nes-validator** - `5c95e7dc48` (feat)
2. **Task 2: Add unit tests for config field validation** - `84428cc2d0` (test)

## Decisions Made

1. **Generator validation kept as WASM stub** -- GeneratorSourceValidation.hpp includes Generator.hpp, GeneratorFields.hpp, FixedGeneratorRate.hpp, SinusGeneratorRate.hpp. These are heavy runtime classes with complex dependencies (magic_enum for enums, string parsing utilities) that would require additional WASM stubs. A minimal pass-through stub is used instead. File, TCP, Network, and all sinks use real validation.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Generator validation kept as stub instead of real code**
- **Found during:** Task 1 (WASM CMakeLists.txt update)
- **Issue:** GeneratorSourceValidation.hpp includes Generator.hpp, GeneratorFields.hpp, FixedGeneratorRate.hpp, SinusGeneratorRate.hpp -- heavy runtime dependencies not available in WASM build
- **Fix:** Created generator_validation_stub.cpp with pass-through validation (as anticipated by the plan's fallback path)
- **Files modified:** nes-topology-editor/wasm/nes-validator/generator_validation_stub.cpp, nes-topology-editor/wasm/nes-validator/CMakeLists.txt
- **Verification:** Native build succeeds, all tests pass
- **Committed in:** 5c95e7dc48

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Generator stub was explicitly anticipated by the plan as a fallback. All other 8 source/sink types use real validation. No scope creep.

## Issues Encountered
None -- native build and all tests passed on first attempt.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- WASM build CMakeLists.txt ready for full WASM recompilation when Emscripten environment is available
- Real config validation active for File, TCP, Network sources and all sinks
- Generator validation can be upgraded to real code when Generator runtime helpers are made WASM-compatible

## Self-Check: PASSED

- generator_validation_stub.cpp exists
- source_validation_stubs.cpp confirmed deleted
- Task 1 commit 5c95e7dc48 exists in git log
- Task 2 commit 84428cc2d0 exists in git log
- All 12 TopologyValidatorTest tests pass

---
*Phase: 08-split-source-sink-validation-and-runtime-modules*
*Completed: 2026-03-16*
