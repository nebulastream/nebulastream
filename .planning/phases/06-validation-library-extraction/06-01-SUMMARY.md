---
phase: 06-validation-library-extraction
plan: 01
subsystem: infra
tags: [cmake, c++, stubs, cpptrace, folly, gtest, validation]

# Dependency graph
requires:
  - phase: 05-proof-of-concept-spikes
    provides: WASM build patterns, Emscripten toolchain, ANTLR4 WASM compilation
provides:
  - nes-validator CMake target (static library)
  - Stub/facade layer for cpptrace and folly (WASM-safe compilation)
  - TopologyValidator public API skeleton (validateTopology)
  - Test scaffold with GTest infrastructure
affects: [06-02-PLAN, 06-03-PLAN, 06-04-PLAN, 07-wasm-integration]

# Tech tracking
tech-stack:
  added: []
  patterns: [stub-header-injection-via-BEFORE, nes-validator-target-layout]

key-files:
  created:
    - nes-validator/CMakeLists.txt
    - nes-validator/include/Validator/TopologyValidator.hpp
    - nes-validator/src/TopologyValidator.cpp
    - nes-validator/stubs/folly/hash/Hash.h
    - nes-validator/stubs/cpptrace/forward.hpp
    - nes-validator/stubs/cpptrace/basic.hpp
    - nes-validator/stubs/cpptrace/cpptrace.hpp
    - nes-validator/stubs/cpptrace/exceptions.hpp
    - nes-validator/stubs/cpptrace/from_current.hpp
    - nes-validator/tests/CMakeLists.txt
    - nes-validator/tests/UnitTests/TopologyValidatorTest.cpp
  modified: []

key-decisions:
  - "cpptrace stubs include full lazy_exception class with raw_trace/stacktrace no-ops, matching real cpptrace API surface used by NES Exception class"
  - "nes-validator links nes-sql-parser PUBLIC which brings nes-sources/nes-sinks transitively -- acceptable for native build; WASM build in later plan will use different strategy"
  - "Root CMakeLists.txt auto-discovers nes-validator via existing nes-* glob pattern -- no manual add_subdirectory needed"

patterns-established:
  - "Stub header injection: target_include_directories(BEFORE PUBLIC stubs/) overrides vcpkg headers"
  - "nes-validator test pattern: add_nes_unit_test + plain target_link_libraries (no PRIVATE keyword, matching NES convention)"

requirements-completed: [VLIB-01, VLIB-02]

# Metrics
duration: 10min
completed: 2026-03-15
---

# Phase 6 Plan 01: Validation Library Foundation Summary

**nes-validator CMake target with cpptrace/folly stub layer, skeleton validateTopology() API, and GTest scaffold**

## Performance

- **Duration:** 10 min
- **Started:** 2026-03-15T19:43:28Z
- **Completed:** 2026-03-15T19:53:30Z
- **Tasks:** 2
- **Files modified:** 11

## Accomplishments
- Created nes-validator static library that compiles without gRPC, Folly, or cpptrace runtime dependencies
- Built complete cpptrace stub layer (forward, basic, exceptions, from_current, cpptrace aggregate) matching the real cpptrace API surface used by NES Exception class
- Created folly/hash/Hash.h stub using std::hash fold expression for SourceDescriptor compatibility
- Established test scaffold with 2 passing GTest tests using NES BaseUnitTest pattern

## Task Commits

Each task was committed atomically:

1. **Task 1: Create nes-validator CMake target with stubs** - `5e97130cd4` (feat)
2. **Task 2: Create test scaffold and verify end-to-end build** - `11a9453a79` (feat)

## Files Created/Modified
- `nes-validator/CMakeLists.txt` - Static library target with stub injection and NES dep linking
- `nes-validator/include/Validator/TopologyValidator.hpp` - Public API: validateTopology(const std::string&)
- `nes-validator/src/TopologyValidator.cpp` - Skeleton returning empty string (placeholder for Plan 02)
- `nes-validator/stubs/folly/hash/Hash.h` - std::hash replacement for folly::hash::hash_combine
- `nes-validator/stubs/cpptrace/forward.hpp` - Forward declarations (frame_ptr, raw_trace, stacktrace)
- `nes-validator/stubs/cpptrace/basic.hpp` - No-op raw_trace, stacktrace, stacktrace_frame, generate_trace
- `nes-validator/stubs/cpptrace/exceptions.hpp` - lazy_exception as std::exception subclass with no-op trace
- `nes-validator/stubs/cpptrace/cpptrace.hpp` - Aggregate header including basic + exceptions
- `nes-validator/stubs/cpptrace/from_current.hpp` - CPPTRACE_TRY/CATCH as plain try/catch
- `nes-validator/tests/CMakeLists.txt` - Test registration using add_nes_unit_test
- `nes-validator/tests/UnitTests/TopologyValidatorTest.cpp` - 2 skeleton tests

## Decisions Made
- cpptrace stubs implement the full API surface (lazy_exception with raw_trace/stacktrace/lazy_trace_holder) rather than minimal stubs, to ensure all NES headers that transitively include cpptrace compile cleanly
- Root CMakeLists.txt auto-discovers nes-validator via the existing `file(GLOB ... "nes-*")` pattern, so no manual modification to root CMakeLists.txt was needed
- Used plain `target_link_libraries` (no PRIVATE/PUBLIC keyword) for test targets to match NES convention established in `add_nes_unit_test` macro

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Initial build failed because `add_tests_if_enabled(tests)` was included before test directory existed; resolved by deferring to Task 2
- Test `target_link_libraries` with PRIVATE keyword conflicted with NES test macro that uses plain signature; fixed by removing PRIVATE keyword

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- nes-validator target compiles and links successfully
- Test infrastructure ready for Plan 02 to add real YAML validation test cases
- Stub layer ready for WASM build (Plan 03/04)
- Plan 02 will implement the full validation pipeline (YAML binding, catalog population, SQL parsing)

---
*Phase: 06-validation-library-extraction*
*Completed: 2026-03-15*
