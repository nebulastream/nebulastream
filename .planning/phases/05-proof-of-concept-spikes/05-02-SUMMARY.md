---
phase: 05-proof-of-concept-spikes
plan: 02
subsystem: infra
tags: [antlr4, wasm, emscripten, sql-parser, embind, fetchcontent]

# Dependency graph
requires:
  - phase: 05-01
    provides: "Emscripten SDK, WASM build infrastructure, vcpkg triplet"
provides:
  - "ANTLR4 C++ runtime compiles to WebAssembly via Emscripten"
  - "NES SQL parser (AntlrSQL.g4) runs in WASM and parses/rejects SQL correctly"
  - "All three Phase 5 spikes pass: C++23, Embind, ANTLR4"
  - "WASM binary size baselines: cpp23=7KB, embind=13KB, antlr4=675KB"
affects: [06-extraction, 07-integration]

# Tech tracking
tech-stack:
  added: [antlr4-runtime-4.13.2, fetchcontent]
  patterns: [fetchcontent-antlr4-from-source, wasm-exceptions-consistent, stub-header-for-nes-deps]

key-files:
  created:
    - nes-topology-editor/wasm/spike-antlr4/CMakeLists.txt
    - nes-topology-editor/wasm/spike-antlr4/main.cpp
    - nes-topology-editor/wasm/spike-antlr4/include/Util/DisableWarningsPragma.hpp
    - nes-topology-editor/wasm/spike-antlr4/generated/AntlrSQLParser.cpp
    - nes-topology-editor/wasm/spike-antlr4/generated/AntlrSQLLexer.cpp
    - nes-topology-editor/wasm/test/test-antlr4.mjs
    - nes-topology-editor/wasm/test/run-all-spikes.mjs
  modified:
    - nes-topology-editor/wasm/CMakeLists.txt
    - nes-topology-editor/wasm/vcpkg.json

key-decisions:
  - "Build ANTLR4 C++ runtime from source via FetchContent instead of vcpkg (no vcpkg binary available for WASM target)"
  - "Use -fwasm-exceptions consistently across ANTLR4 runtime and spike code for correct exception propagation"
  - "Pre-generate parser C++ from NES build output rather than requiring Java in WASM build"
  - "Use singleStatement as grammar entry point (matches NES usage pattern)"

patterns-established:
  - "FetchContent for ANTLR4 runtime: clone from GitHub, build as static lib with -fwasm-exceptions"
  - "Stub headers for NES-specific includes: DisableWarningsPragma.hpp with no-op macros"
  - "Exception handling: -fwasm-exceptions must be on BOTH compile and link flags, for ALL translation units"

requirements-completed: [WASM-02]

# Metrics
duration: 5min
completed: 2026-03-15
---

# Phase 5 Plan 02: ANTLR4 WASM Spike Summary

**ANTLR4 C++ runtime and NES SQL parser compile to WebAssembly, parsing valid NES SQL and rejecting invalid input via Embind-exposed parseSql function**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-15T16:31:52Z
- **Completed:** 2026-03-15T16:37:00Z
- **Tasks:** 2
- **Files modified:** 13

## Accomplishments
- ANTLR4 C++ runtime (4.13.2) compiles to WebAssembly via Emscripten 5.0.3 using FetchContent
- NES SQL parser (generated from AntlrSQL.g4) correctly parses `SELECT * FROM default_logical` and NES-specific `WINDOW TUMBLING(SIZE 10 SEC)` syntax
- Invalid SQL returns PARSE_ERROR instead of crashing (try-catch + consistent -fwasm-exceptions)
- All three Phase 5 spikes pass: C++23, Embind, and ANTLR4
- WASM binary sizes measured: cpp23=7KB, embind=13KB, antlr4=675KB

## Task Commits

Each task was committed atomically:

1. **Task 1: Generate ANTLR4 C++ parser sources and set up ANTLR4 WASM spike** - `daee06a4fa` (feat)
2. **Task 2: Create ANTLR4 Node.js test and verify SQL parsing in WASM** - `bb272a28c1` (feat)

## Files Created/Modified
- `nes-topology-editor/wasm/spike-antlr4/CMakeLists.txt` - Build config with FetchContent for ANTLR4 runtime
- `nes-topology-editor/wasm/spike-antlr4/main.cpp` - Embind parseSql function wrapping ANTLR4 parser
- `nes-topology-editor/wasm/spike-antlr4/include/Util/DisableWarningsPragma.hpp` - Stub for NES-specific warning macros
- `nes-topology-editor/wasm/spike-antlr4/generated/*.cpp,*.h` - Pre-generated parser/lexer from AntlrSQL.g4
- `nes-topology-editor/wasm/test/test-antlr4.mjs` - Node.js test for ANTLR4 WASM parsing
- `nes-topology-editor/wasm/test/run-all-spikes.mjs` - Combined test runner for all 3 spikes
- `nes-topology-editor/wasm/CMakeLists.txt` - Added spike-antlr4 subdirectory
- `nes-topology-editor/wasm/vcpkg.json` - Added antlr4 dependency declaration

## Decisions Made
- **FetchContent over vcpkg:** No vcpkg binary available in the WASM build environment. Used CMake FetchContent to clone and build ANTLR4 runtime from GitHub at configure time. This is a spike-only approach; Phase 6 may use vcpkg if available.
- **Consistent -fwasm-exceptions:** Initial build used -fwasm-exceptions only in linker flags, causing ANTLR4 exceptions to propagate as uncaught WebAssembly.Exception. Fixed by adding -fwasm-exceptions to compile options for both antlr4_static and spike-antlr4 targets.
- **Pre-generated parser sources:** Copied generated C++ from NES cmake-build-debug output (ANTLR4 4.13.2) rather than requiring Java to regenerate. Grammar doesn't change during the spike.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] ANTLR4 git tag has no 'v' prefix**
- **Found during:** Task 1 (cmake configure)
- **Issue:** Plan used `v4.13.2` but ANTLR4 repo tags as `4.13.2`
- **Fix:** Changed GIT_TAG from `v4.13.2` to `4.13.2`
- **Files modified:** spike-antlr4/CMakeLists.txt
- **Committed in:** daee06a4fa (Task 1 commit)

**2. [Rule 3 - Blocking] No vcpkg binary available for WASM target**
- **Found during:** Task 1 (cmake configure)
- **Issue:** `find_package(antlr4-runtime)` fails because vcpkg isn't installed as a standalone tool
- **Fix:** Replaced vcpkg-based find_package with FetchContent to build ANTLR4 from source
- **Files modified:** spike-antlr4/CMakeLists.txt
- **Committed in:** daee06a4fa (Task 1 commit)

**3. [Rule 1 - Bug] ANTLR4 exceptions not caught in WASM**
- **Found during:** Task 2 (test execution)
- **Issue:** Invalid SQL input caused uncaught WebAssembly.Exception crash. The ANTLR4 runtime was compiled without -fwasm-exceptions but the spike linked with it, causing mismatched exception handling.
- **Fix:** Added -fwasm-exceptions to compile options for both antlr4_static and spike-antlr4 targets, and added try-catch in parseSql
- **Files modified:** spike-antlr4/CMakeLists.txt, spike-antlr4/main.cpp
- **Committed in:** bb272a28c1 (Task 2 commit)

---

**Total deviations:** 3 auto-fixed (1 bug, 2 blocking)
**Impact on plan:** All auto-fixes necessary for correct operation. FetchContent approach is equivalent to plan's fallback option. No scope creep.

## Issues Encountered
- ANTLR4 C++ runtime WASM exception handling requires -fwasm-exceptions on ALL translation units (compile + link). Mixing exception handling modes causes uncatchable WebAssembly.Exception errors. This is a critical pattern for Phase 6.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All Phase 5 spikes validated: C++23, Embind, and ANTLR4 WASM compilation confirmed working
- ANTLR4 WASM binary is 675KB (unoptimized) -- Phase 6 can optimize with -Os and wasm-opt
- FetchContent pattern for ANTLR4 is proven and can be reused in the extraction library (Phase 6)
- Stub header pattern (DisableWarningsPragma.hpp) will be needed for any NES source files with NES-specific includes
- RESOLVED blocker: "ANTLR4 C++ runtime WASM compilation -- abandoned antlr4wasm project is a warning sign" -- confirmed ANTLR4 compiles and runs correctly in WASM

---
## Self-Check: PASSED

All created files verified present. Both task commits (daee06a4fa, bb272a28c1) verified in git log.

---
*Phase: 05-proof-of-concept-spikes*
*Completed: 2026-03-15*
