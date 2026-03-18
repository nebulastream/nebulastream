---
phase: 05-proof-of-concept-spikes
plan: 01
subsystem: infra
tags: [emscripten, wasm, cpp23, embind, cmake, vcpkg]

# Dependency graph
requires: []
provides:
  - "Emscripten SDK installation and build infrastructure"
  - "vcpkg wasm32-emscripten triplet for WASM dependency management"
  - "Validated C++23 (std::expected, concepts, ranges) compiles to WASM"
  - "Validated Embind C++ to JavaScript interop works in WASM"
affects: [05-02, 06-extraction, 07-integration]

# Tech tracking
tech-stack:
  added: [emscripten-4.0, embind]
  patterns: [emcmake-cmake-build, modularize-cjs-pattern, vcpkg-wasm-triplet]

key-files:
  created:
    - nes-topology-editor/wasm/CMakeLists.txt
    - nes-topology-editor/wasm/vcpkg.json
    - nes-topology-editor/wasm/triplets/wasm32-emscripten.cmake
    - nes-topology-editor/wasm/spike-cpp23/main.cpp
    - nes-topology-editor/wasm/spike-cpp23/CMakeLists.txt
    - nes-topology-editor/wasm/spike-embind/main.cpp
    - nes-topology-editor/wasm/spike-embind/CMakeLists.txt
    - nes-topology-editor/wasm/test/test-cpp23.mjs
    - nes-topology-editor/wasm/test/test-embind.mjs
  modified: []

key-decisions:
  - "Use .cjs suffix for Emscripten output because parent package.json has type:module"
  - "Embind with MODULARIZE=1 and EXPORT_ES6=0 produces CJS factory importable via createRequire"

patterns-established:
  - "WASM build: emcmake cmake -B build then cmake --build build"
  - "WASM test: Node.js scripts in wasm/test/ that run or import built artifacts"
  - "Output suffix: .cjs to avoid ESM/CJS conflict with parent package.json"

requirements-completed: [WASM-01]

# Metrics
duration: 4min
completed: 2026-03-15
---

# Phase 5 Plan 01: WASM Build Infrastructure Summary

**Emscripten C++23 spike (std::expected, concepts, ranges) and Embind interop spike both compile to WASM and pass Node.js tests**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-15T16:24:14Z
- **Completed:** 2026-03-15T16:27:59Z
- **Tasks:** 2
- **Files modified:** 11

## Accomplishments
- Emscripten SDK installed and activated (latest/4.0.x)
- C++23 spike proves std::expected, concepts, and ranges all compile and run correctly in WASM via Node.js
- Embind spike proves C++ functions can be called from JavaScript using the MODULARIZE factory pattern
- vcpkg wasm32-emscripten triplet created for future dependency management (ANTLR4 in plan 02)

## Task Commits

Each task was committed atomically:

1. **Task 1: Create WASM build infrastructure and C++23 spike** - `f0d3c9d701` (feat)
2. **Task 2: Create Embind interop spike** - `f61a2c0dc1` (feat)

## Files Created/Modified
- `nes-topology-editor/wasm/CMakeLists.txt` - Top-level CMake project for all spikes
- `nes-topology-editor/wasm/vcpkg.json` - Minimal vcpkg manifest (empty deps for now)
- `nes-topology-editor/wasm/triplets/wasm32-emscripten.cmake` - vcpkg triplet for Emscripten cross-compilation
- `nes-topology-editor/wasm/.gitignore` - Excludes emsdk/ and build/ directories
- `nes-topology-editor/wasm/spike-cpp23/main.cpp` - C++23 feature test (std::expected, concepts, ranges)
- `nes-topology-editor/wasm/spike-cpp23/CMakeLists.txt` - Build config for C++23 spike
- `nes-topology-editor/wasm/spike-embind/main.cpp` - Embind interop test (greet function)
- `nes-topology-editor/wasm/spike-embind/CMakeLists.txt` - Build config for Embind spike
- `nes-topology-editor/wasm/test/test-cpp23.mjs` - Node.js test for C++23 spike
- `nes-topology-editor/wasm/test/test-embind.mjs` - Node.js test for Embind spike

## Decisions Made
- Used `.cjs` suffix for Emscripten output instead of `.js` because `nes-topology-editor/package.json` has `"type": "module"`, which causes Node.js to treat `.js` files as ESM while Emscripten generates CJS code with `require()`
- Used `createRequire` in the Embind test to import the CJS module from an ESM test file
- Patched Emscripten's filelock.py to handle `OSError: [Errno 5]` in Docker environment (patch is in gitignored emsdk/ directory, not committed)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Created spike-embind files before cmake configure**
- **Found during:** Task 1 (CMake configure step)
- **Issue:** Top-level CMakeLists.txt references spike-embind subdirectory but Task 1 did not create those files yet
- **Fix:** Created spike-embind/CMakeLists.txt and main.cpp during Task 1 so cmake configure succeeds
- **Files modified:** spike-embind/CMakeLists.txt, spike-embind/main.cpp
- **Verification:** cmake configure succeeds
- **Committed in:** f0d3c9d701 (Task 1 commit)

**2. [Rule 1 - Bug] Changed output suffix from .js to .cjs**
- **Found during:** Task 1 (Node.js execution)
- **Issue:** Parent package.json has `"type": "module"`, causing Node to treat Emscripten's CJS output as ESM
- **Fix:** Changed SUFFIX property to `.cjs` in both spike CMakeLists.txt files
- **Files modified:** spike-cpp23/CMakeLists.txt, spike-embind/CMakeLists.txt
- **Verification:** `node build/spike-cpp23/spike-cpp23.cjs` runs successfully
- **Committed in:** f0d3c9d701 (Task 1 commit)

**3. [Rule 3 - Blocking] Patched Emscripten filelock.py for Docker filesystem**
- **Found during:** Task 1 (Build step)
- **Issue:** Emscripten's filelock.py throws `OSError: [Errno 5]` on `os.close(fd)` in Docker overlay filesystem
- **Fix:** Wrapped `_release()` method in try/except for os.unlink, fcntl.flock, os.close
- **Files modified:** emsdk/upstream/emscripten/tools/filelock.py (gitignored, not committed)
- **Verification:** Build completes successfully after patch

---

**Total deviations:** 3 auto-fixed (1 bug, 2 blocking)
**Impact on plan:** All auto-fixes necessary for correct operation in the project's environment. No scope creep.

## Issues Encountered
- Emscripten filelock.py has a bug in Docker/overlay filesystems where `os.close()` fails with I/O error. Patched locally in the gitignored emsdk directory. This will need to be re-applied after any emsdk reinstall.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- WASM build infrastructure is ready for plan 02 (ANTLR4 spike)
- vcpkg triplet is in place for adding antlr4 dependency
- Emscripten SDK must be installed locally (gitignored) -- run `cd nes-topology-editor/wasm && git clone https://github.com/emscripten-core/emsdk.git && cd emsdk && ./emsdk install latest && ./emsdk activate latest`

---
## Self-Check: PASSED

All 9 created files verified present. Both task commits (f0d3c9d701, f61a2c0dc1) verified in git log.

---
*Phase: 05-proof-of-concept-spikes*
*Completed: 2026-03-15*
