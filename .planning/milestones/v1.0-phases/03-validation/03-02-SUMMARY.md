---
phase: 03-validation
plan: 02
subsystem: testing
tags: [gtest, integration-test, tokio, cxx-bridge, generator-source, backpressure, ffi]

# Dependency graph
requires:
  - phase: 03-validation
    provides: "GeneratorSource, spawn_generator_source FFI, test-only TupleBufferHandle"
  - phase: 02-framework-and-bridge
    provides: "TokioSource class, EmitContext/ErrorContext, bridge_emit callback, CXX bridge"
provides:
  - "End-to-end C++ GTest integration tests for Rust async source pipeline"
  - "Verified buffer ordering, backpressure stability, and shutdown safety"
  - "First full C++ compilation of CXX bridge code (discovered/fixed namespace and include issues)"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns: [cxx-global-scope-using-declarations, tokio-recorder-pattern]

key-files:
  created:
    - nes-sources/tests/TokioSourceTest.cpp
  modified:
    - nes-sources/tests/CMakeLists.txt
    - nes-sources/CMakeLists.txt
    - nes-sources/rust/nes-source-bindings/include/SourceBindings.hpp
    - nes-sources/rust/nes-source-bindings/src/SourceBindings.cpp
    - nes-sources/src/TokioSource.cpp

key-decisions:
  - "CXX bridge functions exposed at global scope via using declarations in SourceBindings.hpp (CXX generates code expecting ::retain, ::getDataPtr etc.)"
  - "nes-sources links PRIVATE to nes-source-bindings for CXX header access (circular dependency broken by PRIVATE linkage)"
  - "TokioRecorder stores raw byte vectors instead of TupleBuffer copies (buffer lifecycle managed by Rust bridge)"
  - "Linker --allow-multiple-definition for duplicate CXX runtime symbols between nes_rust_bindings and nes_source_lib"

patterns-established:
  - "TokioRecorder: thread-safe emit recording with folly::Synchronized + condition_variable for async Rust->C++ test verification"
  - "Global-scope using declarations in CXX bridge headers: required because CXX generates unqualified references"
  - "Direct FFI call pattern: test bypasses TokioSource class to call spawn_generator_source directly for GeneratorSource testing"

requirements-completed: [VAL-02]

# Metrics
duration: 35min
completed: 2026-03-15
---

# Phase 3 Plan 02: C++ Integration Tests for TokioSource Summary

**Three GTest integration tests proving end-to-end Rust-to-C++ buffer pipeline: ordering, backpressure, and shutdown safety via GeneratorSource + CXX FFI**

## Performance

- **Duration:** 35 min
- **Started:** 2026-03-15T15:48:17Z
- **Completed:** 2026-03-15T16:23:48Z
- **Tasks:** 1
- **Files modified:** 6

## Accomplishments
- 3 C++ integration tests pass: GeneratorEmitsIncrementingValues (5 buffers with u64 0-4), BackpressureStability (50 emits, inflight_limit=2), ShutdownMidEmit (clean stop mid-emission)
- First successful full C++ compilation of CXX bridge code -- uncovered and fixed namespace resolution, include path, and linker issues from previous phases
- All 48 Rust workspace tests continue to pass
- End-to-end proof: Rust async source -> Tokio runtime -> bridge thread -> C++ emit callback with correct data

## Task Commits

Each task was committed atomically:

1. **Task 1: Create TokioSourceTest.cpp with end-to-end integration tests** - `c4add196ec` (feat)

## Files Created/Modified
- `nes-sources/tests/TokioSourceTest.cpp` - 3 GTest integration tests exercising GeneratorSource via CXX FFI
- `nes-sources/tests/CMakeLists.txt` - Registered tokio-source-test, linked nes-source-bindings, added linker flags
- `nes-sources/CMakeLists.txt` - Added PRIVATE link from nes-sources to nes-source-bindings for CXX headers
- `nes-sources/rust/nes-source-bindings/include/SourceBindings.hpp` - Added using declarations for CXX global-scope resolution
- `nes-sources/rust/nes-source-bindings/src/SourceBindings.cpp` - Fixed CXX header include path
- `nes-sources/src/TokioSource.cpp` - Fixed CXX header include path and namespace references

## Decisions Made
- **CXX global-scope using declarations:** CXX bridge generates code that references types and functions at `::` global scope. Added `using NES::TupleBuffer;`, `using NES::AbstractBufferProvider;`, and `using namespace NES::SourceBindings;` so CXX-generated code resolves correctly.
- **PRIVATE linkage for circular dependency:** nes-sources needs nes-source-bindings headers (CXX generated), but nes-source-bindings links to nes-sources. Resolved with `target_link_libraries(nes-sources PRIVATE nes-source-bindings)`.
- **TokioRecorder pattern:** Stores raw byte vectors extracted from TupleBuffer data spans instead of copying buffers through BufferManager (simpler, avoids buffer lifecycle complexity in tests).
- **--allow-multiple-definition linker flag:** Two Rust static libraries (nes_rust_bindings, nes_source_lib) both embed the CXX runtime, creating duplicate symbols. Flag allows linker to keep first definition.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] CXX bridge namespace resolution**
- **Found during:** Task 1 (compilation)
- **Issue:** CXX bridge generates code expecting functions like `::retain`, `::getDataPtr` at global scope, but they were declared in `NES::SourceBindings::` namespace. This was a latent bug from Phase 1 -- previous phases only compiled Rust code, never the CXX-generated C++ bridge code.
- **Fix:** Added `using NES::TupleBuffer;`, `using NES::AbstractBufferProvider;`, `using namespace NES::SourceBindings;` to SourceBindings.hpp after the namespace block.
- **Files modified:** nes-sources/rust/nes-source-bindings/include/SourceBindings.hpp
- **Verification:** CXX bridge cpp files compile without errors
- **Committed in:** c4add196ec

**2. [Rule 3 - Blocking] CXX header include paths (.rs.h vs .h)**
- **Found during:** Task 1 (compilation)
- **Issue:** Source files used `#include <nes-source-lib/src/lib.rs.h>` and `<nes-source-bindings/lib.rs.h>` but Corrosion v0.5.2 generates headers as `lib.h` (not `lib.rs.h`). This was another latent issue from previous phases.
- **Fix:** Changed all includes from `.rs.h` to `.h` in TokioSource.cpp, SourceBindings.cpp, and TokioSourceTest.cpp.
- **Files modified:** nes-sources/src/TokioSource.cpp, nes-sources/rust/nes-source-bindings/src/SourceBindings.cpp, nes-sources/tests/TokioSourceTest.cpp
- **Verification:** Headers found, compilation succeeds
- **Committed in:** c4add196ec

**3. [Rule 3 - Blocking] Missing CMake link dependency for CXX headers**
- **Found during:** Task 1 (compilation)
- **Issue:** nes-sources target didn't have include paths to CXX-generated headers (in corrosion_generated directory). TokioSource.cpp couldn't find `<nes-source-lib/src/lib.h>`.
- **Fix:** Added `target_link_libraries(nes-sources PRIVATE nes-source-bindings)` to propagate include directories.
- **Files modified:** nes-sources/CMakeLists.txt
- **Verification:** nes-sources compiles with CXX headers accessible
- **Committed in:** c4add196ec

**4. [Rule 3 - Blocking] CXX namespace references in TokioSource.cpp**
- **Found during:** Task 1 (compilation)
- **Issue:** TokioSource.cpp used `nes_source_lib::ffi_source::spawn_source()` but CXX bridge generates functions at global scope (no `ffi_source` namespace in C++).
- **Fix:** Changed all namespace-qualified calls to global-scope (`::spawn_source`, `::stop_source`, `::SourceHandle`).
- **Files modified:** nes-sources/src/TokioSource.cpp
- **Verification:** Compilation succeeds
- **Committed in:** c4add196ec

**5. [Rule 3 - Blocking] Duplicate CXX runtime symbols at link time**
- **Found during:** Task 1 (linking)
- **Issue:** Both `libnes_rust_bindings.a` and `libnes_source_lib.a` contain identical CXX runtime symbols (cxxbridge1$rust_vec$*, etc.), causing mold linker "duplicate symbol" errors.
- **Fix:** Added `target_link_options(tokio-source-test PRIVATE "LINKER:--allow-multiple-definition")`.
- **Files modified:** nes-sources/tests/CMakeLists.txt
- **Verification:** Linker succeeds, test binary runs correctly
- **Committed in:** c4add196ec

---

**Total deviations:** 5 auto-fixed (5 blocking)
**Impact on plan:** All deviations were latent build issues from Phases 1-2 (which only tested Rust code via cargo, never compiled the full CXX bridge C++ side). These fixes are necessary for any C++ code that uses the CXX bridge. No scope creep.

## Issues Encountered
- Build environment required vcpkg package installation (copied from working worktree) and MLIR/LLVM path configuration. Corrosion path resolution for CXX bridge FILES required filesystem symlinks due to Corrosion v0.5.2 resolving paths relative to crate src/ directory.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All 3 phases complete: FFI foundation, framework/bridge, validation
- Full end-to-end proof: Rust async GeneratorSource -> Tokio runtime -> bridge thread -> C++ emit callback
- Backpressure and shutdown invariants verified under stress
- Integration test pattern established for future Rust source implementations

---
*Phase: 03-validation*
*Completed: 2026-03-15*
