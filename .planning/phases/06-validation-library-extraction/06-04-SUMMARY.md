---
phase: 06-validation-library-extraction
plan: 04
subsystem: validation
tags: [c++, cmake, nes-cli, nes-validator, offline-validation]

# Dependency graph
requires:
  - phase: 06-validation-library-extraction
    plan: 02
    provides: Full validation pipeline with validateTopology() API, YAML binding, 9 passing tests
provides:
  - nes-cli 'validate' subcommand calling NES::Validator::validateTopology()
  - CMake dependency wiring from nes-cli to nes-validator
  - readTopologyYaml() helper for raw YAML file/stdin reading
affects: [07-wasm-integration]

# Tech tracking
tech-stack:
  added: []
  patterns: [validate-subcommand-pattern, private-stubs-include-for-consumers]

key-files:
  created: []
  modified:
    - nes-frontend/apps/cli/CLIStarter.cpp
    - nes-frontend/apps/CMakeLists.txt
    - nes-validator/CMakeLists.txt

key-decisions:
  - "Add 'validate' subcommand rather than replacing existing dump/start paths -- existing paths use gRPC and LegacyOptimizer not available in nes-validator"
  - "Link nes-validator at nes-cli target level (not nes-frontend-lib) since only CLI needs it"
  - "Change nes-validator stubs include from PUBLIC to PRIVATE to prevent stub headers leaking to native consumers"

patterns-established:
  - "Consumer integration pattern: link nes-validator PRIVATE, stubs stay internal to validator build"

requirements-completed: [VLIB-01]

# Metrics
duration: 5min
completed: 2026-03-15
---

# Phase 6 Plan 04: Frontend Restructuring Summary

**nes-cli gains 'validate' subcommand using nes-validator library for offline topology validation without a running NES cluster**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-15T20:14:31Z
- **Completed:** 2026-03-15T20:19:00Z
- **Tasks:** 1
- **Files modified:** 3

## Accomplishments
- Added `validate` subcommand to nes-cli that calls `NES::Validator::validateTopology()` for offline YAML validation
- Wired nes-validator as CMake dependency for nes-cli target
- Fixed nes-validator stubs include visibility from PUBLIC to PRIVATE to prevent header conflicts with real libraries in native builds

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire nes-cli to use nes-validator for offline validation** - `2d244b80ed` (feat)

## Files Created/Modified
- `nes-frontend/apps/cli/CLIStarter.cpp` - Added validate subcommand, readTopologyYaml() helper, Validator include
- `nes-frontend/apps/CMakeLists.txt` - Added nes-validator to nes-cli link dependencies
- `nes-validator/CMakeLists.txt` - Changed stubs include from PUBLIC to PRIVATE

## Decisions Made
- Added a new `validate` subcommand rather than replacing existing `dump`/`start` code paths. The existing paths use `LegacyOptimizer`, `QueryManager`, and gRPC backends which are not part of nes-validator's scope. The `validate` subcommand provides a clean offline validation path that only depends on nes-validator.
- Linked nes-validator at the `nes-cli` target level in `nes-frontend/apps/CMakeLists.txt` rather than in `nes-frontend/CMakeLists.txt`, since only nes-cli (not nes-repl) needs offline validation.
- Changed nes-validator's stubs include directory from `PUBLIC` to `PRIVATE`. The stubs (cpptrace, folly) are only needed when compiling nes-validator's own sources. Exposing them as PUBLIC caused the stub cpptrace headers to override the real vcpkg cpptrace headers in nes-cli, triggering a `folly::hash::twang_mix64` compilation error.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed nes-validator stubs leaking to consumers**
- **Found during:** Task 1 (build verification)
- **Issue:** nes-validator's CMakeLists.txt had `target_include_directories(nes-validator BEFORE PUBLIC stubs)` which caused stub cpptrace headers to override real cpptrace headers when nes-cli linked nes-validator. This broke the Folly include chain (`ParkingLot.h:260: error: no member named 'twang_mix64'`).
- **Fix:** Changed stubs include from `PUBLIC` to `PRIVATE` so stubs only apply during nes-validator compilation, not for consumers.
- **Files modified:** nes-validator/CMakeLists.txt
- **Verification:** nes-cli builds successfully; nes-validator's 9 unit tests still pass
- **Committed in:** 2d244b80ed (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Necessary fix for native consumer builds. No scope creep.

## Issues Encountered
- Pre-existing nes-validator stubs include visibility (PUBLIC) conflicted with real library headers when linked by native consumers. Resolved by making stubs PRIVATE.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- nes-cli now has a working `validate` subcommand using nes-validator
- nes-validator stubs are properly scoped (PRIVATE) for both native and future WASM builds
- Ready for Phase 07 (WASM integration) to compile nes-validator to WebAssembly

## Self-Check: PASSED

- All 3 modified files found on disk
- Task commit (2d244b80ed) verified in git log
- CLIStarter.cpp contains "validateTopology" (verified)
- apps/CMakeLists.txt contains "nes-validator" (verified)
- nes-validator unit tests (9/9) still pass

---
*Phase: 06-validation-library-extraction*
*Completed: 2026-03-15*
