---
phase: 06-validation-library-extraction
plan: 02
subsystem: validation
tags: [c++, yaml-cpp, antlr4, validation, gtest, source-catalog]

# Dependency graph
requires:
  - phase: 06-validation-library-extraction
    plan: 01
    provides: nes-validator CMake target with stubs, skeleton validateTopology, test scaffold
provides:
  - Full validation pipeline: YAML parse -> catalog populate -> SQL validate
  - YAML binding (yaml-cpp convert<> specializations) for NES topology types
  - Source name validation against catalog after SQL parsing
  - 9 passing GTest unit tests covering all validation behaviors
affects: [06-03-PLAN, 06-04-PLAN, 07-wasm-integration]

# Tech tracking
tech-stack:
  added: []
  patterns: [createLogicalQueryPlanFromSQLString-for-sql-validation, getOperatorByType-for-source-name-extraction]

key-files:
  created:
    - nes-validator/src/YamlBinding.hpp
    - nes-validator/src/YamlBinding.cpp
  modified:
    - nes-validator/src/TopologyValidator.cpp
    - nes-validator/CMakeLists.txt
    - nes-validator/tests/UnitTests/TopologyValidatorTest.cpp

key-decisions:
  - "Use createLogicalQueryPlanFromSQLString instead of StatementBinder::parseAndBind for SQL validation -- parseAndBind with multipleStatements parser rule hangs on YAML block scalar queries with trailing newlines"
  - "Source name validation via getOperatorByType<SourceNameLogicalOperator> after plan creation -- ANTLR parser does not check source catalog during parsing"
  - "Skip SinkCatalog population -- StatementBinder and createLogicalQueryPlanFromSQLString do not require SinkCatalog for SQL validation"

patterns-established:
  - "SQL validation pattern: createLogicalQueryPlanFromSQLString + getOperatorByType<SourceNameLogicalOperator> for source name checks"
  - "YAML binding pattern: yaml-cpp convert<> specializations in separate YamlBinding.hpp/cpp mirroring CLIStarter.cpp types"

requirements-completed: [VLIB-03, VLIB-04, VLIB-05, VLIB-06, TEST-01]

# Metrics
duration: 13min
completed: 2026-03-15
---

# Phase 6 Plan 02: Validation Pipeline Implementation Summary

**Full YAML-to-SQL validation pipeline with yaml-cpp binding, SourceCatalog population, ANTLR4 SQL parsing, and source name resolution -- 9 GTest cases passing**

## Performance

- **Duration:** 13 min
- **Started:** 2026-03-15T19:58:00Z
- **Completed:** 2026-03-15T20:11:28Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Implemented complete validateTopology() pipeline: YAML parsing, catalog population, SQL syntax validation, source name resolution
- Ported yaml-cpp convert<> specializations from CLIStarter.cpp for all topology types (SchemaField, LogicalSource, PhysicalSource, Sink, TopologyConfig)
- Created 9 comprehensive unit tests covering valid topologies, missing sources, invalid SQL, empty YAML, multiple queries, orphaned physical sources, invalid data types, and malformed YAML

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement YAML binding and catalog population** - `0e2021b2e0` (feat)
2. **Task 2: Add comprehensive unit tests for validation pipeline** - `7487fee8cc` (test)

## Files Created/Modified
- `nes-validator/src/YamlBinding.hpp` - TopologyConfig struct and yaml-cpp convert<> specialization declarations
- `nes-validator/src/YamlBinding.cpp` - yaml-cpp decode implementations ported from CLIStarter.cpp
- `nes-validator/src/TopologyValidator.cpp` - Full validation pipeline: YAML parse -> catalog -> SQL validate -> source name check
- `nes-validator/CMakeLists.txt` - Added YamlBinding.cpp to sources
- `nes-validator/tests/UnitTests/TopologyValidatorTest.cpp` - 9 GTest test cases with YAML fixture strings

## Decisions Made
- Used `createLogicalQueryPlanFromSQLString` instead of `StatementBinder::parseAndBind` for SQL validation. The `parseAndBind` method uses ANTLR's `multipleStatements` parser rule which hangs on queries with trailing newlines from YAML block scalars. `createLogicalQueryPlanFromSQLString` uses the `query` parser rule directly, matching the approach used by nes-cli.
- Source name validation is done post-parse via `getOperatorByType<SourceNameLogicalOperator>()` to extract source names from the logical plan and check them against the SourceCatalog. The ANTLR parser creates SOURCE nodes without validating source existence.
- Skipped SinkCatalog population entirely -- neither the SQL parser nor the query plan creator requires SinkCatalog. Sink names in INTO clauses are stored as strings in SinkLogicalOperator nodes without catalog validation.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] StatementBinder::parseAndBind hangs on YAML block scalar queries**
- **Found during:** Task 1 (validation pipeline implementation)
- **Issue:** `parseAndBind` uses `parser.multipleStatements()` which hangs when processing query strings with trailing newlines from YAML block scalars. The `free(): invalid pointer` crash also occurred with ANTLR error paths through this code.
- **Fix:** Switched to `createLogicalQueryPlanFromSQLString` which uses the `query` parser rule directly, matching nes-cli's approach. Added manual source name validation via `getOperatorByType<SourceNameLogicalOperator>`.
- **Files modified:** nes-validator/src/TopologyValidator.cpp
- **Verification:** All 9 tests pass, including valid topology and invalid SQL cases
- **Committed in:** 0e2021b2e0 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Necessary workaround for ANTLR parser hang. Same validation outcome achieved via different code path.

## Issues Encountered
- StatementBinder::parseAndBind hangs when processing YAML block scalar queries (trailing newline causes multipleStatements parser to wait for more input). Resolved by using createLogicalQueryPlanFromSQLString directly.
- `free(): invalid pointer` crash when ANTLR4's BailErrorStrategy throws ParseCancellationException through StatementBinder path for invalid SQL. Resolved by the same switch to createLogicalQueryPlanFromSQLString.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- validateTopology() is fully functional with YAML parsing, catalog population, and SQL validation
- All 9 unit tests pass
- Ready for Plan 03 (WASM build) to compile nes-validator to WebAssembly
- Known limitation: type inference validation (e.g., incompatible type operations in queries) requires LegacyOptimizer which was deferred per research recommendation

## Self-Check: PASSED

- All 5 files found on disk
- Both task commits (0e2021b2e0, 7487fee8cc) verified in git log
- TopologyValidator.cpp: 99 lines (meets min_lines: 50)
- TopologyValidatorTest.cpp: 343 lines (meets min_lines: 80)
- YamlBinding.hpp contains "TopologyConfig" (verified)
- YamlBinding.cpp contains "YAML::convert" (verified)
- TopologyValidator.cpp contains "addLogicalSource" and "addPhysicalSource" (verified)
- TopologyValidator.cpp contains "as<TopologyConfig>" pattern (verified)

---
*Phase: 06-validation-library-extraction*
*Completed: 2026-03-15*
