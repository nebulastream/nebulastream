---
phase: 03-physical-operator-tests
plan: "01"
subsystem: testing
tags: [cmake, iree, onnx, physical-operators, inference, cpp]

# Dependency graph
requires:
  - phase: 02-test-model-artifact
    provides: ONNX test models in nes-inference/tests/testdata/
  - phase: 01-infrastructure-and-logical-tests
    provides: InferModelPhysicalOperator implementation, IREEInferenceOperatorHandler, IREEAdapter

provides:
  - ENABLE_IREE_TESTS CMake flag (ON when iree-import-onnx and iree-compile found in PATH)
  - InferModelPhysicalOperatorTest CMake target with correct library linkage
  - Stub test file with SetUpTestSuite and Placeholder test (GTEST_SKIP)
  - Fixed nes-physical-operators inference library (namespace and API corrections)

affects:
  - 03-physical-operator-tests/03-02 (adds real tests to the stub target)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - find_program IREE tool detection pattern in CheckTestUtilities.cmake
    - add_nes_inference_physical_operator_test helper wrapping add_nes_physical_operator_test
    - INFERENCE_TEST_DATA define pointing to nes-inference/tests/testdata

key-files:
  created:
    - nes-physical-operators/tests/InferModelPhysicalOperatorTest.cpp
  modified:
    - cmake/CheckTestUtilities.cmake
    - nes-physical-operators/tests/CMakeLists.txt
    - nes-physical-operators/include/Inference/IREEInferenceOperatorHandler.hpp
    - nes-physical-operators/include/Inference/IREEAdapter.hpp
    - nes-physical-operators/src/Inference/IREEInferenceOperatorHandler.cpp
    - nes-physical-operators/src/Inference/IREEAdapter.cpp
    - nes-logical-operators/src/Functions/ToBase64LogicalFunction.cpp
    - nes-logical-operators/src/Functions/FromBase64LogicalFunction.cpp
    - nes-logical-operators/src/Plans/LogicalPlanBuilder.cpp
    - nes-inference/include/ModelCatalog.hpp
    - nes-query-optimizer/src/LegacyOptimizer/ModelInferenceCompilationRule.cpp

key-decisions:
  - "Model type is NES::Nebuli::Inference::Model but IREEAdapter/IREEInferenceOperatorHandler forward-declared it as NES::Inference::Model — fixed with fully-qualified names"
  - "ModelCatalog::load() called setInputs/setOutputs which no longer exist on Model — removed calls since Model already loads shape info from ONNX"
  - "InferModelNameLogicalOperator and InferModelLogicalOperator now require inputFieldNames — updated callers with empty vec / op.getInputFieldNames()"
  - "SetUpTestSuite is required even in stub to initialize Logger before BaseUnitTest::TearDown forceFlush"

patterns-established:
  - "IREE-backed test targets guarded by if(ENABLE_IREE_TESTS) in CMakeLists"
  - "INFERENCE_TEST_DATA macro for testdata path in IREE test targets"

requirements-completed: [PHYS-01, PHYS-02, PHYS-03, PHYS-04, PHYS-07]

# Metrics
duration: 15min
completed: 2026-03-12
---

# Phase 3 Plan 01: CMake Infrastructure for InferModelPhysicalOperatorTest Summary

**IREE tool detection via find_program guards in CheckTestUtilities.cmake, conditional InferModelPhysicalOperatorTest target linking nes-inference + nes-iree-inference-plugin + nes-nautilus-test-util, with five pre-existing inference library build errors auto-fixed**

## Performance

- **Duration:** 15 min
- **Started:** 2026-03-12T19:20:25Z
- **Completed:** 2026-03-12T19:35:00Z
- **Tasks:** 1
- **Files modified:** 12

## Accomplishments
- CheckTestUtilities.cmake extended with `find_program` checks for `iree-import-onnx` and `iree-compile`, setting `ENABLE_IREE_TESTS` ON/OFF accordingly
- nes-physical-operators/tests/CMakeLists.txt extended with `add_nes_inference_physical_operator_test` helper and conditional `InferModelPhysicalOperatorTest` registration
- Stub test file created with `SetUpTestSuite` (Logger init) and `Placeholder` (GTEST_SKIP), confirmed discovered and skipped by ctest
- Five pre-existing build errors in nes-physical-operators, nes-logical-operators, nes-inference, and nes-query-optimizer fixed to unblock the target

## Task Commits

1. **Task 1: Add IREE tool checks and register test target** - `aa31ca60c8` (feat)

## Files Created/Modified
- `cmake/CheckTestUtilities.cmake` - Added ENABLE_IREE_TESTS find_program guard block
- `nes-physical-operators/tests/CMakeLists.txt` - Added if(ENABLE_IREE_TESTS) block with helper and test registration
- `nes-physical-operators/tests/InferModelPhysicalOperatorTest.cpp` - Stub test with SetUpTestSuite and Placeholder
- `nes-physical-operators/include/Inference/IREEInferenceOperatorHandler.hpp` - Fixed Model namespace (NES::Nebuli::Inference::Model)
- `nes-physical-operators/include/Inference/IREEAdapter.hpp` - Fixed Model forward-declaration and initializeModel signature
- `nes-physical-operators/src/Inference/IREEInferenceOperatorHandler.cpp` - Fixed Model namespace in ctor and getModel()
- `nes-physical-operators/src/Inference/IREEAdapter.cpp` - Fixed initializeModel parameter type
- `nes-logical-operators/src/Functions/ToBase64LogicalFunction.cpp` - Use DataTypeProvider::provideDataType for VARSIZED
- `nes-logical-operators/src/Functions/FromBase64LogicalFunction.cpp` - Use DataTypeProvider::provideDataType for VARSIZED
- `nes-logical-operators/src/Plans/LogicalPlanBuilder.cpp` - Pass empty inputFieldNames to InferModelNameLogicalOperator
- `nes-inference/include/ModelCatalog.hpp` - Remove setInputs/setOutputs calls (methods removed from Model)
- `nes-query-optimizer/src/LegacyOptimizer/ModelInferenceCompilationRule.cpp` - Pass inputFieldNames to InferModelLogicalOperator

## Decisions Made
- Stub test must call `Logger::setupLogging` in `SetUpTestSuite` — without it, `BaseUnitTest::TearDown` crashes on `Logger::getInstance()->forceFlush()` even when test is skipped

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed IREEAdapter and IREEInferenceOperatorHandler namespace mismatch**
- **Found during:** Task 1 (build attempt)
- **Issue:** Both headers forward-declared `Model` in `NES::Inference` namespace but `Model` lives in `NES::Nebuli::Inference`; compiler reported "unknown type name 'Model'"
- **Fix:** Updated all declarations and definitions to use `NES::Nebuli::Inference::Model`
- **Files modified:** IREEAdapter.hpp, IREEInferenceOperatorHandler.hpp, IREEAdapter.cpp, IREEInferenceOperatorHandler.cpp
- **Verification:** nes-physical-operators library compiled without errors
- **Committed in:** aa31ca60c8 (Task 1 commit)

**2. [Rule 1 - Bug] Fixed DataType single-argument construction in Base64 logical functions**
- **Found during:** Task 1 (build attempt, nes-logical-operators dependency)
- **Issue:** `DataType(DataType::Type::VARSIZED)` used single-arg ctor that no longer exists; `DataType` now requires `(Type, NULLABLE)` args
- **Fix:** Replace with `DataTypeProvider::provideDataType(DataType::Type::VARSIZED)`; added `#include <DataTypes/DataTypeProvider.hpp>`
- **Files modified:** ToBase64LogicalFunction.cpp, FromBase64LogicalFunction.cpp
- **Verification:** nes-logical-operators compiled without errors
- **Committed in:** aa31ca60c8 (Task 1 commit)

**3. [Rule 1 - Bug] Fixed LogicalPlanBuilder::addInferModel missing inputFieldNames**
- **Found during:** Task 1 (build attempt, nes-logical-operators dependency)
- **Issue:** `InferModelNameLogicalOperator` constructor now requires two args but `addInferModel` passed only `modelName`
- **Fix:** Pass empty `{}` for `inputFieldNames` (plan builder API has no field names at this level)
- **Files modified:** LogicalPlanBuilder.cpp
- **Verification:** nes-logical-operators compiled without errors
- **Committed in:** aa31ca60c8 (Task 1 commit)

**4. [Rule 1 - Bug] Fixed ModelCatalog::load calling setInputs/setOutputs that no longer exist**
- **Found during:** Task 1 (build attempt, nes-query-optimizer dependency)
- **Issue:** ModelCatalog called `result->setInputs()` and `result->setOutputs()` but `Model` has no such setters
- **Fix:** Removed those two calls; the loaded model already has shape info from ONNX parsing
- **Files modified:** ModelCatalog.hpp
- **Verification:** nes-query-optimizer compiled without errors
- **Committed in:** aa31ca60c8 (Task 1 commit)

**5. [Rule 1 - Bug] Fixed ModelInferenceCompilationRule missing inputFieldNames for InferModelLogicalOperator**
- **Found during:** Task 1 (build attempt, nes-query-optimizer dependency)
- **Issue:** `InferModelLogicalOperator` constructor requires `inputFieldNames` but rule passed only model
- **Fix:** Propagate `op.getInputFieldNames()` from the source `InferModelNameLogicalOperator`
- **Files modified:** ModelInferenceCompilationRule.cpp
- **Verification:** nes-query-optimizer compiled without errors
- **Committed in:** aa31ca60c8 (Task 1 commit)

---

**Total deviations:** 5 auto-fixed (all Rule 1 bugs)
**Impact on plan:** All fixes were pre-existing compilation errors in worktree that became visible when building the new test target's transitive dependencies. No scope creep — all fixes are in inference/operator code directly related to the InferModel feature.

## Issues Encountered
- Stub test crashed with SEGFAULT in ctest because `BaseUnitTest::TearDown` calls `Logger::getInstance()->forceFlush()` unconditionally — resolved by adding `SetUpTestSuite` with `Logger::setupLogging` to the fixture.

## Next Phase Readiness
- `InferModelPhysicalOperatorTest` target builds and links correctly with all required libraries
- `INFERENCE_TEST_DATA` macro points to `nes-inference/tests/testdata/` for .onnx model loading
- Ready for Plan 02 to replace the `Placeholder` test with real lifecycle, correctness, and edge-case tests

## Self-Check: PASSED

All files exist and commit aa31ca60c8 is present in git history.

---
*Phase: 03-physical-operator-tests*
*Completed: 2026-03-12*
