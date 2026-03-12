---
phase: 01-infrastructure-and-logical-tests
plan: 02
subsystem: logical-operators
tags: [cpp, cmake, ml-inference, logical-operators, unit-tests, protobuf]

# Dependency graph
requires:
  - "nes-inference (Model class, deserializeModel)"
  - "nes-grpc (SerializableOperator.pb.h)"
  - "LogicalOperatorConcept (nes-logical-operators)"
provides:
  - "InferModelLogicalOperator value type satisfying LogicalOperatorConcept"
  - "InferModelNameLogicalOperator value type satisfying LogicalOperatorConcept"
  - "nes-inference library (Model, deserializeModel, serializeModel)"
  - "SerializableOperator.proto (Model sub-message)"
  - "LOGI-01 through LOGI-08 unit tests passing"
affects:
  - "01-03 (schema inference tests use InferModelLogicalOperator directly)"
  - "03-01 (physical operator tests need logical operators defined)"

# Tech tracking
tech-stack:
  added:
    - "nes-inference static library (Model.hpp, Model.cpp, CMakeLists.txt)"
    - "SerializableOperator.proto (minimal — Model sub-message only)"
  patterns:
    - "Value-type operator following SelectionLogicalOperator pattern exactly"
    - "Reflector serializes Model to proto bytes (std::string) for rfl reflection"
    - "withInferredSchema: validate field count, check type compatibility, append/replace outputs"
    - "replaceTypeOfField for name collision; addField for new outputs"

key-files:
  created:
    - "grpc/SerializableOperator.proto — minimal proto with Model sub-message"
    - "nes-inference/CMakeLists.txt — add_library(nes-inference Model.cpp)"
    - "nes-inference/include/Model.hpp — Model class with DataType (value, not shared_ptr)"
    - "nes-inference/Model.cpp — deserializeModel/serializeModel implementations"
    - "nes-logical-operators/include/Operators/InferModelLogicalOperator.hpp"
    - "nes-logical-operators/src/Operators/InferModelLogicalOperator.cpp"
    - "nes-logical-operators/include/Operators/InferModelNameLogicalOperator.hpp"
    - "nes-logical-operators/src/Operators/InferModelNameLogicalOperator.cpp"
    - "nes-logical-operators/tests/InferModelLogicalOperatorTest.cpp"
  modified:
    - "nes-logical-operators/CMakeLists.txt — added nes-inference to PUBLIC link libs"
    - "nes-logical-operators/src/Operators/CMakeLists.txt — add_plugin for InferModel and InferModelName"
    - "nes-logical-operators/tests/CMakeLists.txt — added infer-model-logical-operator-test"

key-decisions:
  - "DataType stored as value type (not shared_ptr) in Model — adapted from ls-inference which used shared_ptr<DataType>"
  - "SerializableOperator.proto uses only the Model sub-message; full ls-inference proto omitted (required SerializableFunction.proto and SerializableSchema.proto not present in this worktree)"
  - "Reflector<InferModelLogicalOperator> serializes Model to protobuf bytes (std::string) stored in ReflectedInferModelLogicalOperator struct"
  - "nes-inference created as standalone directory in worktree (not cherry-picked from ls-inference due to API mismatch)"

requirements-completed: [INFRA-04, LOGI-01, LOGI-02, LOGI-03, LOGI-04, LOGI-05, LOGI-06, LOGI-07, LOGI-08]

# Metrics
duration: 20min
completed: 2026-03-12
---

# Phase 1 Plan 2: InferModel Logical Operators and Tests Summary

**InferModelLogicalOperator and InferModelNameLogicalOperator implemented as value types satisfying LogicalOperatorConcept; all 8 LOGI unit tests pass**

## Performance

- **Duration:** ~20 min
- **Started:** 2026-03-12T12:29:00Z
- **Completed:** 2026-03-12T12:49:39Z
- **Tasks:** 2
- **Files modified:** 13

## Accomplishments

- `InferModelLogicalOperator` value type satisfying `LogicalOperatorConcept` — static_assert passes at compile time
- `InferModelNameLogicalOperator` value type satisfying `LogicalOperatorConcept` — withInferredSchema throws CannotInferSchema
- `nes-inference` library with `Model`, `deserializeModel`, `serializeModel` created in worktree
- `SerializableOperator.proto` with `Model` sub-message added to grpc/ directory (auto-picked up by nes-common CMakeLists glob)
- Both operators registered via `add_plugin` in CMakeLists.txt
- All 8 LOGI tests pass: construction, explain, withChildren, withTraitSet, equality, getName, schema propagation, name variant

## Task Commits

1. **Task 1: Create operators and infrastructure** — `e8a7fcedd1`
2. **Task 2: Write all LOGI unit tests** — `c853751306`

## Files Created/Modified

- `grpc/SerializableOperator.proto` — minimal proto with Model sub-message (SerializableDataType import only)
- `nes-inference/CMakeLists.txt` — standalone `nes-inference` static library
- `nes-inference/include/Model.hpp` — Model class with value-type DataType members
- `nes-inference/Model.cpp` — deserializeModel / serializeModel implementations
- `nes-logical-operators/include/Operators/InferModelLogicalOperator.hpp` — operator header with static_assert
- `nes-logical-operators/src/Operators/InferModelLogicalOperator.cpp` — full implementation + Reflector/Unreflector/Register
- `nes-logical-operators/include/Operators/InferModelNameLogicalOperator.hpp` — name variant header with static_assert
- `nes-logical-operators/src/Operators/InferModelNameLogicalOperator.cpp` — name variant implementation
- `nes-logical-operators/CMakeLists.txt` — added `nes-inference` to PUBLIC link libraries
- `nes-logical-operators/src/Operators/CMakeLists.txt` — add_plugin for InferModel and InferModelName
- `nes-logical-operators/tests/CMakeLists.txt` — added `infer-model-logical-operator-test` target
- `nes-logical-operators/tests/InferModelLogicalOperatorTest.cpp` — 8 GTest cases (LOGI-01 through LOGI-08)

## Decisions Made

- **DataType as value (not shared_ptr):** The ls-inference `Model.hpp` uses `shared_ptr<NES::DataType>` internally, but the current branch's `DataType` is a plain value struct with `operator==` and trivial copy. Using value types directly avoids the impedance mismatch.

- **Minimal SerializableOperator.proto:** The full ls-inference version imports `SerializableFunction.proto` and `SerializableSchema.proto` which don't exist in this worktree. A minimal proto with only the `SerializableOperator.Model` message (importing only `SerializableDataType.proto` which exists) was used instead.

- **Reflector serializes model to proto bytes:** `ReflectedInferModelLogicalOperator` stores `optional<string> modelBytes` (protobuf-serialized) and `optional<vector<string>> inputFieldNames`. Both are trivially rfl-serializable. This avoids requiring a custom rfl::Reflector for `NES::Nebuli::Inference::Model`.

- **nes-inference created standalone:** The `nes-inference` directory from ls-inference has API mismatches with the current DataType (shared_ptr vs value type) and different include paths. Created from scratch with the needed adaptations.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] SerializableFunction.proto and SerializableSchema.proto not present**
- **Found during:** Task 1 (proto dependency analysis)
- **Issue:** `SerializableOperator.proto` from ls-inference imports two proto files not in this worktree. Adding them would pull in unrelated protobuf message definitions.
- **Fix:** Created a minimal `SerializableOperator.proto` containing only the `SerializableOperator.Model` nested message, importing only the already-present `SerializableDataType.proto`.
- **Files modified:** `grpc/SerializableOperator.proto`
- **Committed in:** `e8a7fcedd1`

**2. [Rule 1 - Bug] DataType API mismatch: shared_ptr vs value type**
- **Found during:** Task 1 (reading ls-inference Model.hpp vs current DataType.hpp)
- **Issue:** ls-inference Model.hpp uses `shared_ptr<NES::DataType>` but current branch has plain `DataType` struct.
- **Fix:** Adapted Model.hpp and Model.cpp to use `NES::DataType` values directly. Updated `DataTypeSerializationUtil` calls accordingly.
- **Files modified:** `nes-inference/include/Model.hpp`, `nes-inference/Model.cpp`
- **Committed in:** `e8a7fcedd1`

**3. [Rule 1 - Bug] nodiscard on replaceTypeOfField**
- **Found during:** Task 1 build (compile error)
- **Issue:** `Schema::replaceTypeOfField` is `[[nodiscard]]` — return value must be captured. The mutation is in-place (not chained), so `[[maybe_unused]] bool` is correct.
- **Fix:** Added `[[maybe_unused]] bool replaced = copy.outputSchema.replaceTypeOfField(name, dataType);`
- **Files modified:** `nes-logical-operators/src/Operators/InferModelLogicalOperator.cpp`
- **Committed in:** `e8a7fcedd1`

---

**Total deviations:** 3 auto-fixed (1 Rule 3, 2 Rule 1)
**Impact:** All three were necessary for the build to succeed. No scope creep.

## Self-Check

Files verified present:
- `/tmp/worktrees/ls-ml-operator/nes-inference/include/Model.hpp` ✓
- `/tmp/worktrees/ls-ml-operator/nes-logical-operators/include/Operators/InferModelLogicalOperator.hpp` ✓
- `/tmp/worktrees/ls-ml-operator/nes-logical-operators/include/Operators/InferModelNameLogicalOperator.hpp` ✓
- `/tmp/worktrees/ls-ml-operator/nes-logical-operators/tests/InferModelLogicalOperatorTest.cpp` ✓

Commits verified:
- `e8a7fcedd1` — Task 1 feat ✓
- `c853751306` — Task 2 feat ✓

Test results: 8/8 LOGI tests passed ✓

## Self-Check: PASSED

---
*Phase: 01-infrastructure-and-logical-tests*
*Completed: 2026-03-12*
