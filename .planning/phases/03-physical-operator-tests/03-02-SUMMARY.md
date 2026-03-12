---
phase: 03-physical-operator-tests
plan: "02"
subsystem: testing
tags: [iree, onnx, physical-operators, inference, nautilus, cpp, gtest]

# Dependency graph
requires:
  - phase: 03-physical-operator-tests
    plan: "01"
    provides: InferModelPhysicalOperatorTest CMake target, stub test file, INFERENCE_TEST_DATA macro
  - phase: 02-test-model-artifact
    provides: tiny_identity.onnx, tiny_reduction.onnx, tiny_expansion.onnx in testdata/
  - phase: 01-infrastructure-and-logical-tests
    provides: InferModelPhysicalOperator, IREEInferenceOperatorHandler, IREEAdapter implementations

provides:
  - Complete InferModelPhysicalOperator test suite (7 passing tests)
  - Lifecycle test (PHYS-01): setup->execute->terminate without crash or ASAN errors
  - Identity correctness (PHYS-02/03): 100-field output matches 100-float input through Relu
  - Reduction correctness (PHYS-02): 10 outputs all zero from all-zero Gemm weight matrix
  - Expansion correctness (PHYS-02): 100 outputs all zero from all-zero Gemm weight matrix
  - Multi-record correctness (PHYS-04): 5-record buffer processed correctly for all 3 models
  - Zero-tuple edge case (PHYS-07): empty buffer causes no crash and no emitted buffer

affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Two-schema pattern: inputSchema for TupleBuffer population, same schema for bufRef (output fields written to Record in-memory by execute(), not back to TupleBuffer)"
    - "NoOpChildOperator as terminal child required by InferModelPhysicalOperator::setupChild/terminateChild/executeChild"
    - "readRecord with explicit projection list — empty {} means no fields included (not all fields)"
    - "Static model loading in SetUpTestSuite with NES::Inference::enabled() guard"
    - "packFloatsToVarsized: reinterpret_cast float data to char* -> std::string for VARSIZED field"

key-files:
  created: []
  modified:
    - nes-physical-operators/tests/InferModelPhysicalOperatorTest.cpp

key-decisions:
  - "NoOpChildOperator required: InferModelPhysicalOperator::setup() calls setupChild() which INVARIANT(getChild().has_value()). Must attach no-op child before calling setup/terminate."
  - "inputSchema only for buffer + bufRef: output FLOAT32 fields are written to Record in-memory by execute(), not stored back to TupleBuffer. Use inputSchema (VARSIZED only) for both TestTupleBuffer and LowerSchemaProvider bufRef."
  - "readRecord projection must be explicit: readRecord({}) with empty vector means NO fields included (includesField always returns false for empty projections). Must pass {\"input_blob\"} explicitly."
  - "PHYS-05 and PHYS-06 confirmed DROPPED: framework guarantees ordering and single terminate — not testable at unit level."

patterns-established:
  - "Extract raw float from VarVal: record.read(name).cast<nautilus::val<float>>() then nautilus::details::RawValueResolver<float>::getRawValue()"
  - "runVarsizedInference helper: encapsulates full lifecycle, returns vector of Records for assertions"

requirements-completed: [PHYS-01, PHYS-02, PHYS-03, PHYS-04, PHYS-07]

# Metrics
duration: 8min
completed: 2026-03-12
---

# Phase 3 Plan 02: InferModelPhysicalOperator Test Suite Summary

**7 tests verifying the full InferModelPhysicalOperator lifecycle, numerical correctness for 3 ONNX models (identity/reduction/expansion), 5-record multi-buffer processing, and zero-tuple edge case — all passing with real IREE inference**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-12T19:40:37Z
- **Completed:** 2026-03-12T19:49:04Z
- **Tasks:** 2 (implemented together as single commit)
- **Files modified:** 1

## Accomplishments
- Complete InferModelPhysicalOperatorTest.cpp with 7 passing tests covering all plan requirements
- Full lifecycle test confirms setup->execute->terminate with no crash or ASAN errors (PHYS-01)
- Identity model correctness: all 100 float outputs match 100 float inputs through Relu (PHYS-02/03)
- Reduction model correctness: 10 outputs all zero from all-zero-weight 100->10 Gemm (PHYS-02)
- Expansion model correctness: 100 outputs all zero from all-zero-weight 10->100 Gemm (PHYS-02)
- Multi-record buffer processing: 5 distinct records correctly processed for all 3 models (PHYS-04)
- Zero-tuple edge case: empty buffer runs setup+terminate cleanly with no emitted output (PHYS-07)

## Task Commits

1. **Task 1 + Task 2: Write fixture, helpers, lifecycle, correctness, multi-record and zero-tuple tests** - `31d34ca45a` (feat)

## Files Created/Modified
- `nes-physical-operators/tests/InferModelPhysicalOperatorTest.cpp` - Full test suite replacing stub (442 new lines: MockedPipelineContext, NoOpChildOperator, SetUpTestSuite with static model loading, runVarsizedInference helper, 7 TEST_F tests)

## Decisions Made
- **NoOpChildOperator as terminal child**: `InferModelPhysicalOperator::setup()` calls `setupChild()` which asserts `getChild().has_value()`. A minimal `NoOpChildOperator` with all no-op overrides satisfies this without any output side effects.
- **Input-only schema for both TestTupleBuffer and bufRef**: Output fields are written by `execute()` into the `Record`'s in-memory `recordFields` map (not back to the TupleBuffer). Using only the VARSIZED input schema for both avoids tuple-size mismatch between write and read layouts.
- **Explicit projection in readRecord**: Passing `{}` to `readRecord` includes NO fields (empty range find always false). Must pass `{"input_blob"}` explicitly to get the VARSIZED input field into the Record.
- **Raw float extraction**: Use `nautilus::details::RawValueResolver<float>::getRawValue(varval.cast<nautilus::val<float>>())` to extract a raw `float` from a `VarVal` for EXPECT_NEAR assertions.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed PhysicalOperatorConcept::setupChild INVARIANT crash**
- **Found during:** Task 1 (first test run)
- **Issue:** `InferModelPhysicalOperator::setup()` calls `setupChild()` which calls `INVARIANT(getChild().has_value())`. The plan did not mention that a child operator is required.
- **Fix:** Added `NoOpChildOperator` struct implementing `PhysicalOperatorConcept` with no-op setup/execute/terminate, and attached it via `op.setChild(PhysicalOperator(NoOpChildOperator{}))` before calling `op.setup()`.
- **Files modified:** InferModelPhysicalOperatorTest.cpp
- **Verification:** setup() no longer crashes; all lifecycle calls complete successfully
- **Committed in:** 31d34ca45a

**2. [Rule 1 - Bug] Fixed TestTupleBuffer schema mismatch (101-field append with 1 arg)**
- **Found during:** Task 2 (CorrectnessExpansion test run)
- **Issue:** Initial design used a "full schema" (1 VARSIZED + 100 FLOAT32) for TestTupleBuffer, causing `append` to require 101 arguments but only 1 was provided.
- **Fix:** Switched to "input-only schema" (VARSIZED only) for both TestTupleBuffer population and LowerSchemaProvider bufRef. Output fields exist only in the Record's in-memory map after execute().
- **Files modified:** InferModelPhysicalOperatorTest.cpp
- **Verification:** TestTupleBuffer::append(packed_floats) succeeds with 1 argument
- **Committed in:** 31d34ca45a

**3. [Rule 1 - Bug] Fixed empty projection ({}) returning empty Record from readRecord**
- **Found during:** Task 1 (test run after schema fix)
- **Issue:** `bufRef->readRecord({}, recordBuffer, idx)` with empty projections returns an empty Record because `includesField` with empty vector always returns false. InferModelPhysicalOperator::execute() then throws "Field input_blob not found in record".
- **Fix:** Changed projection to `{"input_blob"}` to explicitly include the VARSIZED input field.
- **Files modified:** InferModelPhysicalOperatorTest.cpp
- **Verification:** record.read("input_blob") succeeds; inference runs correctly
- **Committed in:** 31d34ca45a

---

**Total deviations:** 3 auto-fixed (all Rule 1 bugs — missing/incorrect API knowledge at design time)
**Impact on plan:** All fixes were necessary to make the tests runnable. No scope creep. The test logic (lifecycle, correctness assertions, multi-record, zero-tuple) was implemented exactly as planned.

## Issues Encountered
- `readRecord({})` documentation mismatch: header comment says "If {}, then Record contains all fields available" but implementation `includesField(empty, name)` returns false. Resolved by explicit projection.

## Next Phase Readiness
- Phase 3 complete: all 7 tests pass with real IREE inference
- All covered requirements: PHYS-01 through PHYS-04, PHYS-07
- PHYS-05 and PHYS-06 confirmed DROPPED (not testable at unit level)
- No blockers for downstream consumers

## Self-Check: PASSED

File `nes-physical-operators/tests/InferModelPhysicalOperatorTest.cpp` exists and commit `31d34ca45a` is present in git history.

---
*Phase: 03-physical-operator-tests*
*Completed: 2026-03-12*
