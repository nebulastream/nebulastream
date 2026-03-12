# Roadmap: ML Operator Test Coverage

## Overview

Three phases deliver exhaustive unit tests for the ML inference operators. Phase 1 establishes the test infrastructure and covers the entire logical operator and schema inference layer — no IREE dependency, so it runs immediately. Phase 2 creates the tiny `.vmfb` test model artifact that is the hard prerequisite for any physical operator test. Phase 3 uses that artifact to cover the physical operator's full lifecycle, numerical correctness, and all error and edge-case paths.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [ ] **Phase 1: Infrastructure and Logical Tests** - Cherry-pick TestTupleBuffer, fix IREERuntimeWrapper destructor, wire CMake, and write all logical operator and schema inference unit tests
- [ ] **Phase 2: Test Model Artifact** - Compile and commit the tiny identity .vmfb model needed by all physical operator tests
- [ ] **Phase 3: Physical Operator Tests** - Write the full physical operator test suite covering lifecycle, correctness, errors, and edge cases

## Phase Details

### Phase 1: Infrastructure and Logical Tests
**Goal**: The test infrastructure is wired up, the IREERuntimeWrapper destructor is fixed, and every public method and error path of the logical operator and schema inference layer has a passing unit test
**Depends on**: Nothing (first phase)
**Requirements**: INFRA-01, INFRA-03, INFRA-04, LOGI-01, LOGI-02, LOGI-03, LOGI-04, LOGI-05, LOGI-06, LOGI-07, LOGI-08, SCHEMA-01, SCHEMA-02, SCHEMA-03, SCHEMA-04, SCHEMA-05, SCHEMA-06, SCHEMA-07
**Success Criteria** (what must be TRUE):
  1. `InferModelLogicalOperatorTest` builds and all tests pass under AddressSanitizer with zero leaks or errors
  2. Every public method on `InferModelLogicalOperator` (getName, explain, getChildren, withChildren, getTraitSet, withTraitSet, operator==, getInputSchemas, getOutputSchema) has at least one test exercising its contract
  3. `withInferredSchema` happy path tests assert output fields by name and index (not just count), confirming positional correctness
  4. All schema inference error paths (empty schema, missing field, type mismatch, name collision) produce the expected exception or error code rather than crashing
**Plans:** 2/3 plans executed
Plans:
- [x] 01-01-PLAN.md — Cherry-pick TestTupleBuffer and fix IREERuntimeWrapper destructor
- [x] 01-02-PLAN.md — Create InferModel operator implementations and write LOGI unit tests
- [ ] 01-03-PLAN.md — Write all schema inference unit tests (SCHEMA-01 through SCHEMA-07)

### Phase 2: Test Model Artifact
**Goal**: A committed, pre-compiled tiny identity `.vmfb` model exists in the repository and can be loaded by the IREE runtime at test time without requiring the compiler toolchain in CI
**Depends on**: Phase 1
**Requirements**: INFRA-02
**Success Criteria** (what must be TRUE):
  1. `nes-inference/tests/testdata/tiny_identity.vmfb` exists in the repository and is under 10 KB
  2. A generation script (`scripts/create_test_model.py`) is committed alongside the artifact with documented commands and the exact `iree-base-compiler==3.10.0` pin
  3. The artifact loads successfully via `iree_runtime_session_append_bytecode_module_from_memory` with no status error on the CI platform (i.e., compiled with correct CPU target flags for CI)
**Plans**: TBD

### Phase 3: Physical Operator Tests
**Goal**: Every public method, execution lifecycle path, and error condition of `InferModelPhysicalOperator` has a passing unit test running under the Nautilus interpreted backend with a real IREE model
**Depends on**: Phase 2
**Requirements**: PHYS-01, PHYS-02, PHYS-03, PHYS-04, PHYS-05, PHYS-06, PHYS-07
**Success Criteria** (what must be TRUE):
  1. The full lifecycle test (setup → open → execute → close → terminate) passes with no AddressSanitizer errors, confirming the `IREERuntimeWrapper` destructor correctly releases all IREE handles
  2. The numerical correctness test asserts `EXPECT_NEAR` on every output field against known-good expected values from the identity model
  3. Multi-tuple buffer processing (multiple records per buffer) produces per-record correct outputs
  4. Error-path tests for setup skipped, invalid model, and zero-tuple buffer each produce the expected behavior (exception or no-op) without undefined behavior or crashes
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Infrastructure and Logical Tests | 2/3 | In Progress|  |
| 2. Test Model Artifact | 0/? | Not started | - |
| 3. Physical Operator Tests | 0/? | Not started | - |
