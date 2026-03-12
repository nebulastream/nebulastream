# Requirements: ML Operator Test Coverage

**Defined:** 2026-03-12
**Core Value:** Every public method and error path in the ML inference operators has a corresponding unit test

## v1 Requirements

### Infrastructure

- [x] **INFRA-01**: Cherry-pick TestTupleBuffer abstraction (40e18d4a36) for schema-aware buffer access in tests
- [x] **INFRA-02**: Create tiny identity .vmfb test model (shape [1,2] float32 in→out, compiled with iree-base-compiler 3.10.0)
- [x] **INFRA-03**: Fix IREERuntimeWrapper destructor to release IREE handles (instance, session, device)
- [x] **INFRA-04**: CMake registration for new test executables via add_nes_unit_test / add_nes_physical_operator_test

### Logical Operator

- [x] **LOGI-01**: Test InferModelLogicalOperator construction with valid Model
- [x] **LOGI-02**: Test explain() returns meaningful string describing the operator
- [x] **LOGI-03**: Test getChildren/withChildren round-trip preserves children
- [x] **LOGI-04**: Test withTraitSet/getTraitSet correctly propagates traits
- [x] **LOGI-05**: Test operator== equality semantics (same model, same children → equal)
- [x] **LOGI-06**: Test getName returns correct operator identifier
- [x] **LOGI-07**: Test getInputSchemas/getOutputSchema propagation
- [x] **LOGI-08**: Test InferModelNameLogicalOperator (name variant construction and explain)

### Schema Inference

- [x] **SCHEMA-01**: Test withInferredSchema appends model output fields to input schema
- [x] **SCHEMA-02**: Test output field ordering is preserved (positional correctness)
- [x] **SCHEMA-03**: Test error on empty input schema
- [x] **SCHEMA-04**: Test error on missing required input fields
- [x] **SCHEMA-05**: Test error on type mismatch between schema and model input expectations
- [x] **SCHEMA-06**: Test with multiple output fields
- [x] **SCHEMA-07**: Test schema with pre-existing fields of same name as output fields

### Physical Operator

- [ ] **PHYS-01**: Test full lifecycle: setup→open→execute→close→terminate with identity model
- [ ] **PHYS-02**: Test numerical correctness (output == input for identity model)
- [ ] **PHYS-03**: Test field extraction from TupleBuffer using TestTupleBuffer API
- [ ] **PHYS-04**: Test multi-tuple buffer processing (multiple records per buffer)
- [ ] **PHYS-05**: Test error when handler not started (setup skipped)
- [ ] **PHYS-06**: Test idempotent terminate (calling terminate twice doesn't crash)
- [ ] **PHYS-07**: Test execute with empty buffer (zero tuples)

## v2 Requirements

### Compilation Rules

- **COMP-01**: Test ModelInferenceCompilationRule resolves name→model via ModelCatalog
- **COMP-02**: Test LowerToPhysicalInferModel creates correct physical operator
- **COMP-03**: Test lowering preserves input/output field names from model metadata

## Out of Scope

| Feature | Reason |
|---------|--------|
| System/E2E test changes | Existing MNIST systest is sufficient |
| Performance benchmarks | Not part of this testing initiative |
| Operator implementation changes (beyond destructor fix) | Test-only additions |
| Mock-based physical operator tests | User chose real test model approach |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| INFRA-01 | Phase 1 | Complete |
| INFRA-03 | Phase 1 | Complete |
| INFRA-04 | Phase 1 | Complete |
| LOGI-01 | Phase 1 | Complete |
| LOGI-02 | Phase 1 | Complete |
| LOGI-03 | Phase 1 | Complete |
| LOGI-04 | Phase 1 | Complete |
| LOGI-05 | Phase 1 | Complete |
| LOGI-06 | Phase 1 | Complete |
| LOGI-07 | Phase 1 | Complete |
| LOGI-08 | Phase 1 | Complete |
| SCHEMA-01 | Phase 1 | Complete |
| SCHEMA-02 | Phase 1 | Complete |
| SCHEMA-03 | Phase 1 | Complete |
| SCHEMA-04 | Phase 1 | Complete |
| SCHEMA-05 | Phase 1 | Complete |
| SCHEMA-06 | Phase 1 | Complete |
| SCHEMA-07 | Phase 1 | Complete |
| INFRA-02 | Phase 2 | Complete |
| PHYS-01 | Phase 3 | Pending |
| PHYS-02 | Phase 3 | Pending |
| PHYS-03 | Phase 3 | Pending |
| PHYS-04 | Phase 3 | Pending |
| PHYS-05 | Phase 3 | Pending |
| PHYS-06 | Phase 3 | Pending |
| PHYS-07 | Phase 3 | Pending |

**Coverage:**
- v1 requirements: 22 total
- Mapped to phases: 22
- Unmapped: 0 ✓

---
*Requirements defined: 2026-03-12*
*Last updated: 2026-03-12 after roadmap creation (coarse 3-phase structure)*
