# ML Operator Test Coverage

## What This Is

A test improvement initiative for the NebulaStream ML inference branch (`ls/ml-operator`). The goal is to add exhaustive unit tests for the ML logical and physical operators, covering construction, schema inference, execution lifecycle, and all error paths.

## Core Value

Every public method and error path in the ML inference operators has a corresponding unit test, ensuring correctness and preventing regressions as the ML feature matures toward merge.

## Requirements

### Validated

- ✓ InferModelLogicalOperator exists with schema inference — existing
- ✓ InferModelPhysicalOperator exists with IREE execution — existing
- ✓ ModelInferenceCompilationRule resolves model names — existing
- ✓ LowerToPhysicalInferModel lowering rule — existing
- ✓ GTest + BaseUnitTest infrastructure available — existing
- ✓ System-level MNIST inference test exists — existing

### Active

- [ ] Unit tests for InferModelLogicalOperator (construction, explain, equality, children, traits)
- [ ] Unit tests for schema inference (output fields appended, error cases, type mismatches, missing fields)
- [ ] Unit tests for InferModelPhysicalOperator (setup, execute, open/close lifecycle, terminate)
- [ ] Unit tests for physical operator error paths (invalid model, missing fields, malformed input)
- [ ] Create a minimal test model for unit tests (tiny IREE .vmfb, deterministic inputs/outputs)
- [ ] Exhaustive branch coverage (~95%+) across all ML operator code

### Out of Scope

- Compilation rule tests (ModelInferenceCompilationRule, LowerToPhysicalInferModel) — user chose logical + physical only
- System/E2E test changes — existing MNIST systest is sufficient
- Performance benchmarks — not part of this testing initiative
- Changes to operator implementation code — test-only additions

## Context

- Branch: `ls/ml-operator` (worktree at `/tmp/worktrees/ls-ml-operator/`)
- The ML operators follow NebulaStream's LogicalOperatorConcept and PhysicalOperatorConcept patterns
- Existing test patterns: inherit from `Testing::BaseUnitTest`, use TEST_F fixtures, GTest assertions
- Physical operator tests use custom stub implementations rather than GMock (codebase convention)
- Test registration via CMake: `add_nes_unit_test()` macro in `cmake/NebulaStreamTest.cmake`
- No dedicated unit tests exist yet for any ML operator — only a system-level MNIST test
- A tiny test model needs to be created (not the full MNIST model) for fast, deterministic unit tests

## Constraints

- **Test framework**: Must use GTest + BaseUnitTest (codebase standard)
- **Test model**: Must create a minimal .vmfb model rather than using the large MNIST model
- **Coverage**: Target ~95%+ branch coverage on ML operator code
- **No mocking runtime**: Use real (tiny) IREE model for physical operator tests, not mocked handler
- **C++23**: All test code must comply with project C++23 standard

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Use real test model, not mocks | User preference; tests actual IREE integration paths | — Pending |
| Create tiny purpose-built model | Full MNIST too large/slow for unit tests | — Pending |
| Skip compilation rule tests | Scope limited to logical + physical operators | — Pending |
| Target exhaustive coverage | Ensure merge readiness with high confidence | — Pending |

---
*Last updated: 2026-03-11 after initialization*
