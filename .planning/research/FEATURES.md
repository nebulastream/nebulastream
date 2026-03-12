# Feature Research: ML Inference Operator Test Cases

**Domain:** Unit test suite for ML inference logical and physical operators in a C++ stream processing engine
**Researched:** 2026-03-11
**Confidence:** HIGH — derived directly from codebase inspection of existing operator patterns (SelectionLogicalOperator, EmitPhysicalOperator, SliceAssignerTest, EmitPhysicalOperatorTest, LogicalPlanTest) and the LogicalOperatorConcept / PhysicalOperatorConcept interfaces.

---

## Feature Landscape

### Table Stakes (Must Have — Missing = Coverage Is Incomplete)

These are the test cases that anyone reviewing "do we have exhaustive unit tests for the ML operators?" will immediately check. Missing any of these means the test suite is incomplete by definition.

| Test Area | Why Expected | Complexity | Notes |
|-----------|--------------|------------|-------|
| **Logical: construction with valid inputs** | Every operator test starts here; verifies basic API contract | LOW | Construct `InferModelLogicalOperator` with a model name and valid input/output field lists. Assert `getName()` returns the correct constant. |
| **Logical: getName() returns expected constant** | `LogicalOperatorConcept` requires `getName() noexcept`; trivial to test, critical to assert | LOW | Simple string comparison. Pattern: `EXPECT_EQ(op.getName(), "InferModel")` |
| **Logical: getChildren() on fresh operator returns empty** | All existing logical operators start childless; failing here indicates state leak | LOW | `EXPECT_TRUE(op.getChildren().empty())` |
| **Logical: withChildren() produces operator with correct children** | `LogicalOperatorConcept` requires this; parent plan-building depends on it | LOW | Set one or more child operators, verify `getChildren()` returns them. Immutability: original must be unchanged. |
| **Logical: withChildren() is immutable (original unchanged)** | All NebulaStream logical operators use value-semantic immutability; bugs here corrupt plan rewriting | LOW | After `withChildren(...)`, assert original still has empty children. |
| **Logical: equality — equal operators** | `LogicalOperatorConcept` requires `operator==`; optimizer deduplication relies on it | LOW | Two operators with same model name and same inferred schema must be equal. |
| **Logical: equality — unequal operators (different model name)** | Same as above; false negatives hide bugs in plan caching | LOW | Different model name = not equal. |
| **Logical: explain(Short) returns non-empty string** | Used in logging and plan rendering; must not crash or return empty | LOW | `EXPECT_FALSE(op.explain(ExplainVerbosity::Short, id).empty())` |
| **Logical: explain(Debug) returns non-empty string containing model name** | Debug output used in diagnostics | LOW | Assert model name appears in debug explain string. |
| **Logical: withTraitSet() / getTraitSet() round-trip** | `LogicalOperatorConcept` requires this; optimizer uses traits for planning decisions | LOW | Set a trait set, verify round-trip. Pattern copied from `LogicalPlanTest::AddTraits`. |
| **Logical: schema inference — happy path appends output fields** | Core correctness: ML inference adds output fields to the input schema | MEDIUM | Provide input schema with N fields and model that outputs M fields; verify output schema has N+M fields with correct names/types. |
| **Logical: schema inference — output schema preserves input fields** | Output fields must be appended, not replace existing fields (stream processing contract) | MEDIUM | All input fields must appear in output schema unchanged. |
| **Logical: getInputSchemas() after withInferredSchema()** | Required by `LogicalOperatorConcept`; consumers rely on this for compilation | LOW | Assert returns the schema that was passed to `withInferredSchema`. |
| **Logical: getOutputSchema() after withInferredSchema()** | Required by `LogicalOperatorConcept`; compilation uses this to build downstream schemas | LOW | Assert returns schema with original + appended output fields. |
| **Logical: schema inference — empty input schema throws** | All other operators (`SelectionLogicalOperator`) throw `CannotDeserialize` on empty; contract must match | MEDIUM | `EXPECT_THROW(op.withInferredSchema({}), ...)` or `ASSERT_EXCEPTION_ERRORCODE(...)` |
| **Logical: schema inference — missing required input field throws** | Model declares field "x" required; input schema lacks "x" — must reject, not silently corrupt | MEDIUM | Key error path; silent failure here causes runtime crashes at inference time |
| **Logical: schema inference — field type mismatch throws** | Model expects float, input schema has int64 for same field name — must reject with clear error | MEDIUM | Common real-world mistake; untested means silent data corruption at IREE layer |
| **Physical: construction succeeds with valid model path and field descriptors** | Physical operator tests always start with constructability | LOW | Requires the tiny test `.vmfb` model artifact. |
| **Physical: setup() does not throw with valid OperatorHandler registered** | `setup()` is the IREE model loading phase; this is the first real IREE interaction | MEDIUM | Requires stub `ExecutionContext` and `CompilationContext` (same pattern as `EmitPhysicalOperatorTest::MockedPipelineContext`). |
| **Physical: open() initializes per-buffer state without throwing** | Called once per buffer before `execute()`; must not crash on first call | MEDIUM | Provide valid `RecordBuffer` referencing a `TupleBuffer` |
| **Physical: execute() on a single valid record produces correct output** | Core correctness: the IREE inference path actually runs and writes correct output fields | HIGH | Requires tiny deterministic model (e.g., identity or constant-output); compare output field values to known-good expected outputs. |
| **Physical: close() after execute() does not throw** | Lifecycle contract: `open → execute(s) → close` must complete cleanly | MEDIUM | Follows EmitPhysicalOperator pattern. |
| **Physical: terminate() after close() does not throw** | `terminate()` is called once after all buffers; must release IREE resources | MEDIUM | Critical for resource leak prevention — if IREE handles are not released, production workers will leak. |
| **Physical: full lifecycle open → execute → close → terminate** | End-to-end correctness of the operator lifecycle; this is what the query engine calls | HIGH | Single test exercising the entire sequence in order |
| **Physical: getChild() returns nullopt on fresh operator** | `PhysicalOperatorConcept` contract | LOW | `EXPECT_FALSE(op.getChild().has_value())` |
| **Physical: setChild() / getChild() round-trip** | Required by physical plan builder | LOW | Set a child operator, verify it comes back |

### Differentiators (Catch Subtle Bugs — Not Obviously Missing, But High Value)

These are test cases that go beyond the conceptual interface into the behavioral details of an ML inference operator. They distinguish a thorough suite from a minimal one.

| Test Area | Value Proposition | Complexity | Notes |
|-----------|-------------------|------------|-------|
| **Logical: equality considers inferred schema, not just model name** | Two operators with same model name but different inferred schemas are NOT equal after `withInferredSchema`. Optimizer bugs arise if this isn't tested | MEDIUM | Construct two operators with same model name, infer different schemas, assert not equal. |
| **Logical: withInferredSchema() is immutable** | Original must be unchanged after calling `withInferredSchema()`; plan rewriting depends on this | LOW | After calling `withInferredSchema()`, verify original operator's `getOutputSchema()` is still empty/default. |
| **Logical: explain output changes with ExplainVerbosity** | Debug verbosity must include more info (operator ID, field details) than Short; ensures diagnostic quality | LOW | Assert Debug output is longer or contains a superset of Short output. |
| **Logical: schema inference with multiple input fields** | Models typically consume several input fields; test with 3+ input fields of different types | MEDIUM | Covers realistic ML operator usage patterns vs trivial single-field testing. |
| **Logical: output field names do not collide with input field names** | If model output field names collide with input field names, schema corruption occurs downstream | MEDIUM | Use a model that outputs a field named the same as an input field; verify either rename or exception. |
| **Physical: execute() with multiple tuples in single buffer** | Real workloads batch multiple records; single-record tests miss buffer-level iteration bugs | HIGH | Create a `TupleBuffer` with N tuples (e.g., 10), call `open → execute × N → close`, verify all N output records have correct inference results. |
| **Physical: execute() output values are numerically correct** | Correctness of the inference itself, not just "no crash"; without this, the IREE pipeline could silently produce wrong results | HIGH | Requires deterministic tiny model with known inputs/outputs. Example: model that multiplies all inputs by 2.0 — verify output = 2 × input. |
| **Physical: handler is accessible via ExecutionContext during execute()** | The ML operator must look up its `OperatorHandler` during execution; failure means wrong handler or missing state | MEDIUM | Register handler with specific ID in the pipeline context; verify `execute()` succeeds with that handler present. |
| **Physical: open() is called before execute() (invariant enforcement)** | `execute()` called without `open()` should either assert/throw or be safe | MEDIUM | Test what happens if `execute()` is called on a freshly constructed operator without `open()`. Document/assert the expected behavior. |
| **Physical: terminate() is safe to call more than once** | Production query teardown code may call terminate redundantly; should not double-free | MEDIUM | Call `terminate()` twice; verify no crash and no exception. |
| **Physical: execute() with empty TupleBuffer (zero tuples)** | Edge case that arises in real pipelines with sparse data | LOW | Create a buffer with 0 tuples; `open → close` (no `execute` calls); verify no crash. |
| **Logical: reflection round-trip** | `reflect()` and `unreflect()` are required for serialization/deserialization; bugs here break distributed query plans | HIGH | Serialize an operator with `reflect()`, deserialize with `unreflect()`, assert the result equals the original. |
| **Physical: invalid model path at setup() throws with actionable message** | IREE model loading fails with a non-existent `.vmfb` path; error message must be clear | MEDIUM | Provide a path to a file that does not exist; verify exception is thrown, not a crash. |
| **Physical: field not found in input record at execute() throws or produces defined behavior** | If the input record doesn't contain a declared input field, the operator must fail cleanly, not access invalid memory | HIGH | Provide a record missing a field the model expects; verify graceful error, not UB/crash. |
| **Physical: malformed model at setup() throws with actionable message** | Corrupt or invalid `.vmfb` content must fail at `setup()`, not at `execute()` when a record arrives | HIGH | Provide a `.vmfb` file that is syntactically invalid (e.g., a text file renamed to `.vmfb`); verify `setup()` throws. |
| **Physical: output field count matches model declaration** | IREE could silently produce fewer outputs than declared; must be caught at `execute()` or `setup()` | MEDIUM | Use tiny model; count output fields in produced record. |

### Anti-Features (Tests to Deliberately NOT Write)

Tests that seem like good ideas but would create problems — maintenance burden, false coverage, or scope violations.

| Anti-Feature | Why Requested | Why Problematic | What to Do Instead |
|--------------|---------------|-----------------|-------------------|
| **Mock the IREE model loader/handler** | Seems to isolate the test from IREE; avoids needing a `.vmfb` file | PROJECT.md explicitly states "No mocking runtime: Use real (tiny) IREE model." Mocks can't catch IREE ABI changes, field mapping bugs, or data layout issues. | Build a tiny purpose-built `.vmfb` model. The test model creation is a first-class deliverable. |
| **Test the full MNIST model** | Real-world model validates real-world correctness | MNIST model is large and slow. Unit tests must be fast and deterministic. The systest already covers MNIST. | Use a 3–10 neuron constant/identity model. |
| **Test ModelInferenceCompilationRule or LowerToPhysicalInferModel** | These are related components | PROJECT.md explicitly scopes compilation rules OUT. These require a different test setup and are a separate concern. | They are out of scope for this milestone. |
| **Test E2E query execution with the ML operator** | Seems like the ultimate validation | The systest already does this. Unit tests at this granularity add no new coverage. They are also 10–100× slower. | Keep systest for E2E; keep unit tests at operator API level. |
| **Performance/throughput benchmarks** | Useful to know how fast inference is per-tuple | PROJECT.md explicitly excludes performance benchmarks from scope. | Defer to a separate benchmark initiative. |
| **Test operator serialization to/from protobuf** | Used for distributed query plans | Compilation rule tests are out of scope, and protobuf serialization is exercised through the serialization infrastructure that does NOT need ML-specific tests beyond the reflection round-trip. | One reflection round-trip test (in differentiators) is sufficient. |
| **Exhaustive clang-tidy suppression tests** | The team uses NOLINT suppressions | These are infrastructure, not test coverage. | Follow conventions.md; apply suppressions inline. |
| **Thread-safety / concurrent execution tests for the physical operator** | Concurrent tuple processing is interesting | The query engine uses single-threaded pipelines per buffer. Thread safety of operator state is not an ML-specific concern. The existing `EmitPhysicalOperatorTest::ConcurrentSequenceChunkNumberTest` covers the concurrency layer above. | Not in scope for ML operator unit tests. |

---

## Feature Dependencies

```
[Tiny Test Model (.vmfb)]
    └──required by──> [Physical: setup() lifecycle test]
                           └──required by──> [Physical: execute() correctness]
                                                 └──required by──> [Physical: full lifecycle]
                                                 └──required by──> [Physical: multi-tuple execute]
                                                 └──required by──> [Physical: output correctness]

[Logical: construction]
    └──required by──> [Logical: getName / getChildren / explain tests]
    └──required by──> [Logical: withChildren / withTraitSet tests]
    └──required by──> [Logical: schema inference tests]
                           └──required by──> [Logical: getInputSchemas / getOutputSchema tests]
                           └──required by──> [Logical: equality with schema tests]
                           └──required by──> [Logical: reflection round-trip]

[Stub ExecutionContext / PipelineExecutionContext]
    └──required by──> [ALL physical operator tests]
    (pattern from EmitPhysicalOperatorTest::MockedPipelineContext)
```

### Dependency Notes

- **Tiny test model required before any physical operator tests:** The physical operator wraps IREE's runtime. Without a real `.vmfb` artifact (however small), none of the physical operator tests can run. This is a prerequisite, not optional.
- **Logical operator tests have no external dependencies:** They only depend on the Schema type system and the `LogicalOperatorConcept` interface. These tests can be written and run first, independently.
- **Stub pipeline context is reusable across tests:** A single `MockedPipelineContext` struct (following the `EmitPhysicalOperatorTest` pattern) can be shared across all physical operator tests in the test fixture.
- **Schema inference tests require field descriptor types:** The ML operator needs to declare what input fields it consumes and what output fields it produces. These descriptors (model name, input field names/types, output field names/types) must be understood before writing schema inference tests.

---

## MVP Definition

This is a test milestone, not a product, so "MVP" here means: the minimum set that justifies claiming "exhaustive unit tests exist."

### Launch With — Phase 1 (Logical Operator Tests)

These have zero external dependencies and can be written first.

- [ ] InferModelLogicalOperator construction — validates basic API
- [ ] getName(), getChildren(), explain(Short), explain(Debug) — interface contract
- [ ] withChildren() and immutability — plan rewriting correctness
- [ ] withTraitSet() / getTraitSet() round-trip — optimizer correctness
- [ ] Schema inference happy path — output fields appended correctly
- [ ] Schema inference error paths — empty input, missing field, type mismatch
- [ ] getInputSchemas() / getOutputSchema() after withInferredSchema()
- [ ] Equality — same and different operators
- [ ] Reflection round-trip (reflect / unreflect)

### Launch With — Phase 2 (Tiny Test Model + Physical Lifecycle Tests)

These require the `.vmfb` artifact first.

- [ ] Build or generate a tiny deterministic IREE model (3-input, 3-output identity or multiply-by-2)
- [ ] Physical: construction with valid model path
- [ ] Physical: setup() succeeds with registered handler
- [ ] Physical: full lifecycle open → execute → close → terminate
- [ ] Physical: getChild() / setChild() contract
- [ ] Physical: execute() output values are numerically correct

### Add After Phase 2 (Differentiators)

- [ ] Physical: multi-tuple buffer execution
- [ ] Physical: execute() with zero-tuple buffer
- [ ] Physical: terminate() twice (idempotence)
- [ ] Physical: invalid model path at setup() throws
- [ ] Physical: malformed model at setup() throws
- [ ] Physical: missing input field at execute() fails cleanly
- [ ] Logical: equality considers schema, not just model name
- [ ] Logical: withInferredSchema() immutability
- [ ] Logical: output field name collision handling

---

## Feature Prioritization Matrix

| Test Area | Coverage Value | Implementation Cost | Priority |
|-----------|---------------|---------------------|----------|
| Logical construction + interface contract | HIGH | LOW | P1 |
| Logical schema inference (happy path) | HIGH | MEDIUM | P1 |
| Logical schema inference (error paths) | HIGH | MEDIUM | P1 |
| Logical equality | HIGH | LOW | P1 |
| Logical reflection round-trip | HIGH | MEDIUM | P1 |
| Tiny test model creation | HIGH | HIGH | P1 (blocker for physical tests) |
| Physical: full lifecycle | HIGH | HIGH | P1 |
| Physical: execute() correctness | HIGH | HIGH | P1 |
| Physical: invalid model / missing field error paths | HIGH | MEDIUM | P1 |
| Physical: multi-tuple execute | MEDIUM | MEDIUM | P2 |
| Physical: zero-tuple buffer | LOW | LOW | P2 |
| Physical: terminate() idempotence | MEDIUM | LOW | P2 |
| Logical: schema with field name collisions | MEDIUM | MEDIUM | P2 |
| Logical: explain verbosity differences | LOW | LOW | P3 |

**Priority key:**
- P1: Must have for exhaustive coverage claim
- P2: Should have for ~95% branch coverage target
- P3: Nice to have, adds polish

---

## Sources

- Codebase inspection of `nes-logical-operators/include/Operators/LogicalOperator.hpp` — `LogicalOperatorConcept` interface definition
- Codebase inspection of `nes-logical-operators/include/Operators/SelectionLogicalOperator.hpp` and `.cpp` — concrete logical operator reference implementation and `withInferredSchema()` error patterns
- Codebase inspection of `nes-physical-operators/include/PhysicalOperator.hpp` — `PhysicalOperatorConcept` interface
- Codebase inspection of `nes-physical-operators/include/EmitPhysicalOperator.hpp` and `tests/EmitPhysicalOperatorTest.cpp` — canonical physical operator test patterns (stub pipeline context, lifecycle test structure)
- Codebase inspection of `nes-physical-operators/tests/SliceAssignerTest.cpp` — `BaseUnitTest` fixture usage pattern
- Codebase inspection of `nes-logical-operators/tests/LogicalPlanTest.cpp` — logical operator test patterns (equality, children, traits)
- Codebase inspection of `nes-common/tests/Util/include/BaseUnitTest.hpp` — `ASSERT_EXCEPTION_ERRORCODE` macro and test infrastructure
- `PROJECT.md` — explicit scope decisions (no mocks, real IREE model, no compilation rule tests, no performance benchmarks)

---

*Feature research for: ML inference operator unit test suite*
*Researched: 2026-03-11*
