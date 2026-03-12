# Architecture Research

**Domain:** ML operator unit testing in a C++ stream processing engine (NebulaStream)
**Researched:** 2026-03-11
**Confidence:** HIGH

## Standard Architecture

### System Overview

The ML inference feature spans four NES modules. Tests mirror that module structure exactly.

```
┌───────────────────────────────────────────────────────────────┐
│                     TEST LAYER (new)                          │
│                                                               │
│  ┌────────────────────────┐  ┌─────────────────────────────┐  │
│  │ nes-logical-operators/ │  │  nes-physical-operators/    │  │
│  │  tests/UnitTests/      │  │  tests/                     │  │
│  │  Operators/            │  │                             │  │
│  │  InferModelLogical     │  │  InferModelPhysical         │  │
│  │  OperatorTest.cpp      │  │  OperatorTest.cpp           │  │
│  └────────────┬───────────┘  └──────────────┬──────────────┘  │
└───────────────┼────────────────────────────┼────────────────── ┘
                │ tests against              │ tests against
┌───────────────▼────────────────────────────▼────────────────── ┐
│                  PRODUCTION CODE (existing)                    │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  nes-inference/                                          │  │
│  │    include/Model.hpp          Model data structure       │  │
│  │    include/ModelCatalog.hpp   In-memory model registry   │  │
│  │    include/ModelLoader.hpp    ONNX→IREE compilation      │  │
│  └──────────────────────────────────────────────────────────┘  │
│                          ↑ depends on                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  nes-logical-operators/                                  │  │
│  │    include/Operators/InferModelLogicalOperator.hpp       │  │
│  │    src/Operators/InferModelLogicalOperator.cpp           │  │
│  └──────────────────────────────────────────────────────────┘  │
│                          ↓ lowered to                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  nes-physical-operators/                                 │  │
│  │    include/InferModelPhysicalOperator.hpp                │  │
│  │    include/Inference/IREEAdapter.hpp                     │  │
│  │    include/Inference/IREEInferenceOperatorHandler.hpp    │  │
│  │    src/Inference/InferModelPhysicalOperator.cpp          │  │
│  │    src/Inference/IREEAdapter.cpp                         │  │
│  │    src/Inference/IREEInferenceOperatorHandler.cpp        │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Tested How |
|-----------|----------------|------------|
| `Inference::Model` | Value type holding IREE bytecode, field descriptors, shapes | Constructed inline in tests; a tiny pre-built `.vmfb` blob provides the bytecode |
| `InferModelLogicalOperator` | Stores a `Model`, implements `LogicalOperatorConcept` (schema inference, explain, equality, children, traits) | Pure unit test — no IREE runtime involved |
| `InferModelPhysicalOperator` | Executes inference per-record via Nautilus `invoke` trampolines into `IREEAdapter` | Integration unit test using `IREEInferenceOperatorHandler` + real tiny model |
| `IREEInferenceOperatorHandler` | `OperatorHandler` lifecycle — starts/stops per-thread `IREEAdapter` instances | Exercised indirectly through `InferModelPhysicalOperator` tests |
| `IREEAdapter` | Owns IREE session, typed input staging, `infer()` call | Exercised indirectly; could also be tested standalone with tiny model |

## Recommended Project Structure

New test files should be added in these exact locations, matching existing NES conventions:

```
nes-logical-operators/
└── tests/
    ├── CMakeLists.txt                          ← add new test target here
    ├── LogicalPlanTest.cpp                     (existing)
    └── UnitTests/
        └── Operators/
            └── InferModelLogicalOperatorTest.cpp   ← NEW

nes-physical-operators/
└── tests/
    ├── CMakeLists.txt                          ← add new test target here
    ├── EmitPhysicalOperatorTest.cpp            (existing, reference pattern)
    ├── SliceAssignerTest.cpp                   (existing)
    └── InferModelPhysicalOperatorTest.cpp      ← NEW

nes-inference/
└── tests/
    ├── CMakeLists.txt                          (existing)
    ├── ModelLoaderTest.cpp                     (existing, minimal)
    └── testdata/
        └── tiny_identity.vmfb                 ← NEW  (tiny pre-built model)
```

### Structure Rationale

- **Co-location with module:** NES convention is `nes-component/tests/` mirroring the component source. Logical operator tests belong in `nes-logical-operators/tests/`, physical in `nes-physical-operators/tests/`.
- **`UnitTests/` subdirectory for logical tests:** The `TESTING.md` pattern is `tests/UnitTests/[Path]/[Name]Test.cpp` for strict unit tests. Logical operator tests are pure (no IREE), so they belong in `UnitTests/`.
- **Physical operator tests directly in `tests/`:** Existing physical operator tests (`EmitPhysicalOperatorTest.cpp`, `SliceAssignerTest.cpp`) live directly in `tests/` without a `UnitTests/` subdirectory. The physical operator tests use real IREE (tiny model), matching this pattern.
- **Tiny model in `nes-inference/tests/testdata/`:** The model binary is shared test data. Placing it in `nes-inference/tests/testdata/` keeps it with the inference module and is accessible via `CMAKE_CURRENT_SOURCE_DIR` at configure time.

## Architectural Patterns

### Pattern 1: BaseUnitTest Fixture with MockedPipelineContext Stub

Physical operator tests cannot call `setup()` or `terminate()` without a `PipelineExecutionContext`. The codebase convention (see `EmitPhysicalOperatorTest.cpp`) is to define a `MockedPipelineContext` inner struct directly inside the test fixture class, not via GMock.

**What:** Inner struct implementing `PipelineExecutionContext` interface with minimal stub bodies. `getNumberOfWorkerThreads()` returns 1, `emitBuffer()` collects into a vector, etc.

**When to use:** Every `InferModelPhysicalOperatorTest` method that exercises `setup()`, `execute()`, or `terminate()`.

**Example:**
```cpp
class InferModelPhysicalOperatorTest : public Testing::BaseUnitTest
{
    struct MockedPipelineContext final : PipelineExecutionContext
    {
        bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; }
        TupleBuffer allocateTupleBuffer() override { return bm->getBufferBlocking(); }
        WorkerThreadId getId() const override { return INITIAL<WorkerThreadId>; }
        uint64_t getNumberOfWorkerThreads() const override { return 1; }
        std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bm; }
        PipelineId getPipelineId() const override { return PipelineId(1); }
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override
        { return handlers; }
        void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& h) override
        { handlers_ref = &h; }
        void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { FAIL(); }
        // members...
    };
};
```

### Pattern 2: Inline Model Construction for Logical Operator Tests

`InferModelLogicalOperator` tests need a `Inference::Model` but do not need working IREE bytecode — the logical operator is pure data plus schema logic. A `Model` can be constructed with an empty or stub bytecode buffer and populated via setters.

**What:** Build a `Model` with `setInputs()`, `setOutputs()`, `setFunctionName()`, `setInputShape()`, `setOutputShape()`, and a `shared_ptr<byte[]>` of any non-null content. The logical operator tests never call IREE.

**When to use:** All `InferModelLogicalOperatorTest` cases — construction, equality, explain, schema inference, children, traits.

**Example:**
```cpp
static Inference::Model makeStubModel()
{
    auto buf = std::make_shared<std::byte[]>(4);
    Inference::Model m(buf, 4);
    m.setFunctionName("test_fn");
    m.setInputs({{"x", DataType::createFloat()}});
    m.setOutputs({{"y", DataType::createFloat()}});
    m.setInputShape({1});
    m.setOutputShape({1});
    m.setInputSizeInBytes(4);
    m.setOutputSizeInBytes(4);
    return m;
}
```

### Pattern 3: Real Tiny VMFB Model for Physical Operator Tests

The project constraint is "no mocked IREE runtime — use real tiny model." The physical operator instantiates a real `IREEInferenceOperatorHandler` holding the tiny `.vmfb` binary. This mirrors the approach of system tests (`InferModelMNIST.test` uses a real MNIST model binary) but at unit-test scale.

**What:** Load a pre-built minimal IREE `.vmfb` (e.g., a single-float identity or 1-in 1-out linear function) from the test binary directory using `CMAKE_CURRENT_SOURCE_DIR`. The binary is checked into `nes-inference/tests/testdata/`. Tests that need it read the bytecode from disk in `SetUpTestCase()`.

**When to use:** All `InferModelPhysicalOperatorTest` cases that call `setup()` or `execute()`.

**Build order implication:** The tiny `.vmfb` must exist in the source tree before the test is compiled. It is a committed binary artifact, not generated during build, so there is no build-order dependency — it only needs to be created once (milestone phase 1) before physical operator tests are written (milestone phase 2).

## Data Flow

### Logical Operator Test Flow

```
Test constructs Inference::Model (stub bytes, real field descriptors)
    |
    v
InferModelLogicalOperator(model) ─── construction path
    |
    v
withInferredSchema(inputSchemas) ─── schema inference
    |
    v
getOutputSchema() ─── verify fields appended correctly
    |
    v
explain(), operator==, getChildren(), getTraitSet() ─── structural tests
```

### Physical Operator Test Flow

```
SetUpTestCase: read tiny.vmfb from disk
    |
    v
Construct IREEInferenceOperatorHandler(model_with_real_bytecode)
    |
    v
Register handler in std::unordered_map<OperatorHandlerId, shared_ptr<OperatorHandler>>
    |
    v
Construct InferModelPhysicalOperator(handlerId, {"x"}, {"y"})
    |
    v
setup(executionCtx, compilationCtx)      ← calls handler.start() → allocates IREEAdapters
    |
    v
Per record: execute(ctx, record)         ← addModelInput → infer() → getResultAt → record.write
    |
    v
terminate(executionCtx)                  ← calls handler.stop() → clears adapters
```

### Key Data Flows

1. **Bytecode through logical operator:** IREE `.vmfb` is base64-decoded by `InferModelLogicalOperator` constructor path (via `Unreflector`) and stored in `Inference::Model::byteCode`. Tests verify the serialization round-trip via `Reflector`/`Unreflector`.
2. **Per-thread adapter allocation:** `IREEInferenceOperatorHandler::start()` creates one `IREEAdapter` per worker thread. Tests use a `MockedPipelineContext` that returns `getNumberOfWorkerThreads() == 1`, so exactly one adapter is created — predictable and deterministic.
3. **Record-level execution:** `execute()` uses Nautilus `invoke` trampolines. These are called as normal C++ function pointers in unit tests (no JIT compilation occurs in test context).

## CMakeLists.txt Integration Patterns

### Logical Operator Test Registration

Add to `nes-logical-operators/tests/CMakeLists.txt`:

```cmake
function(add_nes_logical_operators_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-logical-operators nes-inference)
endfunction()

# existing:
add_nes_logical_operators_test(nes-logical-plan-test "LogicalPlanTest.cpp")

# new:
add_nes_logical_operators_test(InferModelLogicalOperatorTest
    "UnitTests/Operators/InferModelLogicalOperatorTest.cpp")
```

Note: `nes-inference` must be linked because `InferModelLogicalOperator` holds an `Inference::Model` value type.

### Physical Operator Test Registration

Add to `nes-physical-operators/tests/CMakeLists.txt`:

```cmake
# existing pattern:
function(add_nes_physical_operator_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME}
        nes-data-types nes-physical-operators nes-memory-test-utils nes-test-util nes-inference)
endfunction()

# new:
add_nes_physical_operator_test(InferModelPhysicalOperatorTest
    InferModelPhysicalOperatorTest.cpp)

# Pass the testdata path so the test can find the tiny model:
target_compile_definitions(InferModelPhysicalOperatorTest PRIVATE
    NES_TESTDATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../../../nes-inference/tests/testdata")
```

Alternatively the tiny model path is passed as a compile-time string constant via `target_compile_definitions`, which avoids hard-coded absolute paths.

## Build Order Implications

```
Phase 1: Create tiny VMFB model artifact
  └── tiny_identity.vmfb committed to nes-inference/tests/testdata/
  └── No build-time generation; file is a static binary in the repo

Phase 2: Write + build InferModelLogicalOperatorTest
  └── Depends on: nes-inference (Model), nes-logical-operators (InferModelLogicalOperator)
  └── Does NOT depend on: IREE runtime, tiny model file
  └── CMake target: InferModelLogicalOperatorTest
  └── Build: cmake --build build --target InferModelLogicalOperatorTest

Phase 3: Write + build InferModelPhysicalOperatorTest
  └── Depends on: tiny model file (from Phase 1), nes-physical-operators, nes-inference, IREE vcpkg port
  └── CMake target: InferModelPhysicalOperatorTest
  └── Build: cmake --build build --target InferModelPhysicalOperatorTest
```

The logical operator tests can be compiled and run independently of IREE. Physical operator tests require the IREE runtime library (already present as a vcpkg dependency) and the tiny model binary (checked into the repo).

## Test Fixture Hierarchy

```
testing::Test
    └── Testing::BaseUnitTest          (nes-common/tests/Util/include/BaseUnitTest.hpp)
            ├── InferModelLogicalOperatorTest
            │     - SetUpTestCase(): Logger::setupLogging(...)
            │     - members: Inference::Model stubModel (from makeStubModel())
            │
            └── InferModelPhysicalOperatorTest
                  - SetUpTestCase(): Logger::setupLogging(...), read tiny.vmfb bytes
                  - members:
                    - std::shared_ptr<BufferManager> bm
                    - std::unordered_map<OperatorHandlerId, shared_ptr<OperatorHandler>> handlers
                    - MockedPipelineContext (inner struct)
                  - helper: createUUT() → constructs InferModelPhysicalOperator + registers handler
                  - helper: run(fn, buffer) → wires ExecutionContext + calls fn
```

`Testing::BaseUnitTest` provides per-test timeout protection (5 min), log flushing in `TearDown()`, and `TestSourceNameHelper`. All ML operator tests inherit from it.

## Anti-Patterns

### Anti-Pattern 1: Using the Full MNIST Model in Unit Tests

**What people do:** Copy the MNIST `.vmfb` from `nes-systests/inference/` into unit tests.

**Why it's wrong:** MNIST is ~1.2 MB of bytecode and takes seconds to compile/load through IREE. Unit tests should be fast (< 1 second each). The full model also exercises ML accuracy, not operator correctness.

**Do this instead:** Build a minimal 1-input/1-output identity or linear model (e.g., a 1x1 matrix multiply). It exercises the full IREE code path in milliseconds and produces deterministic outputs.

### Anti-Pattern 2: Mocking IREEAdapter or IREEInferenceOperatorHandler with GMock

**What people do:** Define GMock `MOCK_METHOD` stubs for `IREEAdapter::infer()` to avoid needing a real model.

**Why it's wrong:** The project explicitly chose real model over mocked runtime (documented in `PROJECT.md` Key Decisions). GMock mocks would not catch IREE integration bugs (wrong input staging, output size mismatches, thread-safety issues in adapter allocation). GMock is also not the codebase convention for this layer — `EmitPhysicalOperatorTest.cpp` uses manual stubs for `PipelineExecutionContext`, not GMock.

**Do this instead:** Use a real tiny model. A 1-in/1-out IREE function is trivial to produce and covers the full execution path.

### Anti-Pattern 3: Registering Tests Outside the Module's Own CMakeLists.txt

**What people do:** Add `InferModelPhysicalOperatorTest` to the root `CMakeLists.txt` or to `nes-inference/tests/CMakeLists.txt`.

**Why it's wrong:** Violates NES co-location convention. Tests belong in the module that contains the production code under test. `InferModelPhysicalOperator` lives in `nes-physical-operators/`, so its tests belong in `nes-physical-operators/tests/`.

**Do this instead:** Add the test target inside the module's own `tests/CMakeLists.txt` using the module-specific test function (`add_nes_physical_operator_test`).

### Anti-Pattern 4: Testing Schema Inference Without Calling withInferredSchema

**What people do:** Construct `InferModelLogicalOperator` and immediately call `getOutputSchema()`, expecting populated fields.

**Why it's wrong:** Schema inference is a two-phase operation. The logical operator stores an empty output schema at construction. `withInferredSchema(inputSchemas)` returns a new operator with the schema populated by appending model output fields.

**Do this instead:** Always test the full inference path: `op.withInferredSchema({inputSchema}).getOutputSchema()`. This is the code path the optimizer walks.

## Integration Points

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| `nes-inference` → `nes-logical-operators` | `Inference::Model` value type (include header dep) | `InferModelLogicalOperator` holds `Model` by value; tests must link `nes-inference` |
| `nes-logical-operators` → `nes-physical-operators` | `LowerToPhysicalInferModel` lowering rule (not under test) | Tests bypass the lowering pipeline entirely |
| `nes-physical-operators` → IREE vcpkg | `IREERuntimeWrapper` wraps the C IREE API | Tests exercise this path via tiny model; no additional mocking |
| Test fixtures → `nes-common` test utilities | `Testing::BaseUnitTest` from `nes-common/tests/Util/` | Automatic via `nes-test-util` link target in `add_nes_test()` |

## Sources

- Direct inspection of `nes-physical-operators/tests/EmitPhysicalOperatorTest.cpp` (canonical physical operator test pattern)
- Direct inspection of `nes-logical-operators/tests/LogicalPlanTest.cpp` (logical layer test pattern without `BaseUnitTest`)
- Direct inspection of `nes-physical-operators/tests/CMakeLists.txt` (test registration macro)
- Direct inspection of `nes-inference/tests/CMakeLists.txt` (custom `add_nes_test_inference` macro)
- Direct inspection of `cmake/NebulaStreamTest.cmake` (base `add_nes_test`, `gtest_discover_tests`)
- Direct inspection of `nes-physical-operators/include/InferModelPhysicalOperator.hpp`
- Direct inspection of `nes-physical-operators/src/Inference/InferModelPhysicalOperator.cpp`
- Direct inspection of `nes-logical-operators/include/Operators/InferModelLogicalOperator.hpp`
- Direct inspection of `nes-logical-operators/src/Operators/InferModelLogicalOperator.cpp`
- Direct inspection of `nes-physical-operators/include/Inference/IREEInferenceOperatorHandler.hpp`
- Direct inspection of `nes-physical-operators/include/Inference/IREEAdapter.hpp`
- Direct inspection of `nes-inference/include/Model.hpp` and `ModelCatalog.hpp`
- Project constraints documented in `.planning/PROJECT.md`

---
*Architecture research for: ML operator unit testing in NebulaStream*
*Researched: 2026-03-11*
