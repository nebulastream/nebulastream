# Pitfalls Research

**Domain:** ML inference operator unit testing with real IREE runtime (C++ / NebulaStream)
**Researched:** 2026-03-11
**Confidence:** HIGH — based on direct source inspection of `IREERuntimeWrapper`, `IREEAdapter`, `IREEInferenceOperatorHandler`, `InferModelPhysicalOperator`, and `InferModelLogicalOperator` in commits `101dc3d17a` through `172f093734`.

---

## Critical Pitfalls

### Pitfall 1: IREE Session Not Destroyed — Raw Pointer Members Without Destructor

**What goes wrong:**
`IREERuntimeWrapper` stores three raw C pointers — `instance`, `session`, `device` — and an `iree_vm_function_t` struct. None are released in any destructor. Every test that calls `IREEAdapter::initializeModel()` will allocate a live IREE runtime instance and session and leak them on test teardown. In GTest, object lifetimes are test-scoped; over a full test suite, cumulative leaks will cause address sanitizer failures or OOM crashes in CI, even if each individual test appears to pass.

**Why it happens:**
IREE's C API is reference-counted via `iree_runtime_instance_release()`, `iree_runtime_session_release()`, and `iree_hal_device_release()`. The wrapper was written to demonstrate functionality, not to own lifetimes. Developers testing with real models assume GTest teardown is sufficient — it is not for raw C handles.

**How to avoid:**
Before writing any physical operator test that touches a real IREE model, verify that `IREERuntimeWrapper` has a proper destructor (or RAII wrapper) that calls `iree_runtime_session_release()`, `iree_runtime_instance_release()`, and `iree_hal_device_release()` in reverse order. If the destructor is absent, add it before writing tests — otherwise every physical operator test is a leak, and the leak will be flagged by the address sanitizer CI configuration (`x64-linux-address-libcxx.cmake`).

**Warning signs:**
- AddressSanitizer output contains `iree_runtime_instance_create` in the leak stack trace.
- IREE tests hang on teardown (session close blocks on pending async work).
- `valgrind` reports reachable memory from `iree_allocator_system()` allocations after test exit.

**Phase to address:**
Phase that sets up the test model and test harness — before writing any physical operator test body.

---

### Pitfall 2: Test Model Requires IREE Compiler Toolchain, Not Just IREE Runtime

**What goes wrong:**
A `.vmfb` file (IREE VM FlatBuffer) cannot be generated at test time using the IREE runtime library. It requires `iree-compile` (or the Python `iree.compiler` package) to compile from MLIR or ONNX to bytecode. If a developer attempts to generate a test model on-the-fly inside a GTest fixture or CMake test step without the compiler toolchain present, the build will fail or the test will be skipped silently. The CI environment has `ireeruntime` (runtime only) as a vcpkg dependency — the compiler is a separate, much heavier dependency.

**Why it happens:**
The existing system test uses pre-compiled `.onnx` model files in `nes-systests/testdata/model/` that are compiled to `.vmfb` at query registration time via `ModelLoader`. For unit tests, this full ONNX→IREE compilation path is too heavy (requires ONNX tools + IREE compiler). Developers assume "IREE is installed, so I can create a model in my test" — but the installed library is runtime-only.

**How to avoid:**
Pre-compile the tiny test model to `.vmfb` offline using `iree-compile` and commit the resulting binary alongside the test. The test CMakeLists.txt should reference the committed `.vmfb` directly, not attempt to compile it. Alternatively, use `iree-compile` in a one-time script under `scripts/` and document the regeneration procedure. Never attempt runtime compilation inside a test fixture.

**Warning signs:**
- Test setup calls `ModelLoader::load()` or `iree-compile` — those paths require the full toolchain.
- CMakeLists.txt has a custom command that invokes `iree-compile` at build time without checking availability.
- Tests that pass locally (dev has full IREE toolchain) fail in CI (runtime-only vcpkg install).

**Phase to address:**
The dedicated "create minimal test model" task — this must be done and the artifact committed before any physical operator test can be written.

---

### Pitfall 3: Uninitialized Members Crash Without `start()` Being Called

**What goes wrong:**
`IREERuntimeWrapper` has uninitialized members `nDim` (size_t) and `instance`/`session`/`device` (raw pointers). `IREEAdapter` has `inputSize` and `outputSize` that are zero-initialized only if using value-initialization, but the default constructor does not explicitly initialize them. If a test constructs `IREEAdapter` and calls `addModelInput()` or `getResultAt()` before `initializeModel()`, the PRECONDITION check (`index < inputSize / sizeof(T)`) will trigger with `inputSize == 0`, immediately aborting.

More critically: `IREEInferenceOperatorHandler::getIREEAdapter(threadId)` accesses `threadLocalAdapters[threadId % threadLocalAdapters.size()]`. If `start()` has not been called, `threadLocalAdapters` is empty and `threadLocalAdapters.size()` returns 0 — causing division by zero and undefined behavior before any assertion can fire.

**Why it happens:**
The operator lifecycle in NebulaStream is `setup() → open() → execute() → close() → terminate()`, with the operator handler initialized in `start()`. Tests that skip the lifecycle and call `execute()` directly will hit uninitialized state. The existing `EmitPhysicalOperatorTest` shows the correct pattern (constructing the handler, calling `start()` via setup), but it is easy to copy only the `execute()` part.

**How to avoid:**
The test harness must call `handler->start(pec, 0)` before any `execute()` call. Follow the exact `EmitPhysicalOperatorTest` pattern: create a `MockedPipelineContext` that returns `getNumberOfWorkerThreads() == 1`, register the handler in the context's operator handler map, then call `setup()` on the physical operator before calling `execute()`.

**Warning signs:**
- Test directly constructs `IREEInferenceOperatorHandler` and calls `getIREEAdapter()` without calling `start()`.
- `threadLocalAdapters.size()` is zero at time of access — undefined behavior, may manifest as SIGFPE or a silent wrong result.
- Test passes on one machine but crashes on another (UB is layout-dependent).

**Phase to address:**
Physical operator test harness setup — establish the correct lifecycle pattern once, then reuse it across all test cases.

---

### Pitfall 4: `nautilus::invoke()` Path Is Not Exercised by Direct C++ Calls

**What goes wrong:**
`InferModelPhysicalOperator::execute()` routes all IREE adapter calls through `nautilus::invoke()`, which is Nautilus's mechanism for calling C++ functions from JIT-compiled code. When calling the physical operator's `execute()` method directly from a GTest (interpreted mode), `nautilus::invoke()` is invoked in interpreted mode — which is correct and exercisable. However, if Nautilus is configured to only run in compiled mode (MLIR backend), the `nautilus::invoke()` calls will attempt to JIT-compile, require MLIR to be available, and either skip or crash.

The existing tests in `nes-nautilus/tests/` show that Nautilus tests use a `NautilusTestUtils` runner that explicitly selects the backend. Physical operator tests must use the same approach, not instantiate `InferModelPhysicalOperator` and call `execute()` directly.

**Why it happens:**
Developers see `execute()` as a normal C++ method and call it like one, not realizing the body contains `nautilus::invoke()` calls that are backend-sensitive. The Nautilus interpreter backend will work; the MLIR backend requires the full compiler stack.

**How to avoid:**
Use Nautilus's interpreted mode for unit tests (set via the test utils). If the project's existing physical operator tests call `execute()` directly without going through Nautilus (as `EmitPhysicalOperatorTest` does — it calls `open()`/`close()`, not `execute()`), then the ML operator tests should follow the same direct-invocation pattern, understanding that the `nautilus::invoke()` calls inside `execute()` will run in interpreted mode. Document which backend is in use.

**Warning signs:**
- Test links against `nes-nautilus-mlir` backend and `execute()` fails with a JIT compilation error.
- Nautilus test runs that work locally fail in CI where MLIR is not in the test image.
- The test CMakeLists does not specify a Nautilus backend configuration.

**Phase to address:**
Physical operator test harness setup — choose and document the Nautilus backend for tests before writing execute() test cases.

---

### Pitfall 5: `iree_allocator_null()` Means Bytecode Buffer Lifetime Must Exceed Session Lifetime

**What goes wrong:**
`IREERuntimeWrapper::setup()` calls `iree_runtime_session_append_bytecode_module_from_memory()` with `iree_allocator_null()` as the module allocator. This tells IREE to use the provided memory directly without copying it — the session does not own the buffer. In `IREEAdapter::initializeModel()`, the buffer comes from `model.getByteCode()`, which returns a `span<const byte>` backed by a `shared_ptr<byte[]>` inside `Model::RefCountedByteBuffer`.

If a test creates a `Model`, extracts the byte span, constructs an `IREEAdapter`, and the original `Model` object is then destroyed (e.g., by going out of scope, or being moved into the handler constructor), the underlying buffer is released while the IREE session still holds a non-owning pointer to it. The next `execute()` call will read freed memory — silent corruption, not a crash.

**Why it happens:**
The `Model` class uses shared ownership (`shared_ptr<byte[]>`), which obscures the fact that the IREE session is a non-owning borrower. Test authors naturally write: `auto model = makeTestModel(); auto adapter = IREEAdapter::create(); adapter->initializeModel(model);` — where `model` may go out of scope before the test uses the adapter.

**How to avoid:**
In every test that uses `IREEAdapter` directly, ensure the `Model` object (or the `IREEInferenceOperatorHandler` that owns the model) outlives the adapter. The cleanest pattern: keep the model alive for the full test by storing it in the test fixture, the same way `IREEInferenceOperatorHandler` does (it takes a model by value and stores it as a member).

**Warning signs:**
- AddressSanitizer reports a use-after-free inside IREE session execution.
- Test succeeds in Debug build (memory not reused immediately) but fails in Release (memory reused).
- `Model` is constructed as a temporary passed directly to `initializeModel()`.

**Phase to address:**
Physical operator test harness setup — establish ownership invariants in the test fixture before writing any execute() tests.

---

### Pitfall 6: Schema Inference Tests Succeed Without Validating Output Field Order

**What goes wrong:**
`InferModelLogicalOperator::withInferredSchema()` appends model output fields to the input schema in the order returned by `model.getOutputs()`. Tests that only check "the output schema has N fields" will miss bugs where fields are appended in the wrong order — a type mismatch between schema position and record field name. Since IREE output is positional (`getResultAt(index)`), wrong ordering causes silent wrong-type inference: the model's first output (e.g., `logit_0`) is written to the schema field for the second output (e.g., `logit_1`), producing numerically wrong but structurally valid results.

**Why it happens:**
Schema equality in NebulaStream is field-by-field, but test assertions like `EXPECT_EQ(outputSchema.getFields().size(), expectedSize)` check only the count. Developers confident in "the schema is right" skip order verification. The bug only shows at runtime when downstream operators read the wrong field.

**How to avoid:**
Every schema inference test must check both the field names AND their positions: `EXPECT_EQ(outputSchema.getField(N).getName(), expectedName)` for each index. Additionally, verify that no output field overwrites an existing input field name.

**Warning signs:**
- Test asserts only field count, not field names or order.
- Model output field names are generic (`c0`, `c1`, ...) making order bugs hard to spot.
- Test uses `EXPECT_TRUE(outputSchema.hasField("c0"))` without checking which position `c0` appears at.

**Phase to address:**
Logical operator schema inference tests — every test case must assert field order, not just field existence.

---

### Pitfall 7: Error Path Tests Rely on Exceptions But IREE Uses Status Codes Internally

**What goes wrong:**
The physical operator error paths (`InferenceRuntime` exception) are thrown inside `IREERuntimeWrapper::execute()` when IREE status codes indicate failure. However, to exercise these paths in unit tests, the test must either (a) use a real IREE session that is deliberately misconfigured, or (b) inject a failure at the adapter level. Option (a) requires knowing exactly which IREE inputs cause a status failure — for example, calling `execute()` without a `setup()` (the session pointer is null, causing a segfault, not a status failure). Option (b) requires a test-only adapter stub.

The PROJECT.md constraint "no mocking runtime" means option (b) is excluded for physical operator tests. But for error paths specifically — invalid model bytes, missing function name, wrong input size — the real IREE runtime will either segfault (null pointer), throw `InferenceRuntime`, or silently produce garbage. Tests that expect `EXPECT_THROW(adapter->infer(), InferenceRuntime)` for a malformed model may instead get a segfault.

**Why it happens:**
IREE's C API does not guarantee clean status codes for all error conditions. Some errors (passing null bytecode, null session pointer) are precondition violations that cause undefined behavior, not IREE status failures. Developers assume "malformed input → status error → exception" but the actual behavior may be a crash.

**How to avoid:**
For error path tests on the physical operator, test only conditions where IREE will actually return a status error (wrong function name after valid setup, input buffer wrong size after valid setup). For conditions that would crash IREE (null session, null bytecode), test at the `IREEAdapter`/`IREEInferenceOperatorHandler` layer using PRECONDITION assertion checks, not IREE invocations. Clearly distinguish "IREE API error → InferenceRuntime exception" tests from "precondition violation → PRECONDITION abort" tests.

**Warning signs:**
- Test calls `adapter->infer()` on an adapter where `initializeModel()` was not called — this will segfault, not throw.
- Test expects `EXPECT_THROW` for a null bytecode span — IREE will likely crash before status propagation.
- Error path tests pass on ASan builds (signals converted to errors) but fail on non-instrumented builds.

**Phase to address:**
Physical operator error path tests — classify each error scenario as "throws `InferenceRuntime`" vs. "triggers PRECONDITION abort" before writing the test.

---

### Pitfall 8: Test Model Output Values Are Not Numerically Validated

**What goes wrong:**
Tests that only assert "inference succeeded" (no crash, correct field count) do not verify that the model is computing the correct result. A tiny test model compiled to `.vmfb` might silently produce wrong outputs due to a shape mismatch, a wrong input layout (row-major vs. column-major), or a wrong data type (float32 expected, float16 provided). Since `IREEAdapter::addModelInput(index, value)` writes to a raw byte buffer without type checking beyond `inputSize / sizeof(T)`, a test passing `float` where the model expects `int32` will write the correct number of bytes but with garbage values — inference succeeds, but outputs are meaningless.

**Why it happens:**
Unit tests for operators typically validate structure (did execution complete, is the schema right?) not computation. For ML operator tests, structure validation is necessary but not sufficient — the test model must produce known, verifiable outputs for known inputs to catch this class of bug.

**How to avoid:**
The tiny test model must have deterministic, documented inputs and expected outputs. The test must assert `EXPECT_NEAR(result, expectedValue, tolerance)` for each output field after inference. Design the test model so the expected output can be computed independently (e.g., a linear model with weights set to 1, so output equals sum of inputs, or a constant model that always returns a fixed value regardless of input).

**Warning signs:**
- Physical operator tests only check for absence of exceptions.
- Test uses a model where the expected output is unknown or must be fetched from the model itself.
- No `EXPECT_NEAR` or `EXPECT_FLOAT_EQ` assertions in any physical operator test.

**Phase to address:**
Test model design and physical operator execute() tests — define expected outputs before writing the test, not after.

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Skip `terminate()` test coverage | Faster test writing | Resource leaks (IREE session, handler state) go undetected; `stop()` is currently a no-op but may gain cleanup logic | Never — `terminate()` must be tested to protect against future implementation changes |
| Use MNIST `.vmfb` for unit tests instead of tiny model | No model creation work | Test suite takes 10-30s per test case instead of <1s; CI timeout risk; non-deterministic in timing | Never for unit tests — tiny model only |
| Assert only `SUCCEED()` for IREE integration (like existing `ModelLoaderTest`) | Zero maintenance burden | Provides false coverage signal; 0% branch coverage on actual IREE execution paths | Only for tool-availability checks, not for operator execution |
| Copy the IREE adapter state instead of using shared_ptr | Simpler test fixture | Copies raw C pointers (double-free on destructor) if `IREERuntimeWrapper` ever gets a destructor | Never — always transfer via shared_ptr |
| Skip `open()`/`close()` lifecycle in physical operator tests | Simpler test code | `open()`/`close()` paths are uncovered; schema mismatch bugs are invisible | Never — both paths must be tested |

---

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| IREE runtime C API | Calling `iree_runtime_session_append_bytecode_module_from_memory()` with a temporary byte span | Keep the `Model` object (which owns the `shared_ptr<byte[]>`) alive for the full session lifetime; store it in the test fixture |
| IREE function lookup | Calling `execute()` before calling `setup()`, leaving `function.module == nullptr` | Always call `setup()` in the test fixture's `SetUp()`, mirroring `IREEInferenceOperatorHandler::start()` |
| `nautilus::invoke()` in tests | Assuming direct C++ call skips Nautilus entirely | In interpreted mode, `nautilus::invoke()` works correctly; in compiled mode it requires MLIR; choose interpreted mode for unit tests |
| `PipelineExecutionContext::getNumberOfWorkerThreads()` | Returning 0 from the mock context | `IREEInferenceOperatorHandler::start()` iterates `[0, getNumberOfWorkerThreads())` — returning 0 means no adapters are created; return at least 1 |
| `getIREEAdapter(threadId)` indexing | Assuming threadId is zero-based and contiguous | The indexing is `threadId % threadLocalAdapters.size()` — a threadId of 0 with 1 adapter works; use `WorkerThreadId(0)` in single-threaded tests |

---

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Using MNIST model (407 KB) in unit tests | Unit tests take 5-30s each; CI timeouts | Commit a tiny purpose-built `.vmfb` (target: <10KB, <100ms inference) | From the first test — MNIST IREE compilation adds >2s per test |
| Creating a new IREE instance per test case | Memory usage climbs across test suite; slow teardown | Reuse the IREE instance across related test cases via `SetUpTestSuite` / `TearDownTestSuite` if safe; or accept one instance per fixture | At 50+ test cases — cumulative IREE session overhead is significant |
| Loading model bytecode from disk in every test | I/O latency makes tests flaky in high-load CI | Embed test model bytecode as a C++ `std::array<std::byte, N>` constant or use `TEST_DATA_DIR` with cached file loading | In parallel CI runs with shared NFS storage |

---

## "Looks Done But Isn't" Checklist

- [ ] **Physical operator test:** Only asserts no crash — verify `EXPECT_NEAR` on actual float output values.
- [ ] **Lifecycle coverage:** `setup()` and `terminate()` are tested, not just `execute()` — verify both exist as separate test cases.
- [ ] **Error paths:** Only "happy path" is tested — verify at least one test for wrong field name, one for size mismatch, one for model not found.
- [ ] **Schema inference test:** Only checks field count — verify field names and positions are asserted individually.
- [ ] **ThreadId test:** Only tests with `WorkerThreadId(0)` — verify the `threadId % size` indexing is tested with a handler initialized for 2+ worker threads.
- [ ] **`terminate()` test:** Assumed to be a no-op and skipped — verify it is covered even if it does nothing, to catch future regressions when cleanup logic is added.
- [ ] **Model lifetime:** Test creates a temporary `Model` and passes its bytecode to IREE — verify the `Model` object is stored in the fixture, not as a local variable.
- [ ] **CMakeLists registration:** Test file written but not added to `add_nes_unit_test()` in CMakeLists.txt — verify the test actually appears in `ctest -N` output.

---

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| IREE resource leaks discovered after test suite written | MEDIUM | Add destructor to `IREERuntimeWrapper` releasing handles in reverse order; re-run ASan build to confirm clean |
| Test model compiled with wrong shape — all tests produce garbage | MEDIUM | Re-run `iree-compile` with corrected shape flags; replace committed `.vmfb`; update expected output values in tests |
| Tests passing in interpreter mode fail in compiled mode | HIGH | Audit all `nautilus::invoke()` call sites in `InferModelPhysicalOperator`; ensure all captured functions are `+[]` lambdas with no captures (required for Nautilus JIT) |
| Schema test misses field order bug that reaches production | HIGH | Add regression test asserting full field ordering; audit all `withInferredSchema()` callers |
| `iree_allocator_null()` lifetime bug causes flaky corruption | HIGH | Change `IREERuntimeWrapper::setup()` to copy bytecode internally (use `iree_allocator_system()`); update tests accordingly |

---

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| IREE resource leaks (no destructor) | Test harness setup / model creation phase | Run test suite under AddressSanitizer; zero leaks from IREE allocations |
| Test model requires compiler toolchain | Test model creation phase (first) | Committed `.vmfb` loads successfully in a runtime-only environment (no `iree-compile` needed) |
| Uninitialized members / `start()` not called | Physical operator harness setup | Tests call `setup()` before `execute()`; handler `start()` is verified to populate `threadLocalAdapters` |
| `nautilus::invoke()` backend sensitivity | Physical operator harness setup | Document selected backend; tests pass in CI (interpreted mode) |
| Bytecode buffer lifetime (allocator_null) | Physical operator harness setup | Model object stored in fixture; ASan clean |
| Schema field order not validated | Logical operator schema inference tests | Every schema test asserts field names and positions by index |
| Error paths crash instead of throw | Physical operator error path tests | Error path tests explicitly classified as PRECONDITION-abort vs. exception; none cause unexpected SIGSEGV |
| Test model output not numerically validated | Physical operator execute() tests | Every execute() test has `EXPECT_NEAR` on at least one output field |

---

## Sources

- Direct source inspection: `IREERuntimeWrapper.hpp` / `.cpp`, `IREEAdapter.hpp` / `.cpp`, `IREEInferenceOperatorHandler.hpp` / `.cpp`, `InferModelPhysicalOperator.cpp`, `InferModelLogicalOperator.cpp` (commits `101dc3d17a`, `e6892663c8`, `172f093734` in the NebulaStream repository).
- Existing test pattern: `nes-physical-operators/tests/EmitPhysicalOperatorTest.cpp` — establishes the correct `MockedPipelineContext` / `setup()` / lifecycle pattern.
- IREE C API behavior: `iree_allocator_null()` semantics documented in IREE runtime headers (non-owning allocator, caller retains lifetime responsibility).
- Known codebase issues: `CONCERNS.md` — `spdlog::shutdown()` disabled (test hang risk during logging teardown), `BackpressureHandler` state machine untested (pattern applicable here: lifecycle state machines need explicit state transition tests).
- CI configuration: `vcpkg/custom-triplets/x64-linux-address-libcxx.cmake` — confirms AddressSanitizer is in use in CI; leaks will be caught.

---
*Pitfalls research for: ML inference operator unit testing with real IREE runtime*
*Researched: 2026-03-11*
