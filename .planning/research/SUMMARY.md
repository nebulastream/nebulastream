# Project Research Summary

**Project:** ML Inference Operator Unit Tests (NebulaStream `ls/ml-operator`)
**Domain:** C++ unit testing for IREE-based ML inference operators in a stream processing engine
**Researched:** 2026-03-11
**Confidence:** HIGH

## Executive Summary

This project adds exhaustive unit tests for the `InferModelLogicalOperator` and `InferModelPhysicalOperator` classes in NebulaStream's `ls/ml-operator` branch. The feature is already implemented; what is missing is test coverage. The system uses IREE 3.10.0 (vcpkg-pinned) as its inference runtime, with the operator spanning four modules: `nes-inference` (Model value type and loader), `nes-logical-operators` (schema inference, plan integration), `nes-physical-operators` (IREE execution at record granularity), and the IREE C runtime itself. The recommended approach is a two-phase test suite: logical operator tests first (no IREE dependency, pure schema/interface coverage), followed by physical operator tests using a purpose-built tiny `.vmfb` test model.

The single most important prerequisite is creating and committing a minimal IREE model artifact (an identity or constant-output function, targeting <10KB) before any physical operator test can be written or run. This model must be pre-compiled offline using `iree-base-compiler==3.10.0` (matching the vcpkg runtime pin) and cannot be generated at test time — the CI environment has the IREE runtime library but not the compiler toolchain. All physical tests load this artifact from the test data directory and construct `NES::Inference::Model` directly, bypassing `ModelLoader::load()` entirely.

The critical risks are IREE resource leaks from raw pointer members in `IREERuntimeWrapper` that lack a destructor (will trigger AddressSanitizer failures in CI), a bytecode buffer lifetime trap where `iree_allocator_null()` means the IREE session borrows — not owns — the buffer (requiring the `Model` to outlive the session), and Nautilus backend sensitivity where `execute()` contains `nautilus::invoke()` calls that must run under the interpreted backend in unit tests. All three must be addressed in the test harness setup phase before any execute-path test is written.

---

## Key Findings

### Recommended Stack

The test stack works entirely within what is already present. GTest (via `Testing::BaseUnitTest`) is the test runner; the IREE C runtime (ireeruntime 3.10.0, vcpkg) is already linked into `nes-physical-operators`; and `nes-memory-test-utils` provides the `BufferManager` stubs needed for `PipelineExecutionContext`. The only new external tooling is a one-time offline use of `iree-base-compiler[onnx]==3.10.0` and the `onnx` Python library to produce the test model artifact.

**Core technologies:**
- **GTest + `Testing::BaseUnitTest`**: Test runner — project standard; `BaseUnitTest` adds timeout protection and log flushing. Every test must inherit from it.
- **IREE C runtime 3.10.0** (ireeruntime vcpkg port): Inference execution for physical operator tests — already linked; no additional dependency needed.
- **ONNX Python library + `iree-base-compiler[onnx]==3.10.0`** (offline, one-time): Model authoring — produces the tiny deterministic `.vmfb` test artifact. Must match the runtime version exactly; `.vmfb` format is not stable across minor versions.
- **`nes-inference`, `nes-memory-test-utils`, `nes-test-util`**: Internal libraries — `nes-inference` provides the `Model` value type; `nes-memory-test-utils` provides the buffer manager stub; `nes-test-util` provides `BaseUnitTest` and `ASSERT_EXCEPTION_ERRORCODE`.

**Critical version constraint:** `iree-base-compiler` pip package must be pinned to `==3.10.0` when compiling the test model. A version mismatch between compiler and runtime produces a cryptic status error at `iree_runtime_session_append_bytecode_module_from_memory` — the model appears to load but fails silently on first invocation.

### Expected Features (Test Cases)

**Must have (table stakes — Phase 1: logical tests):**
- `InferModelLogicalOperator` construction, `getName()`, `getChildren()`, `explain(Short/Debug)` — basic interface contract
- `withChildren()` immutability and child round-trip — plan rewriting correctness
- `withTraitSet()` / `getTraitSet()` round-trip — optimizer correctness
- Schema inference happy path: output fields appended in correct order with correct types
- Schema inference error paths: empty input schema, missing required field, type mismatch
- `getInputSchemas()` / `getOutputSchema()` after `withInferredSchema()` — compiler contract
- Equality: same operators equal, different model names not equal
- Reflection round-trip (`reflect()` / `unreflect()`) — serialization for distributed query plans

**Must have (table stakes — Phase 2: physical tests):**
- Tiny test model artifact created and committed (blocker for all physical tests)
- Physical operator construction, `setup()` with registered handler, full lifecycle (`open → execute → close → terminate`)
- `getChild()` / `setChild()` round-trip
- `execute()` output values numerically correct (EXPECT_NEAR against known-good expected values)
- Invalid model path at `setup()` throws; malformed model at `setup()` throws
- Missing input field at `execute()` fails cleanly (no UB/crash)

**Should have (Phase 3: differentiators):**
- Multi-tuple buffer execution (10+ tuples in one buffer)
- Zero-tuple buffer edge case
- `terminate()` idempotence (safe to call twice)
- Equality considers inferred schema, not just model name
- Output field name collision handling
- `withInferredSchema()` immutability (original unchanged)
- Schema inference: field names and positions asserted individually (not just count)

**Defer / out of scope:**
- Compilation rule tests (`ModelInferenceCompilationRule`, `LowerToPhysicalInferModel`) — separate concern
- E2E query execution — covered by existing systest
- Performance/throughput benchmarks — separate initiative
- Thread-safety / concurrent execution — not ML-operator-specific
- Mocking `IREEAdapter` or `IREEInferenceOperatorHandler` — explicitly excluded by PROJECT.md

### Architecture Approach

Tests mirror the four-module production structure. Logical operator tests live in `nes-logical-operators/tests/UnitTests/Operators/InferModelLogicalOperatorTest.cpp`; physical operator tests live in `nes-physical-operators/tests/InferModelPhysicalOperatorTest.cpp`; the shared test model artifact lives in `nes-inference/tests/testdata/tiny_identity.vmfb`. Two fixture patterns are established by the codebase: inline stub `Model` construction for logical tests (no IREE needed), and `MockedPipelineContext` inner struct for physical tests (following `EmitPhysicalOperatorTest.cpp`).

**Major components:**
1. **`Inference::Model`** — Value type holding IREE bytecode and field descriptors; constructed inline in logical tests (stub bytes + metadata); loaded from `.vmfb` in physical tests.
2. **`InferModelLogicalOperator`** — Stores a `Model`, implements `LogicalOperatorConcept`; tested without IREE involvement (pure schema/interface layer).
3. **`InferModelPhysicalOperator` + `IREEInferenceOperatorHandler` + `IREEAdapter`** — End-to-end IREE execution path; tested with real tiny model via `MockedPipelineContext` stub; `IREEInferenceOperatorHandler` and `IREEAdapter` exercised indirectly through the physical operator tests.
4. **`MockedPipelineContext` (test harness)** — Inner struct implementing `PipelineExecutionContext`; must return `getNumberOfWorkerThreads() == 1` (not 0) so the handler creates exactly one adapter; reused across all physical operator tests.

### Critical Pitfalls

1. **IREE resource leaks from `IREERuntimeWrapper`** — `instance`, `session`, and `device` are raw C pointers with no destructor; will fail AddressSanitizer CI. Verify or add a proper RAII destructor calling `iree_runtime_session_release()`, `iree_runtime_instance_release()`, and `iree_hal_device_release()` in reverse order before writing any physical test.

2. **Test model requires compiler toolchain, not just runtime** — Never call `ModelLoader::load()` or attempt to compile ONNX at test time. Pre-compile the tiny model offline, commit the `.vmfb` binary, and reference it via a CMake compile-time path constant (`target_compile_definitions`).

3. **`IREEInferenceOperatorHandler::start()` must be called before `execute()`** — `threadLocalAdapters` is empty until `start()` runs; accessing it causes division-by-zero UB. The test harness must call `setup()` on the physical operator (which triggers `handler->start()`) before any execute-path test. Follow the `EmitPhysicalOperatorTest` lifecycle pattern exactly.

4. **Bytecode buffer lifetime: `iree_allocator_null()` means IREE borrows, not owns** — If the `Model` object goes out of scope before the IREE session is destroyed, the session holds a dangling pointer. Store the `Model` (or the `IREEInferenceOperatorHandler` that owns it) in the test fixture, never as a local variable.

5. **Schema tests must assert field order, not just field count** — `withInferredSchema()` appends output fields positionally; wrong ordering causes silent type mismatches at runtime. Every schema inference test must assert `EXPECT_EQ(schema.getField(i).getName(), expectedName)` for each index, not just `EXPECT_EQ(schema.size(), N)`.

---

## Implications for Roadmap

Based on combined research, a three-phase structure is strongly indicated by hard technical dependencies.

### Phase 1: Logical Operator Tests

**Rationale:** `InferModelLogicalOperator` has zero IREE dependency — it operates purely on `Schema` types and the `LogicalOperatorConcept` interface. These tests can be written, built, and run immediately with no prerequisites. Starting here produces fast feedback, establishes the test fixture structure, and delivers meaningful coverage without any model artifact work.

**Delivers:** Complete unit coverage of the logical operator: construction, interface contract (getName, explain, children, traits), schema inference (happy path and all error paths), equality, and reflection round-trip.

**Addresses (from FEATURES.md):** All "Launch With — Phase 1" table-stakes features.

**Avoids (from PITFALLS.md):** Schema field order pitfall (Pitfall 6) — assert by index; `withInferredSchema()` immutability; reflection round-trip correctness.

**CMake:** Add `InferModelLogicalOperatorTest` to `nes-logical-operators/tests/CMakeLists.txt` linking `nes-logical-operators` and `nes-inference`.

**Research flag:** No deeper research needed — `LogicalOperatorConcept` interface is well-documented and the `SelectionLogicalOperator` provides a complete reference pattern.

---

### Phase 2: Tiny Test Model Creation

**Rationale:** This phase has no C++ code — it is the offline model-creation step. It is a hard prerequisite for all physical operator tests. Blocking it out as an explicit phase prevents it from being merged into Phase 3 and causing delays when the first physical test fails because no `.vmfb` exists.

**Delivers:** A committed binary artifact at `nes-inference/tests/testdata/tiny_identity.vmfb` (target: <10KB, identity or constant-output function, float `[1,2]` input/output), with documented expected inputs and outputs for use in `EXPECT_NEAR` assertions.

**Uses (from STACK.md):** Python 3.10+, `onnx==1.17.0`, `iree-base-compiler[onnx]==3.10.0`, `iree-import-onnx` and `iree-compile` with production flags (`--iree-hal-target-device=local --iree-hal-local-target-device-backends=llvm-cpu --iree-llvmcpu-target-cpu=host`). A generation script under `scripts/create_test_model.py` should be committed alongside the artifact.

**Avoids (from PITFALLS.md):** Compiler toolchain pitfall (Pitfall 2) — the artifact is committed, not generated at test time.

**Research flag:** No deeper research needed — the exact commands are verified against production `ModelLoader.cpp` flags and documented in STACK.md.

---

### Phase 3: Physical Operator Tests

**Rationale:** Depends on Phase 2 (requires the `.vmfb` artifact). The physical operator test harness must be established correctly before individual test cases are added — three of the critical pitfalls (resource leaks, uninitialized members, buffer lifetime) manifest in harness setup, not in individual test assertions.

**Delivers:** Complete unit coverage of the physical operator: construction, full lifecycle (`setup → open → execute → close → terminate`), output value numerical correctness, error paths (invalid model, malformed model, missing field), `getChild`/`setChild` contract.

**Uses (from STACK.md):** IREE C runtime 3.10.0 (already linked), `nes-memory-test-utils` (buffer manager stub), `nes-inference` (Model loading).

**Implements (from ARCHITECTURE.md):** `MockedPipelineContext` inner-struct pattern; `TEST_MODEL_PATH` compile-time constant via `target_compile_definitions`; full operator lifecycle harness.

**Avoids (from PITFALLS.md):**
- Pitfall 1 (IREE resource leaks) — verify `IREERuntimeWrapper` destructor before writing tests.
- Pitfall 3 (uninitialized members) — always call `setup()` before `execute()`.
- Pitfall 4 (Nautilus backend sensitivity) — use interpreted mode; document backend choice.
- Pitfall 5 (buffer lifetime) — store `Model`/handler in fixture, not as locals.
- Pitfall 7 (error paths crash vs. throw) — classify each error scenario before writing tests.
- Pitfall 8 (output not numerically validated) — include `EXPECT_NEAR` in all execute() tests.

**CMake:** Add `InferModelPhysicalOperatorTest` to `nes-physical-operators/tests/CMakeLists.txt` using `add_nes_physical_operator_test()`, link `nes-inference`, add `target_compile_definitions` for the testdata path.

**Research flag:** The `IREERuntimeWrapper` destructor gap needs verification during implementation — if the destructor is absent, it must be added before tests are written (not after). This is a code change, not just a test concern.

---

### Phase 4: Differentiator Tests (Hardening)

**Rationale:** After Phase 3 delivers the mandatory coverage, this phase adds the tests that distinguish a thorough suite from a minimal one. No new infrastructure needed — all harness is in place from Phase 3.

**Delivers:** Multi-tuple buffer execution, zero-tuple edge case, `terminate()` idempotence, schema equality with inferred schema, field name collision handling, `withInferredSchema()` immutability, and full field-order assertions on all schema tests.

**Addresses (from FEATURES.md):** All "Add After Phase 2 (Differentiators)" items.

**Research flag:** No additional research needed — all patterns established.

---

### Phase Ordering Rationale

- **Logical before physical:** Logical operator tests have zero prerequisites; physical tests require both the `.vmfb` artifact and a correct harness. Starting with logical maximizes early feedback.
- **Model creation as a standalone phase:** The `.vmfb` artifact is a blocking binary dependency with a non-trivial offline compilation step. Treating it as an explicit phase ensures it is not deprioritized and prevents Phase 3 from stalling.
- **Harness correctness before test volume:** The three harness-level pitfalls (resource leaks, uninitialized members, buffer lifetime) are invisible to individual test assertions — they manifest as ASan failures or intermittent crashes across the full suite. The harness must be correct before test count is increased.
- **Differentiators last:** All differentiator tests reuse Phase 3 infrastructure. Writing them last means any harness corrections from Phase 3 are already in place.

### Research Flags

Phases needing deeper investigation during implementation:
- **Phase 2 (model creation):** If building on a CPU architecture where `--iree-llvmcpu-target-cpu=host` produces an incompatible binary for CI machines (e.g., local x86 AVX512 vs. CI x86 baseline), substitute `generic`. Verify CI CPU capabilities before committing the artifact.
- **Phase 3 (harness setup):** The `IREERuntimeWrapper` destructor must be inspected at implementation time. If absent, adding it is a correctness fix that should be filed as a separate concern and tracked — it affects production code, not just tests.
- **Phase 3 (Nautilus backend):** Verify which Nautilus backend is active in the test environment by checking how `EmitPhysicalOperatorTest` configures it. If `execute()` cannot be called directly (compiled-only mode), physical tests must use the Nautilus test runner utilities instead.

Phases with standard, well-documented patterns (no additional research needed):
- **Phase 1 (logical tests):** `SelectionLogicalOperator` and `LogicalPlanTest.cpp` provide complete reference patterns. All interface methods are defined by `LogicalOperatorConcept`.
- **Phase 4 (differentiators):** All infrastructure in place from Phase 3.

---

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | IREE version confirmed from vcpkg.json; compiler flags verified against production `ModelLoader.cpp`; GTest and internal library versions are repo-locked. No uncertainty. |
| Features | HIGH | Derived directly from codebase inspection of `LogicalOperatorConcept`, `PhysicalOperatorConcept`, `EmitPhysicalOperatorTest.cpp`, and `PROJECT.md` scope decisions. |
| Architecture | HIGH | All file locations, CMake macros, test fixture patterns, and module boundaries confirmed by direct source inspection. `MockedPipelineContext` pattern verified against `EmitPhysicalOperatorTest.cpp`. |
| Pitfalls | HIGH | Derived from direct source inspection of `IREERuntimeWrapper`, `IREEAdapter`, `IREEInferenceOperatorHandler`. Three of the eight pitfalls are confirmed code-level issues (no destructor, `iree_allocator_null()` non-ownership, `getNumberOfWorkerThreads()` returning 0). |

**Overall confidence:** HIGH

### Gaps to Address

- **`IREERuntimeWrapper` destructor status:** Research confirmed the gap (raw C pointers, no destructor calls in any current source). Whether this is an existing bug or an intentional omission must be confirmed before writing physical tests. If it is a bug, a fix must be merged or it will fail ASan CI. Handle at Phase 3 kickoff.
- **Nautilus backend configuration for physical operator `execute()` tests:** `EmitPhysicalOperatorTest` tests `open()`/`close()` but does not appear to test `execute()` directly. The exact mechanism for controlling which Nautilus backend runs in unit tests was not resolved. This needs a 15-minute code investigation at Phase 3 start: check how `nes-nautilus/tests/` configures backends and whether `InferModelPhysicalOperator::execute()` can be called without a JIT compilation path.
- **`Model` constructor signature for `NES::Inference::Model(shared_ptr<byte[]>, size_t)`:** Confirmed from reading `IREEAdapter.cpp` context; the exact constructor signature should be verified against `nes-inference/include/Model.hpp` before writing test helpers. Low risk — failure is a compile error, not a runtime bug.

---

## Sources

### Primary (HIGH confidence — direct codebase inspection)

- `nes-physical-operators/src/Inference/IREERuntimeWrapper.cpp` — IREE C API usage, raw pointer members, absence of destructor
- `nes-physical-operators/tests/EmitPhysicalOperatorTest.cpp` — canonical physical operator test pattern (`MockedPipelineContext`, lifecycle)
- `nes-logical-operators/tests/LogicalPlanTest.cpp` — logical operator test patterns (equality, children, traits)
- `nes-inference/ModelLoader.cpp` — production `iree-compile` flags; why `load()` must not be called from unit tests
- `vcpkg/vcpkg-registry/ports/ireeruntime/vcpkg.json` — IREE runtime version 3.10.0 confirmed
- `nes-physical-operators/include/Inference/IREEAdapter.hpp`, `IREEInferenceOperatorHandler.hpp` — adapter lifecycle, `iree_allocator_null()` semantics
- `nes-logical-operators/include/Operators/InferModelLogicalOperator.hpp` / `SelectionLogicalOperator.hpp` — `LogicalOperatorConcept` interface
- `nes-physical-operators/include/InferModelPhysicalOperator.hpp` — `PhysicalOperatorConcept` interface
- `.planning/PROJECT.md` — explicit scope decisions (no mocking, real tiny model, no compilation rule tests)
- `vcpkg/custom-triplets/x64-linux-address-libcxx.cmake` — confirms AddressSanitizer active in CI

### Secondary (HIGH confidence — official documentation)

- [IREE ONNX guide](https://iree.dev/guides/ml-frameworks/onnx/) — `iree-import-onnx` + `iree-compile` pipeline and pip package
- [ONNX Python API docs](https://onnx.ai/onnx/intro/python.html) — `make_model`, `make_node`, `make_graph` helpers
- [iree-base-compiler on PyPI](https://pypi.org/project/iree-base-compiler/) — version 3.10.0 confirmed as January 2026 release
- [IREE v3.10.0 release notes](https://github.com/iree-org/iree/releases/tag/v3.10.0) — released 2025-02-02

---

*Research completed: 2026-03-11*
*Ready for roadmap: yes*
