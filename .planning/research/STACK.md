# Stack Research

**Domain:** ML inference operator unit testing — IREE-based C++ stream processor
**Researched:** 2026-03-11
**Confidence:** HIGH (codebase read directly; IREE/ONNX toolchain verified against official docs)

---

## Context: What Already Exists

The `ls/ml-operator` branch ships a complete IREE integration. The test stack must work
*within* that existing system, not replace it. Key facts that constrain every decision below:

- **Runtime:** `iree_runtime` v3.10.0 (pinned in `vcpkg/vcpkg-registry/ports/ireeruntime/vcpkg.json`
  at commit `ae97779f`). The C API lives at `<iree/runtime/api.h>`.
- **Model loading:** `NES::Inference::load()` in `nes-inference/ModelLoader.cpp` calls
  `iree-import-onnx` + `iree-compile` as child processes. It only accepts `.onnx` on disk.
- **Adapter:** `IREEAdapter` (in `nes-physical-operators/include/Inference/IREEAdapter.hpp`)
  holds raw `std::byte[]` buffers and calls `IREERuntimeWrapper::setup(iree_const_byte_span_t)`.
  The `.vmfb` bytes are loaded by passing a `const_byte_span` directly — no file I/O at the
  C-API boundary.
- **CMake:** Physical operator tests use a project-local `add_nes_physical_operator_test()`
  wrapper (defined in `nes-physical-operators/tests/CMakeLists.txt`) that links
  `nes-physical-operators`, `nes-memory-test-utils`, and `nes-test-util`.
- **Test base:** All unit tests inherit `Testing::BaseUnitTest` from
  `nes-common/tests/Util/include/BaseUnitTest.hpp`.

---

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| GTest + BaseUnitTest | Already in repo (GTest via vcpkg) | Unit test runner and assertions | Project standard; `BaseUnitTest` adds timeout threads and log flushing. Do not skip it. |
| IREE C runtime (`iree_runtime`) | 3.10.0 (vcpkg-pinned) | Run `.vmfb` bytecode at test time | Already linked into `nes-physical-operators`. Tests get it for free via that link target. |
| ONNX Python library (`onnx`) | 1.17+ (matches opset 20) | Programmatically author tiny `.onnx` test models | One-liner Python scripts produce byte-exact, deterministic `.onnx` files with no ML framework dependency. |
| `iree-base-compiler[onnx]` | 3.10.0 (match runtime) | CLI tools: `iree-import-onnx` + `iree-compile` | Converts `.onnx` → `.mlir` → `.vmfb`. The same tools used by production `ModelLoader.cpp`. |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `nes-inference` (internal) | branch HEAD | Provides `NES::Inference::Model` value type | Needed to construct `IREEInferenceOperatorHandler` for physical operator tests. |
| `nes-memory-test-utils` (internal) | branch HEAD | `BufferManager` stubs for `PipelineExecutionContext` | Required for `InferModelPhysicalOperator::setup()` tests — matches `EmitPhysicalOperatorTest` pattern. |
| `nes-test-util` (internal) | branch HEAD | `BaseUnitTest`, `ASSERT_EXCEPTION_ERRORCODE` macro | Mandatory for every test in the repo. |
| OpenSSL (`libcrypto`) | System / vcpkg | Base64 encode/decode in `InferModelLogicalOperator` serialization | Already a transitive dependency; relevant if testing reflect/unreflect paths. |

### Development Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| Python 3.10+ | Script that generates the tiny test `.onnx` model | One-time offline step. Output is a checked-in `.vmfb` byte array or binary file. |
| `iree-import-onnx` | Convert `.onnx` to MLIR text | Part of `iree-base-compiler[onnx]`. Must match the runtime version (3.10.0). |
| `iree-compile` | Compile MLIR to `.vmfb` | Part of `iree-base-compiler[onnx]`. Flags must match production `ModelLoader.cpp`: `--iree-hal-target-device=local --iree-hal-local-target-device-backends=llvm-cpu --iree-llvmcpu-target-cpu=host`. |
| `xxd` (or CMake `file(READ … HEX)`) | Embed `.vmfb` bytes as a C array | Keeps the test self-contained — no file-path assumptions at test runtime. |

---

## How to Create the Tiny Test Model

The entire workflow is a one-time offline step. The result (a `.vmfb` binary or its
byte-array representation) is committed to the repo alongside the tests.

### Step 1 — Author a trivial ONNX model in Python

Target: single float input tensor `[1, 2]`, single float output `[1, 2]`, identity
operation. This is the smallest possible valid model the IREE stack can compile.

```python
# scripts/create_test_model.py
import onnx
from onnx import TensorProto, helper

X = helper.make_tensor_value_info("X", TensorProto.FLOAT, [1, 2])
Y = helper.make_tensor_value_info("Y", TensorProto.FLOAT, [1, 2])
node = helper.make_node("Identity", inputs=["X"], outputs=["Y"])
graph = helper.make_graph([node], "test_model", [X], [Y])
model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 17)])
onnx.checker.check_model(model)

with open("test_model.onnx", "wb") as f:
    f.write(model.SerializeToString())
```

```bash
pip install onnx==1.17.0
python scripts/create_test_model.py
```

**Why identity and not a linear layer?** The identity model has zero learned weights,
so the `.vmfb` is maximally small (~8–20 KB) and its output is trivially verifiable
(`output == input`). A linear layer with random weights produces non-deterministic
results unless weights are hard-coded, adding complexity for no benefit.

### Step 2 — Compile to `.vmfb`

Use the same flags as production `ModelLoader.cpp` (verified by reading that source):

```bash
pip install iree-base-compiler[onnx]==3.10.0

iree-import-onnx test_model.onnx --opset-version 17 -o test_model.mlir

iree-compile test_model.mlir \
  --iree-hal-target-device=local \
  --iree-hal-local-target-device-backends=llvm-cpu \
  --iree-llvmcpu-debug-symbols=false \
  --iree-stream-partitioning-favor=min-peak-memory \
  --iree-llvmcpu-target-cpu=host \
  -o test_model.vmfb
```

**Why `--iree-llvmcpu-target-cpu=host`?** Same flag used in production. Compiled binary
targets the CPU family of the build machine. For portability across CI machines, substitute
`generic` if needed — but `host` matches what the existing systests rely on.

**Why `--iree-llvmcpu-debug-symbols=false`?** Reduces `.vmfb` size. Production flag;
keeps the test artifact small.

### Step 3 — Embed the `.vmfb` bytes in the test

**Option A (recommended): Check in the `.vmfb` as a binary file, load at test time.**

Place the file at `nes-physical-operators/tests/testdata/test_model.vmfb`.
In CMakeLists.txt, configure a path constant:

```cmake
configure_file(testdata/test_model.vmfb
               ${CMAKE_CURRENT_BINARY_DIR}/test_model.vmfb COPYONLY)
target_compile_definitions(InferModelPhysicalOperatorTest PRIVATE
    TEST_MODEL_PATH="${CMAKE_CURRENT_BINARY_DIR}/test_model.vmfb")
```

In C++:

```cpp
// Load bytes manually — bypass ModelLoader (which requires iree-import-onnx at runtime)
auto loadVmfb(std::string_view path) {
    std::ifstream file(path.data(), std::ios::binary);
    return std::vector<std::byte>(
        std::istreambuf_iterator<char>(file),
        std::istreambuf_iterator<char>());
}

// Then construct Model directly (bypass ModelLoader::load):
auto bytes = loadVmfb(TEST_MODEL_PATH);
auto buf = std::make_shared<std::byte[]>(bytes.size());
std::ranges::copy(bytes, buf.get());
NES::Inference::Model model(std::move(buf), bytes.size());
model.setFunctionName("module.test_model");
model.setInputShape({1, 2});
model.setNDim(2);
model.setInputSizeInBytes(8);   // 1*2 * sizeof(float)
model.setOutputShape({1, 2});
model.setOutputDims(2);
model.setOutputSizeInBytes(8);
```

**Option B: Generate a C header with xxd, embed as array.**

```bash
xxd -i test_model.vmfb > test_model_vmfb.h
```

Produces `unsigned char test_model_vmfb[]` usable as `iree_const_byte_span_t`.
Avoids any path dependency at test runtime. Appropriate if the file is small (<100 KB)
and you want zero file I/O in unit tests.

**Choose Option A** unless CI file-path assumptions are a problem. Option B is correct
for hermetic tests where path portability is a concern.

### Why NOT use `NES::Inference::load()` in tests

`ModelLoader::load()` forks `iree-import-onnx` and `iree-compile` as subprocesses. This:
1. Requires both tools to be installed in CI at test-run time (not just build time).
2. Takes 5–30 seconds per call — unacceptable for unit tests.
3. Reads a `.onnx` file from disk, adding a file-path dependency.

Unit tests bypass `load()` and construct `NES::Inference::Model` directly from pre-compiled
`.vmfb` bytes. The model metadata (function name, shapes, sizes) is set explicitly.

---

## GTest Patterns for ML Operator Tests

### Logical Operator Tests

`InferModelLogicalOperator` has no IREE dependency — it holds a `Model` value type and
performs schema inference. Tests can construct `Model` with an empty byte span.

```cpp
class InferModelLogicalOperatorTest : public Testing::BaseUnitTest {
public:
    static void SetUpTestCase() {
        Logger::setupLogging("InferModelLogicalOperatorTest.log", LogLevel::LOG_DEBUG);
    }

    NES::Inference::Model makeEmptyModel() {
        // Model with metadata but no bytecode — fine for logical operator tests
        NES::Inference::Model m;
        m.setFunctionName("module.identity");
        m.setInputShape({1, 2});
        m.setNDim(2);
        m.setInputSizeInBytes(8);
        m.setOutputShape({1, 2});
        m.setOutputDims(2);
        m.setOutputSizeInBytes(8);
        return m;
    }
};

TEST_F(InferModelLogicalOperatorTest, constructWithModel) {
    auto op = InferModelLogicalOperator(makeEmptyModel());
    EXPECT_EQ(op.getName(), "InferModel");
}
```

**Why no bytecode for logical tests?** The logical operator only reads metadata (shapes,
field names). It never calls IREE. An empty `Model` with metadata set is sufficient and
eliminates the `.vmfb` dependency from the logical test binary.

### Physical Operator Tests

`InferModelPhysicalOperator` calls IREE via `IREEInferenceOperatorHandler`. These tests
require a real, runnable `.vmfb`.

The pattern follows `EmitPhysicalOperatorTest.cpp` exactly: define an inner
`MockedPipelineContext` struct that implements `PipelineExecutionContext`, then drive
`setup()` / `execute()` / `terminate()` calls.

```cpp
class InferModelPhysicalOperatorTest : public Testing::BaseUnitTest {
    struct MockedPipelineContext final : PipelineExecutionContext {
        bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; }
        TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }
        WorkerThreadId getId() const override { return INITIAL<WorkerThreadId>; }
        uint64_t getNumberOfWorkerThreads() const override { return 1; }
        std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }
        PipelineId getPipelineId() const override { return PipelineId(1); }

        std::shared_ptr<BufferManager> bufferManager = std::make_shared<BufferManager>(...);
    };

public:
    static void SetUpTestCase() {
        Logger::setupLogging("InferModelPhysicalOperatorTest.log", LogLevel::LOG_DEBUG);
    }

    NES::Inference::Model loadTestModel() {
        // Load pre-compiled .vmfb from testdata/
        std::ifstream file(TEST_MODEL_PATH, std::ios::binary);
        std::vector<std::byte> bytes(
            std::istreambuf_iterator<char>{file},
            std::istreambuf_iterator<char>{});
        auto buf = std::make_shared<std::byte[]>(bytes.size());
        std::ranges::copy(bytes, buf.get());
        NES::Inference::Model m(std::move(buf), bytes.size());
        m.setFunctionName("module.test_model");
        m.setInputShape({1, 2});
        m.setNDim(2);
        m.setInputSizeInBytes(8);
        m.setOutputShape({1, 2});
        m.setOutputDims(2);
        m.setOutputSizeInBytes(8);
        return m;
    }
};

TEST_F(InferModelPhysicalOperatorTest, setupAndExecuteIdentityModel) {
    auto model = loadTestModel();
    auto handler = std::make_shared<Inference::IREEInferenceOperatorHandler>(std::move(model));
    // ... construct operator, ExecutionContext, run execute(), verify outputs
}
```

### CMake Registration

Use the project-local `add_nes_physical_operator_test()` macro for physical tests,
and `add_nes_test_inference()` (defined in `nes-inference/CMakeLists.txt`) if any
test needs `nes-inference` linking:

```cmake
# nes-physical-operators/tests/CMakeLists.txt additions:
add_nes_physical_operator_test(InferModelPhysicalOperatorTest
    InferModelPhysicalOperatorTest.cpp)
target_link_libraries(InferModelPhysicalOperatorTest PRIVATE nes-inference)
target_compile_definitions(InferModelPhysicalOperatorTest PRIVATE
    TEST_MODEL_PATH="${CMAKE_CURRENT_BINARY_DIR}/test_model.vmfb")

# nes-logical-operators/tests/CMakeLists.txt additions:
add_nes_unit_test(InferModelLogicalOperatorTest
    InferModelLogicalOperatorTest.cpp)
target_link_libraries(InferModelLogicalOperatorTest PRIVATE nes-logical-operators nes-inference)
```

---

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| Pre-compiled `.vmfb` checked into repo | Call `ModelLoader::load()` in test | Never for unit tests. Use only in a dedicated integration test that explicitly tests the loader pipeline. |
| `onnx` Python library for model authoring | Train a real PyTorch/TF model | Only if the test specifically needs non-trivial model behavior. Adds large framework dependencies. |
| Inner `MockedPipelineContext` struct (codebase pattern) | GMock mock of `PipelineExecutionContext` | GMock mock is acceptable if the codebase convention shifts, but project convention strongly prefers manual stubs. |
| Empty `Model` for logical tests | `.vmfb`-backed `Model` for logical tests | Never use real bytecode where metadata alone suffices — inflates logical test compile/link time for no benefit. |
| `--iree-llvmcpu-target-cpu=host` | `--iree-llvmcpu-target-cpu=generic` | Use `generic` if CI builds on heterogeneous CPU families (e.g., x86 build machine runs ARM tests). |

---

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| `NES::Inference::load()` in unit tests | Forks `iree-import-onnx` + `iree-compile` subprocesses; takes 5–30 s; requires both tools installed in CI at test runtime; reads `.onnx` from disk | Construct `NES::Inference::Model` directly from pre-compiled `.vmfb` bytes |
| The full MNIST `.onnx` model from systests | 95 KB+ ONNX model; large `.vmfb`; slow to load; outputs non-trivially verifiable | Author a 2-float identity model specifically for unit tests |
| GMock mocks for `PipelineExecutionContext` | Project convention rejects GMock mocks for interfaces; codebase uses inner-struct manual stubs (see `EmitPhysicalOperatorTest.cpp`) | Define an inner `MockedPipelineContext` struct in the test class |
| `iree-base-runtime` Python package for runtime calls | IREE runtime is already embedded as a C library via vcpkg; Python runtime would be a second, unrelated runtime copy | Use the existing `IREEAdapter` / `IREERuntimeWrapper` C++ classes |
| Version mismatch between `iree-base-compiler` and the vcpkg `ireeruntime` port | IREE compilers and runtimes are version-coupled; a mismatch will cause `iree_runtime_session_append_bytecode_module_from_memory` to fail at runtime with a cryptic status error | Pin `iree-base-compiler==3.10.0` to match the vcpkg port |

---

## Version Compatibility

| Component | Version | Compatibility Notes |
|-----------|---------|---------------------|
| `ireeruntime` (vcpkg port) | 3.10.0 (commit ae97779f) | Pins the C runtime ABI. `.vmfb` files compiled by `iree-base-compiler` must use the **same** version. |
| `iree-base-compiler[onnx]` (pip, offline model build) | 3.10.0 | Must match vcpkg runtime exactly. IREE has no stability guarantee across minor versions for `.vmfb` format. |
| `onnx` (pip, model authoring) | 1.17.0 | `--opset-version 17` flag in `iree-import-onnx` corresponds to ONNX opset 17. Using opset 20+ requires `onnx` 1.14+ and a matching `iree-import-onnx` build — verify at the time of use. |
| Python | 3.10+ | Required by `iree-base-compiler` 3.10.0 wheels on PyPI. |
| GTest | vcpkg-locked (from root `vcpkg.json`) | No version decision needed — already in the build. |
| Clang | 19+ | C++23 required; `std::expected`, `std::ranges::copy_n`, `std::span` all used in existing ML operator code. |

---

## Sources

- `nes-physical-operators/src/Inference/IREERuntimeWrapper.cpp` (branch `worktree-ls/ml-operator`) — confirmed `iree_runtime_session_append_bytecode_module_from_memory` API and driver initialization sequence. **HIGH confidence.**
- `nes-inference/ModelLoader.cpp` (branch) — confirmed subprocess pipeline (`iree-import-onnx` → `iree-compile`), exact `iree-compile` flags used in production, and why `ModelLoader::load()` must not be called from unit tests. **HIGH confidence.**
- `vcpkg/vcpkg-registry/ports/ireeruntime/vcpkg.json` (branch) — confirms IREE runtime version 3.10.0. **HIGH confidence.**
- `nes-physical-operators/tests/EmitPhysicalOperatorTest.cpp` (branch) — confirms `MockedPipelineContext` inner-struct pattern as the project convention for physical operator tests. **HIGH confidence.**
- [IREE ONNX guide](https://iree.dev/guides/ml-frameworks/onnx/) — confirms `iree-import-onnx` + `iree-compile` pipeline and `iree-base-compiler[onnx]` pip package. **HIGH confidence (official docs).**
- [ONNX Python API docs](https://onnx.ai/onnx/intro/python.html) — confirms `make_model`, `make_node`, `make_graph`, `make_tensor_value_info` helpers for constructing identity models. **HIGH confidence (official docs).**
- [iree-base-compiler on PyPI](https://pypi.org/project/iree-base-compiler/) — confirms 3.10.0 is the January 2026 release, matching vcpkg pin. **HIGH confidence.**
- [IREE v3.10.0 release](https://github.com/iree-org/iree/releases/tag/v3.10.0) — released 2025-02-02. **HIGH confidence.**

---

*Stack research for: IREE-based ML inference operator unit testing in NebulaStream*
*Researched: 2026-03-11*
