# Model Inference

This document describes how NebulaStream evaluates ML models inside a streaming
query. It is aimed at contributors who need to extend, debug, or replace the
current implementation.

## Overview

NebulaStream currently relies on [IREE] (the IREE Runtime Engine) for model
inference. IREE compiles a tensor program down to its own bytecode and
executes it on a small runtime that abstracts the compute device behind a
*Hardware Abstraction Layer* (HAL). For every query that contains a
`MODEL_INFERENCE(...)` table-valued function, NES turns the registered model
into IREE bytecode, instantiates a per-worker-thread IREE runtime, and
invokes the model once per record.

Everything that touches IREE lives in the `nes-inference` module. The logical
and physical operators (`InferModelLogicalOperator`,
`InferModelPhysicalOperator`) and the per-runtime wrapper
(`InferenceRuntime`) carry only generic concepts (MLIR text, compiled
bytecode, input/output shapes). The intent is that swapping the backend
later does not propagate through the query optimizer or the operator code.

[IREE]: https://iree.dev

## Tooling and integration boundary

Two IREE binaries are used:

- `iree-import-onnx` — ONNX → MLIR (the `torch` dialect, as `!torch.vtensor<[...],f32>`).
- `iree-compile` — MLIR → IREE bytecode for a specific target.

NES does not link against the IREE compiler library. Both binaries are
installed with `pip install iree-base-compiler iree-turbine onnx` into a
dedicated venv inside the development and runtime Docker images
(`docker/dependency/Development.dockerfile`,
`docker/runtime/RuntimeBase.dockerfile`); `nes-inference` discovers them on
`$PATH` and shells out to them.

The reason for shelling out is that NES already builds against LLVM/MLIR 19
via its own MLIR package (used by `nes-nautilus`). IREE 3.11 ships its own,
ABI-incompatible LLVM/MLIR snapshot. Linking the IREE compiler embedded API
into NES would pull in two LLVM toolchains in the same address space; the
Python distributions side-step that by being self-contained processes.

### Installing the tools on bare metal

If you are running NES outside the provided Docker images you have to install
`iree-compile` and `iree-import-onnx` yourself, with the same version as the
runtime checks against (currently `IREE 3.11.0`). The recommended path
mirrors what the Docker images do:

```sh
pip install \
    iree-base-compiler==3.11.0 \
    iree-turbine \
    onnx
iree-compile     --version  # sanity check
iree-import-onnx --version  # sanity check
```

Both binaries must be reachable on `$PATH` of every process that handles `CREATE MODEL` (the coordinator and the offline
`nebucli`) **and** on `$PATH` of every worker process (which compiles MLIR to bytecode at lowering time).
A version mismatch, for example, an older `iree-compile` paired with a runtime built against IREE 3.11, produces the
same effect: `iree-compile --version` is parsed at runtime startup, the mismatch disables inference, and queries that
try to use a model are rejected.

Only the IREE *runtime* library (the C API behind `<iree/runtime/api.h>`)
is linked into NES, and only into `nes-inference-runtime`. Every other
target depends on `nes-inference`, which never includes an IREE header.

## Lifecycle

A model goes through four stages from registration to execution:

```
CREATE MODEL          parse              optimizer            lowering
─────────────►  catalog  ────►  name op  ────►  model op  ────►  physical op
   import        (MLIR)        (resolved        (compiled        (per-thread
                                schema)         bytecode)        runtime)
```

### 1. Registration

Models are registered before any query can reference them. Two paths feed
into the same `ModelStatementHandler`:

- SQL: `CREATE MODEL name ('/path/to/model.onnx') INPUT (...) OUTPUT (...);`
- Topology YAML: a top-level `models:` list (see
  `docs/guide/nebulastream-frontend.md`).

`ModelCatalog::registerModel` validates the request and stores it:

1. The path on disk must exist. The check happens wherever `CREATE MODEL`
   is processed — that is the coordinator for a deployed cluster, or the
   `nebucli` process for the offline CLI. The model file does **not** need
   to exist on the workers; only the importer's host needs it.
2. The ONNX is converted to MLIR via `iree-import-onnx`.
3. `MlirAnalyzer` scrapes the function name and the input/output tensor
   shapes from the MLIR text and returns a `ModelSignature`.
4. The user-declared input/output schemas are validated against the
   tensor shape: every non-VARSIZED field must be `FLOAT32`, the field
   count must equal the tensor element count, and `VARSIZED` is only
   accepted as a single bulk-byte field that mirrors the entire tensor
   verbatim. This makes the `(model, schema)` pair an invariant carried
   downstream.

Once accepted, the catalog stores the imported MLIR (a refcounted byte
buffer plus the signature) keyed by name.

### 2. Query parsing

A query references a model with the table-valued function

```sql
SELECT * FROM MODEL_INFERENCE(model_name, upstream_stream) INTO sink;
```

The parser does not resolve `model_name` — it builds an
`InferModelNameLogicalOperator` that carries only the literal name string
plus the list of input field names. The model itself is not yet attached.

The contract for input/output handling at this stage is:

- Every field name listed in the model's input schema must be present in
  the upstream stream's schema. Aliasing or projection upstream is the
  caller's responsibility.
- The model's output fields are appended to the operator's output schema.
- Every other upstream field is passed through unchanged.

### 3. Resolution

`InferModelResolutionRule` (a semantic-analyzer rule, see
`nes-query-optimizer/src/Rules/Semantic/`) walks the plan, looks each
`InferModelNameLogicalOperator` up in the catalog, and replaces it with
an `InferModelLogicalOperator` that owns the resolved `RegisteredModel`
(imported MLIR + validated schema).

The rule must run *before* `TypeInferenceRule`, because schema inference for the operator
depends on knowing the model's output fields, which only exist after
resolution. This ordering is enforced by `requiredBy()` in the rule.

### 4. Lowering and compilation

IREE bytecode is architecture-dependent: the compile flags
(`--iree-llvmcpu-target-cpu=host`) tell `iree-compile` to use the host
CPU's feature set. Cross-compiling for a heterogeneous worker pool
would require a target-CPU mapping per worker; that infrastructure does
not exist yet, so compilation is deferred to the workers, where it can
use native compilation.

This happens during logical-to-physical lowering
(`LowerToPhysicalInferModel` in `nes-query-compiler`):

1. `compileModel(imported)` shells out to `iree-compile` with the MLIR on
   stdin and reads bytecode from stdout. This is unrelated to NES's own
   query compilation pipeline (Nautilus); it is a separate subprocess
   invocation per logical operator.
2. The signature flows through unchanged — the compiled model carries the
   same function name and shapes.
3. An `InferModelPhysicalOperator` is constructed with the compiled model,
   the input field names, and the output field names.

### 5. Per-record execution

`InferModelPhysicalOperator` owns a `ThreadLocalRuntimeWrapper` (a
`shared_ptr` for pointer stability). At pipeline setup, the wrapper
allocates one `InferenceRuntime` per worker thread, each of which:

- Creates an IREE instance and a `local-sync` HAL device.
- Loads the bytecode into a session.
- Allocates I/O byte buffers sized from the model's signature.

Per record, the operator:

1. Writes the input fields into the runtime's input buffer (one f32 slot
   per FLOAT32 field, or a `memcpy` of the whole VARSIZED blob for the
   single-field bulk-byte case).
2. Invokes the runtime, which pushes the buffer as a HAL buffer view and
   dispatches the call.
3. Reads the output buffer back into the record.

There are no batch optimizations. Each tuple is fed in and read out
individually, even though IREE supports batched inputs in principle —
this is the most obvious place to optimize next.

## Constraints and known limitations

The constraints come from three places: the analyzer (in NES), the
compiler flags we pass to `iree-compile`, and the runtime build flags
we pass to the IREE runtime CMake.

### From `MlirAnalyzer`

- Only f32 tensor element types. Mixed precision, int8 quantized models,
  fp16, etc. are rejected at registration time.
- A single tensor input and a single tensor output. Multi-input or
  multi-output graphs are not supported (the regex matches one `func.func`
  signature with one argument and one return).
- ONNX is the only accepted format (`.onnx` extension check). Importing
  a TFLite or PyTorch model would require a separate importer path.
- `iree-import-onnx` is invoked without an `--opset-version`; whatever
  default the installed importer picks is what's used.

### From the `iree-compile` flags

| Flag                                               | Implication                                                                                                                                                                                                    |
|----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--iree-hal-target-device=local`                   | CPU only — no GPU or accelerator targets.                                                                                                                                                                      |
| `--iree-hal-local-target-device-backends=llvm-cpu` | The LLVM-CPU backend; no `vmvx` (interpreted) fallback, so any model with ops the LLVM-CPU backend cannot lower will fail to compile.                                                                          |
| `--iree-llvmcpu-target-cpu=host`                   | Bytecode is tuned to the worker's microarchitecture. A cluster with heterogeneous CPUs would need per-worker compilation (which we already do, by accident). The bytecode is **not** portable across machines. |
| `--iree-llvmcpu-debug-symbols=false`               | No debug symbols in the JITed code. Crashes inside the model show up as opaque addresses.                                                                                                                      |
| `--iree-stream-partitioning-favor=min-peak-memory` | The stream partitioner optimizes for memory footprint over throughput. For long-running streaming workloads we may want to revisit this.                                                                       |

### From the IREE runtime build flags

The runtime is built with `IREE_BUILD_COMPILER=OFF` and a deliberately
minimal driver/backend set (see `.nix/ireeruntime/package.nix` and
`vcpkg/vcpkg-registry/ports/ireeruntime/portfile.cmake`):

| Flag                                                             | Implication                                                                                                                                                                             |
|------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `IREE_ENABLE_THREADING=OFF`                                      | The runtime is single-threaded inside one session. We compensate at the operator level by holding one session per worker thread; you cannot rely on IREE's own task-system parallelism. |
| `IREE_HAL_DRIVER_DEFAULTS=OFF` + `IREE_HAL_DRIVER_LOCAL_SYNC=ON` | Only the synchronous local CPU driver is registered — no CUDA, Vulkan, Metal, etc. The `local-sync` driver runs the dispatch in the calling thread without an executor pool.            |
| `IREE_TARGET_BACKEND_DEFAULTS=OFF`                               | The runtime image carries no compile backends; it only loads pre-compiled bytecode.                                                                                                     |
| `IREE_INPUT_TORCH=OFF`                                           | Runtime-side input dialect handling for torch is off; input dialect lowering happens entirely in the Python compiler.                                                                   |

### Other operational caveats

- Compilation is not cached. Every query that uses a model recompiles it
  on the worker, even if an identical model has been compiled before.
- `iree-compile` and `iree-import-onnx` are version-pinned (currently
  `IREE 3.11.0`) and the runtime checks the `iree-compile --version`
  string at startup; a mismatch disables inference and is reported by
  `inferenceEnabled()`.
- Model files must be reachable by the process that handles `CREATE MODEL`,
  not by the workers. The catalog ships imported MLIR, not the original
  ONNX, across the wire.

## Glossary

- **HAL** — Hardware Abstraction Layer. IREE's runtime layer between the
  bytecode interpreter and the actual compute device. It defines devices,
  allocators, buffers, command buffers, and synchronization primitives.
  *Drivers* implement HAL for specific backends (CPU, CUDA, Vulkan,
  WebGPU, …). We compile in only one driver, `local-sync`, which is the
  simplest of them: a synchronous, in-process CPU executor that runs
  dispatches on the calling thread.
- **MLIR** — Multi-Level Intermediate Representation. The compiler IR
  family that both `iree-import-onnx` (output) and `iree-compile` (input)
  speak. The catalog stores models as MLIR text in the `torch` dialect.
- **Bytecode** — IREE's own VM bytecode, the output of `iree-compile`.
  This is what `InferenceRuntime` loads into a session.
- **Session** — An IREE runtime concept: an instance + device + loaded
  bytecode module, ready to invoke. We hold one per worker thread.
