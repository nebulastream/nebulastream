# Model Inference

This document describes how NebulaStream evaluates ML models inside a streaming
query. It is aimed at contributors who need to extend, debug, or replace the
current implementation.

## Overview

NebulaStream relies on [OpenVINO](https://docs.openvino.ai/2025/index.html) for model inference.
Models are converted into the OpenVINO Intermediate Representation (IR) — an XML topology plus a
binary weights blob — and executed by the OpenVINO runtime on the CPU device.
For every query that contains a `MODEL_INFERENCE(...)` table-valued function,
NES converts the registered model to IR, instantiates a per-worker-thread
runtime, and invokes the model once per record.

Everything backend-specific lives in the `nes-inference` module. The logical
and physical operators (`InferModelLogicalOperator`,
`InferModelPhysicalOperator`) and the per-runtime wrapper (`InferenceRuntime`)
carry only generic concepts (imported payload, compiled payload, input/output
shapes). OpenVINO is the only backend today, but the abstractions allow for extensibility:

- `RuntimeBackend` (`nes-inference/runtime/include/RuntimeBackend.hpp`) is the
  abstract execution interface; `OpenVinoRuntimeBackend` is its only
  implementation, selected by `createRuntimeBackend()` in `InferenceRuntime.cpp`.
- `BackendTool.hpp` holds the backend-agnostic external-tool discovery
  (`$PATH` lookup, `--version` parsing, subprocess execution) used by importers.
- `Inference.hpp` exposes only `importModel` / `compileModel`; the module
  internals behind them are free to change.

## Tooling and integration boundary

One external binary is used:

- `ovc` — the OpenVINO model converter: ONNX (and other supported formats) → OpenVINO IR.

NES does not link against the converter. It is installed with
`pip install openvino` into a dedicated venv inside the development and
runtime Docker images (`docker/dependency/Development.dockerfile`,
`docker/runtime/RuntimeBase.dockerfile`); `nes-inference` discovers it on
`$PATH` and shells out to it.

The OpenVINO *runtime* library is linked into `nes-inference` (for reading the
converted IR back at import time, to scrape shapes) and into
`nes-inference-runtime` (for execution). No other target includes an OpenVINO
header.

### Installing the tools on bare metal

If you are running NES outside the provided Docker images you have to install
`ovc` yourself, with the same version the runtime checks against — see
`expectedOpenVinoVersion` in `nes-inference/src/BackendTool.hpp` (currently
`2025.3`), which must match the OpenVINO version of the vcpkg port NES links
against:

```sh
pip install openvino==2025.3.0
ovc --version  # sanity check
```

`ovc` must be reachable on the `$PATH` of every process that handles
`CREATE MODEL` — the coordinator for a deployed cluster, or the offline
`nebucli`. Workers do not need it: they receive the already-converted IR.
A version mismatch is treated as "tool unavailable": the version string is
parsed at startup, and on mismatch model import is disabled and queries that
try to register a model are rejected.

## Lifecycle

A model goes through four stages from registration to execution:

```
CREATE MODEL          parse              optimizer            lowering
─────────────►  catalog  ────►  name op  ────►  model op  ────►  physical op
   import       (OpenVINO IR)   (resolved       (executable      (per-thread
                                 schema)         payload)         runtime)
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
2. The model is converted to OpenVINO IR via `ovc`, producing an XML
   topology and a `.bin` weights blob.
3. The IR is read back through the OpenVINO runtime to scrape the
   input/output element types and tensor shapes.
4. The user-declared input/output schemas are validated against the
   tensor shape: every non-VARSIZED field must be `FLOAT32`, the field
   count must equal the tensor element count, and `VARSIZED` is only
   accepted as a single bulk-byte field that mirrors the entire tensor
   verbatim. This makes the `(model, schema)` pair an invariant carried
   downstream.

Once accepted, the catalog stores the IR — the XML as the primary payload and
the weights as the auxiliary payload, both refcounted byte buffers — keyed by
name.

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
(imported IR + validated schema).

The rule must run *before* `TypeInferenceRule`, because schema inference for the operator
depends on knowing the model's output fields, which only exist after
resolution. This ordering is enforced by `requiredBy()` in the rule.

### 4. Lowering

`LowerToPhysicalInferModel` in `nes-query-compiler` moves the model from its
imported stage to its executable stage via `compileModel(imported)` and
constructs an `InferModelPhysicalOperator` with it, the input field names, and
the output field names.

For OpenVINO there is no separate ahead-of-time compilation step: the IR is
what the runtime consumes, so `compileModel` only hands the payload to the next
lifecycle stage. Device-specific compilation happens inside OpenVINO when the
worker calls `ov::Core::compile_model` at pipeline setup, which is also why the
IR stays portable across heterogeneous workers.

### 5. Per-record execution

`InferModelPhysicalOperator` owns a `ThreadLocalRuntimeWrapper` (a
`shared_ptr` for pointer stability). At pipeline setup, the wrapper
allocates one `InferenceRuntime` per worker thread, each of which:

- Obtains an `ov::CompiledModel` for the model. The first thread to ask reads the
  IR through the process-wide `ov::Core`, reshapes it to the model's input shape
  and compiles it for the `CPU` device with `ACCURACY` execution mode and
  `LATENCY` performance hint; the result is cached process-wide, keyed by the IR
  payload and shape, so the remaining threads reuse it instead of recompiling.
- Creates its own `ov::InferRequest` from that compiled model.
- Allocates I/O byte buffers sized from the model's signature.

Per record, the operator:

1. Writes the input fields into the runtime's input buffer (one f32 slot
   per FLOAT32 field, or a `memcpy` of the whole VARSIZED blob for the
   single-field bulk-byte case).
2. Invokes the runtime, which wraps the I/O buffers as `ov::Tensor`s over the
   existing memory (no copy) and calls `infer()`.
3. Reads the output buffer back into the record.

There are no batch optimizations. Each tuple is fed in and read out
individually, even though OpenVINO supports batched inputs in principle —
this is the most obvious place to optimize next.

## Constraints and known limitations

### From the importer

- Only f32 tensor element types on both sides. Mixed precision, int8
  quantized models, fp16, etc. are rejected at registration time.
  Conversion runs with `--compress_to_fp16=False` so weights are kept at
  full precision.
- A single tensor input and a single tensor output. Multi-input or
  multi-output graphs are rejected.
- A dynamic dimension is resolved to an extent of 1 and logged as a warning.
  Inference evaluates the model once per tuple, so a dynamic batch dimension is
  exactly the shape that works, and a stock model does not have to be re-exported
  to be usable. For a FLOAT32 schema the resolved shape is still checked against
  the declared field count at registration, so a wrong resolution surfaces as an
  error there; a VARSIZED schema skips that check and trusts the resolution.
- An input tensor of rank >= 2 whose leading (batch) dimension is fixed above 1 is
  rejected: such a model wants several samples per invocation, which single-tuple
  inference cannot supply. The check is not applied to the output tensor, where a
  leading dimension above 1 is a per-sample result rather than a batch.
- Accepted inputs are the formats `ovc` handles: `.onnx`, `.pb`, `.pbtxt`,
  `.meta`, `.tflite`, `.pdmodel`, `.pt2`, or a SavedModel directory.

### From the runtime configuration

| Setting                                | Implication                                                                                                                          |
|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| device `CPU`                           | No GPU/NPU targets. The vcpkg OpenVINO port is built with the `cpu` and `ir` features only.                                           |
| `ExecutionMode::ACCURACY`              | OpenVINO will not trade numerical accuracy for speed (no implicit bf16/fp16 downcasting on capable CPUs).                             |
| `PerformanceMode::LATENCY`             | Optimizes single-request latency rather than throughput; combined with one `InferRequest` per worker thread, NES owns the parallelism. |
| shared `ov::Core` behind a mutex       | Model loading is serialized across worker threads, and compiled models are cached so each distinct model is compiled once per process. Only setup is affected — `infer()` runs unsynchronized. |

### Other operational caveats

- Conversion is not cached. Registering the same model file twice under
  different names runs `ovc` twice. Compilation *is* cached, but only within a
  process and for the process lifetime — the cache has no eviction, so a worker
  that loads many distinct models keeps every compiled model alive.
- `ovc` is version-pinned (see `expectedOpenVinoVersion`) and its
  `--version` output is checked at startup; a mismatch disables model
  import.
- Model files must be reachable by the process that handles `CREATE MODEL`,
  not by the workers. The catalog ships the converted IR, not the original
  ONNX, across the wire.

## Glossary

- **IR** — OpenVINO's Intermediate Representation: an XML file describing the
  network topology plus a `.bin` file holding the weights. This is what `ovc`
  emits and what the catalog stores and ships to workers.
- **`ovc`** — the OpenVINO model converter CLI, shipped in the `openvino`
  Python package.
- **`ov::Core`** — the OpenVINO runtime entry point: reads IR, compiles a
  model for a device, and owns the plugin registry. NES keeps one per process.
- **`ov::InferRequest`** — a single executable instance of a compiled model
  with its own state. NES holds one per worker thread.
