# Reference query engine prototype

Configure with `-DNES_USE_REFERENCE_QUERY_ENGINE=ON` to use the Rust query manager in the single-node worker. The worker selects `ReferenceNodeEngine` from the separate `nes-reference-engine` module when this option is enabled. That type exposes the same methods used by the worker as the original `NodeEngine`; the compile-time branch in `SingleNodeWorker` selects the type without changing the original query engine or runtime. The `../reference-engine` checkout is required at configure and build time. Corrosion imports `rust/reference-adapter` as a static library; the reference engine itself is a path dependency and is not modified.

When building in Docker, mount `../reference-engine` at the same absolute path inside the container in addition to the usual NebulaStream mounts. The standard `nes-docker` wrapper mounts only the NebulaStream worktree. The opt-in configuration uses a separate online Cargo home because the development image's offline vendor set does not contain `crossbeam-channel`.

The adapter translates each `ExecutableQueryPlan` into a Rust `QueryPlan`, preserving source and pipeline edges. Every NebulaStream `TupleBuffer` is retained as an opaque Rust `DataBuffer`; fan-out clones the handle and the final drop releases the C++ buffer. C++ source callbacks feed the Rust source trait, and Rust pipeline execution invokes `ExecutablePipelineStage::execute` with a NebulaStream execution context. A source or pipeline has no sharing ID, so the Rust manager treats it as private to its query.

This is an opt-in prototype with these limits:

- An initial `absorb` with empty state starts each C++ stage before the plan is submitted. Dropping the Rust pipeline stops the stage. Output from these lifecycle hooks is not routed.
- `PipelineExecutionContext::repeatTask` is unavailable. The adapter reports one worker thread and serializes calls to each C++ stage.
- Source output currently uses an unbounded channel. Backpressure and the configured in-flight buffer limit are not applied.
- Later `absorb` calls leave an already started C++ stage running, and `emit` is a no-op. C++ stages do not provide state import/export methods, so state transfer during query adaptation is unsupported.
- C++ stage exceptions cannot currently be propagated into the reference manager's failure event; the adapter catches them at the ABI boundary. Source errors currently end the stream.
- The adapter reports query start, running, and stop events. It does not yet reproduce the old engine's per-task or per-pipeline statistics.
- Stop is not called which is important for time based operators as they will advance the watermark

These gaps need to be resolved before this backend can replace the default engine.
