# Reference query engine prototype

Configure with `-DNES_USE_REFERENCE_QUERY_ENGINE=ON` to use the Rust query manager in the single-node worker. The worker selects `ReferenceNodeEngine` from the separate `nes-reference-engine` module when this option is enabled. That type exposes the same methods used by the worker as the original `NodeEngine`; the compile-time branch in `SingleNodeWorker` selects the type without changing the original query engine or runtime. The `../reference-engine` checkout is required at configure and build time. Corrosion imports `rust/reference-adapter` as a static library; the reference engine itself is a path dependency and is not modified.

When building in Docker, mount `../reference-engine` at the same absolute path inside the container in addition to the usual NebulaStream mounts. The standard `nes-docker` wrapper mounts only the NebulaStream worktree. The opt-in configuration uses a separate online Cargo home because the development image's offline vendor set does not contain `crossbeam-channel`.

The adapter translates each `ExecutableQueryPlan` into a Rust `QueryPlan`, preserving source and pipeline edges. Every NebulaStream `TupleBuffer` is retained as an opaque Rust `DataBuffer`; fan-out clones the handle and the final drop releases the C++ buffer. C++ source callbacks feed the Rust source trait, and Rust pipeline execution invokes `ExecutablePipelineStage::execute` with a NebulaStream execution context. An executable plan may contain sharing IDs keyed by source origin ID and pipeline ID. The reference adapter forwards these IDs to the Rust manager; absent IDs leave the source or pipeline private to the plan.

This is an opt-in prototype with these limits:

- An initial `absorb` with empty state starts each C++ stage before the plan is submitted. Dropping the Rust pipeline stops the stage. Output from these lifecycle hooks is not routed.
- `PipelineExecutionContext::repeatTask` is unavailable. The adapter reports one worker thread and serializes calls to each C++ stage.
- Source output currently uses an unbounded channel. Backpressure and the configured in-flight buffer limit are not applied.
- Compiled C++ stages now export one operator state tree as a `TupleBuffer` with child buffers. The Rust manager passes this opaque buffer from `emit` to `absorb`. The physical-operator default implementation recurses into the child; `EmitPhysicalOperator` exports its active sequence/chunk bookkeeping and debug completed-sequence set. Other stateful operators and formatter state still need explicit implementations before they can be adapted safely.
- C++ stage exceptions cannot currently be propagated into the reference manager's failure event; the adapter catches them at the ABI boundary. Source errors currently end the stream.
- The adapter reports query start, running, and stop events. It does not yet reproduce the old engine's per-task or per-pipeline statistics.
- Stop is not called which is important for time based operators as they will advance the watermark

These gaps need to be resolved before this backend can replace the default engine.

## Predicate-reorder adaptation runner

Build with `-DNES_USE_REFERENCE_QUERY_ENGINE=ON`, then run `build-docker/nes-reference-engine/nes-reference-adaptation-prototype [DURATION_SECONDS]` (default: 10 seconds), or `ctest --test-dir build-docker -R '^reference-adaptation-prototype$' --output-on-failure`. The executable registers one Generator source and a Void sink, then repeatedly compiles and adapts between two SQL queries that differ only in the order of `AND` predicates. Two nested computed projections keep CSV parsing in a source input pipeline separate from the predicate pipeline. The runner attaches stable sharing IDs to the source and input pipeline in each executable plan, so they are reused across versions; their formatter state is not migrated. The remaining pipelines and the Void sink are paired by position. Void sink uses native buffers to count received tuples. Its count is exported to a state buffer and imported into each replacement sink. The runner counts state export and import events through the statistic listener, checks that the final sink's count equals the number of tuples received across all versions, and prints interval and overall tuples-per-second throughput. A shared source owns a separate backpressure controller because the sink is replaced.

This runner fixes the catalog, SQL variants, source count, and pipeline shape. Pairing pipelines by position and sharing source/input pipelines by position are valid for this narrow experiment; a general adaptation planner must derive semantic identities and compatible state mappings from the plans. The reported throughput includes source generation, CSV parsing, query processing, query compilation, adaptation, and sink execution, and is limited by the configured Generator rate of 100,000 tuples per second.
