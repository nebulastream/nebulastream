**This PR adds an adaptive optimization benchmark** for NebulaStream that swaps filter orderings at runtime based on workload statistics, plus the underlying infrastructure required to make that swap zero-downtime and statistically driven.

## What changes at runtime

A query like `WHERE bid < X AND price < Y` is deployed with a workload-statistic build branch spliced *into the same source thread* as the data query. On every window-close the build branch's histogram is read back through an in-line probe, a Selection predicate decides whether the current regime favors the alternative filter ordering, and a GrpcSink ships the verdict to the coordinator. The coordinator-side callback flips a named `SwitchRegistry` gate via gRPC — no query stop, no re-deploy, no source restart; the two compiled filter chains keep running side-by-side and observe the new gate value on their next buffer.

## New infrastructure

- **Source splicing** (`nes-query-engine/RunningSource*`, `nes-query-optimizer/Traits/SpliceToRunningSourceTrait`): a build branch's source operator can be tagged so that `ExecutableQueryPlan::instantiate` redirects it to an already-running source thread for the same logical source instead of spawning a duplicate. Each successor carries its own backpressure semaphore, so a slow build branch can't stall the data path.
- **Deferred source start** (`DeferSourceStartTrait`, `RunningSourceRegistry`): when multiple build branches need to splice in before emission starts, the source waits for `expectedSpliceCount` attachments. Avoids `MultiOriginWatermarkProcessor` sequence-gap issues caused by mid-flight attachment.
- **Switchable pipeline stages** (`nes-runtime/Pipelines/SwitchableCompiledExecutablePipelineStage`, `nes-executable/SwitchRegistry`): the query compiler can wrap intermediate pipeline stages so each compiled variant is held alongside its alternates; an atomic gate (the `SwitchRegistry` slot) selects which variant runs per buffer. Exposed via a gRPC `SetSwitch` RPC.
- **Workload-domain statistic orchestration** (`nes-statistics/StatisticCoordinator::collectWorkloadStatistic`): splices the build branch (Source+Watermark+StatisticBuild+StatisticStoreWriter+Probe+Selection+Projection+GrpcSink) into the running data query's plan and registers per-probe routing callbacks. The probe runs inline with the build — no separate heartbeat query, triggers fire at the build's natural window-close cadence.
- **MemorySource looping + monotonic timestamps** (`nes-sources/MemorySource`): pre-parses a CSV dataset into native-layout `TupleBuffer`s at `setup()` and replays them N times before optionally switching to a second file, with a `monotonic_timestamp_field` option that rewrites an event-time column to stay monotonic across replays. Replaces the CPU-bound `GeneratorSource` parser path that capped benchmarks at ~330k tup/s.
- **Logical-source expansion rule** updates (`nes-query-optimizer/Rules/Semantic/LogicalSourceExpansionRule`): when two subtrees share a source-name operator, the rule now produces a single `Union(SourceDescriptors)` so one source thread fans buffers out to both consumers.
- **Per-query operator-fusing toggle** (`nes-sql-parser/StatementBinder`): SQL `SET (FALSE AS QUERY.FUSE)` clause disables operator fusing for the query, used by benchmarks to keep filter orderings observable.

## REPL changes

- `--companion-statistic` deploys a statistic alongside any `SELECT` query.
- `--companion-condition` / `--companion-condition-2` provide gated-probe predicates (with their own `--companion-target-value-N` for the regime each one favors).
- `--companion-switch-to-sql` provides the paired filter ordering — when set, the swap callback uses `SetSwitch` instead of stop-redeploy.
- Other `--companion-*` flags configure metric, window, event-time field, histogram bounds.

## Benchmarks added

Under `scripts/benchmarking/adaptive-optimization/`:

| Script | Purpose |
|---|---|
| `generate_bid_data.py` | Deterministic numpy generator for 30M-row CSV datasets matching the `bid` schema, including a regime-shifted second file for workload-cycling experiments. |
| `run_adaptive_optimization_benchmark.py` | The headline experiment: two gated probes (one per regime) flip the workload switch as the source alternates between regime A and regime B. `--fixed-variant {bid_first, price_first}` skips all companions for a clean single-query baseline using the same data path and SQRT chain. |
| `run_adaptive_optimization_benchmark.sh` | Wrapper that runs the three variants (adaptive + two fixed) into one results directory for head-to-head comparison. |
| `run_constant_workload_swap_benchmark.{py,sh}` | Stress test for the swap mechanism under a non-varying workload. |
| `run_passthrough_benchmark.{py,sh}` | Source-throughput ceiling. |
