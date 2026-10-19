# Checkpointing and Recovery

This document describes how a single-node worker checkpoints its queries and recovers them after a restart.
It is implementation-oriented: it covers what is persisted, when, and which consistency the design provides.

## Scope

A worker that checkpoints keeps enough state on local disk to restart all of its queries after a crash:

- the plan of every query, which recovery recompiles,
- the state of the stateful operators that support checkpointing, i.e., windowed aggregations and hash joins,
- the input that the sources emitted but that no checkpoint covers yet,
- the length of the output that file sinks wrote up to the checkpoint.

The mechanism is local to one worker. It does not coordinate with other workers, and it relies on the plan, the operators and the
binary staying the same across the restart.

## Configuration

Checkpointing is opt-in and configured per worker:

| Option | Default | Meaning |
|---|---|---|
| `worker.checkpoint.checkpoint_directory` | `/tmp/nes-checkpoints` | Directory that stores the checkpoints of the worker. |
| `worker.checkpoint.checkpoint_interval_ms` | `0` | Interval between two checkpoint rounds. `0` disables checkpointing. |
| `worker.checkpoint.recover_from_checkpoint` | `false` | Restart the queries found in the checkpoint directory when the worker starts. |

Recovered queries are compiled like any other query and therefore benefit from the worker's compilation cache
(`worker.compilation_cache`).

## Checkpoint Rounds

Each worker owns a `CheckpointManager`. Every interval, it runs a numbered checkpoint round:

1. **Prepare.** The prepare callbacks write the checkpoint of every query that takes part in the round into the query's directory of
   this round.
2. **Complete.** Only if all prepare callbacks succeeded, the manager atomically replaces `completed_round` with the round's number.
3. **Commit.** Every query that took part records the round as its own `completed_round`. Afterwards, the commit callbacks discard what
   the round covers: sources delete the persisted input up to their checkpoint, and older rounds of the query are removed.

A failing prepare callback aborts the round before it is completed, so recovery keeps using the previous round. Recovery reads the
round that the query recorded, or the round in the worker's `completed_round` if the worker stopped before the query recorded its first
round. A crash in the middle of a round therefore cannot mix the state of two rounds.

The prepare callbacks of a query share the prefix `query/<localQueryId>/`. Before the first of them runs, the query's prepare guard
acquires the exclusive checkpoint lock of all checkpointable operator handlers of the query. Every pipeline holds the shared lock of its
checkpointed handlers from processing a buffer until the buffer's task completed, which reports the buffer to its source. While the
guard is held, no pipeline modifies the state that the round captures, and the progress of the sources matches the state.
The guard tries to acquire the locks for a bounded time and fails the round if the query stays busy, rather than blocking the query.
The round includes every callback of the query that is registered once the guard is acquired.

A query takes part in a round once it is running: its sources register their callbacks before, and a round without them would miss
their progress. A query that is still starting keeps its previous round and does not hold back the rounds of the other queries.
Sources and sinks keep their callbacks after they stopped, so that every round covers them until the worker removes the query. The
guard of a query that stopped or failed captures the query without locking, and a query whose operator state cannot be checkpointed
takes part in no round.

## On-Disk Layout

```text
<checkpoint_directory>/
  completed_round                     number of the last completed round
  .recovery_snapshot/                 copy of the directory, taken when the worker starts with recovery
  query_<localQueryId>/
    completed_round                   number of the last completed round that the query took part in
    plan.pb                           serialized plan
    manifest                          format version
    failed                            marks a failed query, which recovery skips
    source_replay/
      origin_<originId>/
        pruned.meta                   sequence and byte offset up to which the input is deleted
        <persistedSequence>.bin       one file per persisted source buffer
    rounds/
      <round>/
        state/<operatorHandlerId>/hash_map_slices.bin
        source/origin_<originId>      persisted sequence up to which the round covers the source's input
        sink                          number of bytes the file sink wrote up to the round
```

The directory of a query is removed once the query stopped. The directory of a failed query stays for inspection, marked with a
`failed` file so that recovery skips it.

## Components

### Operator State

Window handlers whose slices are `HashMapSlice`s, i.e., aggregation and hash join, support checkpointing. Their lowering hands them
the hash map configs and the function that creates slices, which is the same function the build operators use.

A checkpoint writes all active slices of a handler together with their hash maps, and the end up to which the handler emitted windows
to the probe. Recovery marks these windows as emitted again, so that restoring their slices does not emit them a second time. A chained hash map is a tree of `TupleBuffer`s.
It is written as such: the payload, the number of tuples and the order of the children of each buffer. Nested state that refers to
child buffers by index, e.g., the paged vectors of the hash join or the state of a median aggregation, stays valid across the round trip.
The pointers inside the map are process-local, so deserialization re-creates the entries in insertion order, which rebuilds the chains,
and relocates the pointers of var-sized fields to the restored var-sized pages.

A recovered handler restores its slices once, in `start()`, which the probe calls during its setup before any build pipeline processes
data. A checkpoint that exists but cannot be read fails the query instead of starting from empty state, as the sources may already
have discarded the input that the checkpoint covers.

The query compiler assigns operator handler ids deterministically per query, so a recompiled query finds the state of its handlers.

Other stateful handlers, e.g., of the nested loop join, do not support checkpointing. A query with such a handler takes part in no round.
After a restart, it replays its input from the beginning and rewrites its output, which gives the same result at a higher cost.

### Sources

A source of a checkpointed query persists every buffer before it emits it. The query reports each buffer once all successors of the
source processed it, and a round covers the input up to the highest gapless buffer. Once the round completed, the source deletes the
covered buffers.

A recovered source first replays the persisted buffers after its checkpoint and then continues reading where the persisted input ends,
if the source can skip input (file sources can). The recovered source numbers its buffers from the initial sequence number again, so
that watermark processing and the input formatter see a gapless sequence. Persisted sequence numbers keep increasing across restarts.

Sources that assign their own sequence numbers, e.g., network sources, do not persist their input.

### Sinks

A file sink records the length of its output in every round. A recovered file sink truncates its output to that length and appends
the output of the recovered query, which reproduces everything that the checkpoint does not cover. Once the sink stopped, the rounds
record its final length.

### Recovery

A worker that starts with `recover_from_checkpoint` first copies its checkpoint directory into `.recovery_snapshot/`, so that recovery
reads a stable state while the recovered queries already write new rounds. If the snapshot cannot be prepared, recovery reads the live
directory instead. Recovered sources keep persisting to their live directory, which equals the snapshot until they start. The worker then recompiles and starts the query of every directory with a readable plan and manifest. Recovered queries
keep their query ids, so clients can keep polling them.

## Consistency

A completed round is a consistent cut for every query: while the guard holds the handlers' locks, the round captures the operator state
and the progress of the sources, and the sink length of the same moment. Recovery restores the state, replays the input after the cut and
truncates the output to the cut, so the recovered query produces the same results as an uninterrupted run, with these limitations:

- The progress of a source covers the buffers that its successor pipelines completed. If a buffer reaches a checkpointed handler only
  through a further pipeline, the round may cover it before the handler processed it, and it is missing after recovery.
- A window that was emitted to the probe before the round, but whose results the sink did not write yet, is missing after recovery.
- Replay assumes that every source buffer ends at a tuple boundary. Otherwise, the input formatter cannot complete a tuple that spans
  the cut.
- The checkpoint files are written without `fsync`.
- A query whose state cannot be checkpointed keeps all of its input until it stops.

## Testing

Besides unit tests for the individual components, `MultiQueryCheckpointRecoveryTest` recovers queries of an embedded worker after a
restart. Its sources read from pipes, so that the test checkpoints the queries at a known point of their input, and it checks that the
recovered output equals the output of an uninterrupted run and that unreadable checkpoints are not recovered.

The systest can test recovery end to end as well. With
`--restart-worker-every-n-tuples <n>`, it runs the worker in its own process and reads file sources with the `SystestTupleFile` source,
which emits one tuple per buffer and crashes the worker after every `n` tuples. The systest restarts the worker with recovery until all
queries terminated and compares their results with the expected ones. This mode requires a topology with a single worker and skips
queries that expect an error, as the worker process reports errors without their original code.

The crash frequency trades coverage for runtime: each restart recompiles and replays the queries, so tests with large inputs need a
higher `n`. The limitations above show up as occasional result mismatches, most often for outer joins, whose many probe tasks per window
leave windows between the probe and the sink when a round is taken.
