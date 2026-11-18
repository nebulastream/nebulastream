# Incremental Stream Query Placement (ISQP) and Deployment (ISQD)

This document describes the port of ISQP and ISQD onto the current NebulaStream code base.
The original implementation is a research prototype on top of a heavily outdated NebulaStream version (coordinator, `GlobalExecutionPlan`,
ZMQ network stack, `nes-runtime` reconfiguration messages):

- ISQP: *Incremental Stream Query Placement in Massively Distributed and Volatile Infrastructures* (ICDE 2025).
- ISQD: *Incremental Stream Query Deployment under Continuous Infrastructure Changes in the Cloud-Edge Continuum* (PVLDB 19(2), 2025).

The port stays as close as possible to the original ideas and code structure.
Where the current code base forces a different solution, or where the original has a logical issue, this document says so
in the sections **Deviation** and **Fixed issue**.
Shortcuts that the original takes and that the port takes as well are listed under **Shortcut**.

## Core ideas

**ISQP** re-places only the operators that are invalidated by a batch of topology changes:

1. Topology change events (node added/removed, link added/removed) and query events (added/removed) are processed as a batch.
2. For every removed link, the affected queries and the *pinned* upstream and downstream operators are computed.
   Upstream operators that still run and downstream operators that are still reachable stay where they are.
   Only the operators between them are re-placed.
3. The re-placement is recorded in a change log per query. Overlapping change log entries are compacted.
4. The placement amendment removes the invalid placements and places the operators again between the pinned operators.
5. Affected queries are processed in parallel. Concurrent amendments synchronize on the worker capacities either optimistically (OCC)
   or pessimistically (strict 2PL on the topology nodes of the selected path).

**ISQD** deploys the result of the amendment incrementally:

1. The deployment context computer gives every affected decomposed query plan (DQP) a state: to be deployed, to be updated, to be
   drained, or to be updated and drained (state migration).
2. A stateful operator that moves gets an ad-hoc state migration query: a source on the old worker reads the operator state, the regular
   network channels carry it to the new worker, where it initializes the operator.
3. New DQPs are deployed first. Then a reconfiguration marker, a map from `(DQP, version)` to an action, is injected at the affected
   upstream DQPs and flows downstream through the data channels like a punctuation:
   - **Drain**: flush the in-flight tuples, then terminate.
   - **Update**: switch to the new version of the DQP (the network sinks connect to their new receivers).
   - **Update and drain**: hand the state of the stateful operators over to the migration query, then terminate.
4. Upstream DQPs that lost their downstream connection buffer their output and replay it once they are reconnected.

## Mapping to the current code base

| Original                                                  | Port                                                                      |
|-----------------------------------------------------------|---------------------------------------------------------------------------|
| `Topology::addTopologyNodeAsChild`, `removeTopologyNodeAsChild`, `findAllDownstreamNodes` | `NetworkTopology::addLink`, `removeLink`, `findAllDownstreamNodes`, mirrored in `WorkerCatalog` |
| `TopologyNode::occupySlots`, `releaseSlots`               | `WorkerCatalog::tryOccupySlots`, `releaseSlots`, `getAvailableSlots`      |
| `SharedQueryPlan` (one per merged query)                 | `SharedQueryPlan` (one per distributed query, no query merging)            |
| Operator properties `PINNED_WORKER_ID`, operator state    | `SharedQueryPlan::getPinnedWorker`, `getOperatorState`                     |
| Parent links of mutable operator nodes                    | `OperatorGraph` over the immutable logical operators                      |
| `ChangeLog`, `ChangeLogEntry`                             | `ChangeLog`, `ChangeLogEntry`                                             |

### Worker capacity

The original occupies the slots of a topology node for every placed operator, both for the holistic and the incremental placement.

**Deviation:** the initial (holistic) placement keeps the current semantics, where the capacity of a worker limits the operators of a
single query (`BottomUpOperatorPlacer`). The systests rely on this, as they run many queries concurrently on capacity-limited workers.
The slots of the initial placement are still accounted for (`WorkerCatalog::occupySlots`, unchecked), but only the placement amendment
of ISQP enforces the remaining capacity (`tryOccupySlots`). The remaining capacity never drops below zero.

### Shared query plan and change log

**Deviation:** the current code base does not merge queries, so every distributed query has its own shared query plan and the shared
query id is the distributed query id. Queries placed by the initial placement start as `Deployed` with all operators `Placed`.

**Fixed issue:** the original keys change log entries by the wall-clock time in microseconds, so two entries recorded in the same
microsecond overwrite each other. The port uses a logical timestamp.

**Fixed issue:** when compacting two entries, the original merges their pinned operators by erasing from both sets while iterating them
in the order of their pointer addresses. For chains of three or more operators, the result depends on the memory layout. The port drops
an operator iff an operator of the other entry is more upstream (for the upstream operators) or more downstream (for the downstream
operators).
