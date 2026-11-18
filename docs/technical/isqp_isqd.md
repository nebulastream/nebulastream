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
| Network operator properties `UPSTREAM_LOGICAL_OPERATOR_ID`, `DOWNSTREAM_LOGICAL_OPERATOR_ID`, `CONNECTED_SYS_DECOMPOSED_PLAN_DETAILS` | `NetworkChannel` recorded by the `QueryDecomposer` (one per hop) |
| Operator property `PLACED_DECOMPOSED_PLAN_ID`             | Hosted operators per local plan recorded by the `QueryDecomposer`         |
| `GlobalExecutionPlan` (execution nodes with decomposed query plans) | `GlobalExecutionPlan` (decomposed query plans and channels per query) |
| `DecomposedQueryIdWithVersion`, DQP `QueryState`          | `DecomposedQueryId`, `DecomposedQueryPlanVersion`, `DecomposedQueryPlanState` |
| `findNetworkOperatorsForLink`, `findUpstreamAndDownstreamPinnedOperators` (`IncrementalPlacementUtils`) | same names in `Placement/IncrementalPlacementUtils` |
| Placement addition strategies (`BottomUpStrategy`, …) placing operators between pinned operators | `BottomUpOperatorPlacer::placeIncrementally` (the ILP-based bottom-up placement with fixed operators) |
| `PlacementRemovalStrategy`                                | `PlacementRemovalStrategy`                                                |
| `DeploymentContext`, `DeploymentUnit`, `ReconfigurationMarkerUnit` | same names in `DeploymentContext.hpp`                            |
| `BasePlacementAdditionStrategy` (`performPathSelection`, `computeDecomposedQueryPlans`, `addNetworkOperators`, `updateExecutionNodes`) | `PlacementAdditionStrategy` |

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

### Operator identity

**Deviation:** the original identifies the operators of the decomposed query plans by the operator ids of the global plan. The current
logical operators get a new id whenever they are copied with a modification (e.g., `withChildren`, `withInferredSchema`), and
`withOperatorId` is reserved for the serialization. The operators in the local plans therefore have other ids than in the global plan.
Instead, the decomposition records which operators of the global plan each local plan hosts, and the network channels between local
plans (identified by their channel id, which is part of the network source and sink configuration).

### Global execution plan

**Deviation:** the original keeps the decomposed query plans per execution node and locks execution nodes during a placement amendment.
The port keeps them per query: there are no execution nodes in the current code base, and a placement amendment only changes the plans of
its own query. The topology nodes that the original locks for its pessimistic mode are the workers of the `WorkerCatalog`.
Each decomposed query plan remembers the id of the local query that runs it on its worker.

### Pinned operators

The computation follows the original: the upstream operator of every channel using a removed link stays pinned. If the worker of its
downstream operator is still reachable from the worker of the upstream operator, the downstream operator stays pinned as well.
Otherwise, a breadth-first search over the downstream operators pins the closest operators on reachable workers; the operators in between
are re-placed and their other upstream operators are pinned.

**Fixed issue:** the original repeats the reachability computation in a busy loop until it acquires read locks on all topology nodes.
The port computes reachability on a snapshot of the topology, so no locks and no retries are needed.

**Shortcut (same as the original):** the upstream operators are assumed to keep running, which only holds as long as only leaf workers
change their parent.

### Placement removal

The removal processes the operators between the pinned operators of a change log entry. Operators to be re-placed release their slots and
become `ToBePlaced`; operators to be removed become `Removed`. Channel chains from or to a re-placed operator, and chains using a removed
link, are removed. Their forwarding plans and the plans hosting re-placed operators are drained (`MarkedForMigration`).

**Deviation:** the original removes operators from the plans and redeploys the remaining plan as a new version. The current query
engine cannot change a running plan, so the port keeps a plan, updates only the receivers of its network sinks (see placement addition),
or drains it as a whole. A plan that hosts both re-placed operators and operators that stay cannot be reconfigured incrementally.

**Deviation:** a drained plan detaches from receiving plans that keep running instead of forwarding the reconfiguration marker to them
(`DeploymentContext::detachedChannels`). The receiving plan keeps its network source and a new sender attaches to it. In the original,
the downstream plan of a removed chain is replaced by a new plan with a new id, which (a) loses the state of the stateful operators in that
plan and (b) never terminates if the plan has inputs that are not affected by the change, as no marker arrives on them.

### Placement addition

The addition follows the steps of the original's `BasePlacementAdditionStrategy`: path selection, placement of the operators to be placed,
computation of the new decomposed query plans (connected operators on the same worker), insertion of network operators along topology
paths (with forwarding plans on intermediate workers), and validation of the occupied slots.

**Deviation (path selection):** the original selects one path per pinned upstream worker before placing. The port passes all workers on
paths between the pinned workers to the ILP-based bottom-up placement, the only placement strategy of the current code base, and lets it
choose.

**Deviation (pinned plans):** the original merges the new operators into the plans of the pinned operators and redeploys those plans. The
port keeps the plans of pinned operators running:
- a pinned upstream operator's plan is updated in place: its network sink connects to the first hop of the new chain
  (`MarkedForRedeployment` with a `NetworkSinkUpdate`, marker action Update);
- a pinned downstream operator's plan does not change: the last hop of the new chain reuses the channel of the removed chain, so the new
  sender attaches to the existing network source;
- a pinned and a placed operator on the same worker are connected by a channel on that worker.
