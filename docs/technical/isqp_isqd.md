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
| `QueryPlacementAmendmentPhase` (incremental and holistic), `computeReconfigurationMarkerDeploymentUnit` | `QueryPlacementAmendmentPhase` |
| `ReconfigurationMarker`, `ReconfigurationMarkerEvent`, `{Drain,Update,UpdateAndDrain}QueryMetadata` | `ReconfigurationMarker` with `{Drain,Update,UpdateAndDrain}QueryMetadata` in nes-common |
| `NetworkSink::reconfigure` (`ConnectToNewReceiver`, marker handling), `BufferStorage` | `NetworkSink::connectToNewReceiver`, `NetworkSink::stop` with `ReconfigurationState`, buffering while the receiver is detached |
| `ISQPRequest`, `ISQPEvents` (`AddNode`, `RemoveNode`, `AddLink`, `RemoveLink`) | `IncrementalPlacementRequest`, `TopologyChangeEvent` |
| `PlacementAmendmentInstance`, `DeploymentPhase`, `updateReconfigurationMarker` | `PlacementAmendmentInstance`, `DeploymentPhase`, `DeploymentPhase::computeReconfigurationMarker` |
| `PlacementAmendmentHandler`, `PlacementAmendmentMode` (OPTIMISTIC/PESSIMISTIC), topology node locks | `PlacementAmendmentHandler`, `PlacementAmendmentMode`, `LockedWorkers`, `WorkerCatalog::tryLockWorker` |

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
or drains it as a whole. If a plan hosts both operators to be re-placed and operators that stay, the amendment phase re-places the
operators that stay as well and pins their neighbours instead (`expandToWholePlans`). Sources in such a plan stay on their worker but are
restarted in a new plan.

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

**Shortcut:** the query engine runs exactly one sink per local plan. If the operators placed on a worker would form a plan with several
outgoing edges (shared sub-plans), the addition fails and the query is redeployed holistically.

### Placement amendment phase

The phase follows the original: for every compacted change log entry it computes the plans at which the reconfiguration marker is
injected (Cond-1: plans of the pinned upstream operators; Cond-2: the first plan after a removed link, as the marker cannot travel over
it), runs the placement removal and addition, and records state migrations for stateful operators that moved. Failed entries are recorded
again and the query becomes `PartiallyProcessed`.

**Shortcut (same as the original):** only joins are considered stateful. Other stateful operators that move (window aggregations) are
drained, i.e., they emit their windows when the marker arrives instead of handing their state over.

**Deviation (holistic baseline):** the original's holistic mode re-places the operators with its placement strategies and redeploys all
plans. The port removes all plans of the query and places and decomposes the query from scratch with the initial placement.

### Reconfiguration marker

**Deviation:** the original keys the marker events by decomposed query plan id and version and serializes the marker with protobuf. The
workers of the current code base only know the ids of their local queries, so the events are keyed by local query id; the frontend
translates its decomposed query plans when it builds the marker. The marker is serialized as JSON with reflect-cpp, because it is sent both
over gRPC and in-band over the data channels, and nes-common does not depend on the generated protobuf code.

The drain events carry the outgoing channels whose receiving plan keeps running (see placement removal). The update-and-drain event does
not name a new version of the plan to start, as the state migration query is deployed separately (see state migration).

### Worker: marker handling

A marker reaches a local query either injected by the coordinator (`QueryEngine::injectReconfigurationMarker`) or over a channel (a
network source ends its stream with it). The query records it in its `ReconfigurationState`:
- **Drain / update and drain:** all sources of the query end their stream (network sources detach their receiver, so their sender keeps
  the data the query did not take). When the pipelines terminate, each network sink forwards the marker to receivers that are drained as
  well and detaches from receivers that keep running.
- **Update:** the network sinks of the query connect to their new receivers right away. If the old receiver still consumes, the sink
  sends everything to it, followed by the marker, then opens the new channel. If the old receiver detached (it was drained by an injected
  marker, Cond-2), the parked channel is redirected and the new receiver gets the kept data first.

**Deviation:** the original buffers in the network sink's `BufferStorage` after a `StartBuffering` RPC sent before the topology change.
The port buffers implicitly: a sink whose receiver detached keeps its data (in the parked sender channel and its backpressure handler)
until the update redirects it. No separate buffering RPC is needed, as the coordinator injects the marker into the plan after a removed
link, which detaches it from the upstream sender.

### Request processing and deployment

The `QueryManager` of the frontend plays the role of the coordinator: it keeps the global execution plan and the shared query plans of the
queries it started. An `IncrementalPlacementRequest` applies a batch of topology events, finds the queries affected by every removed
link, marks the operators to re-place, and runs a `PlacementAmendmentInstance` per affected query. The `DeploymentPhase` starts the new
plans, injects the reconfiguration marker at the marker units and removes the drained plans once they terminated.

**Deviation:** the original's request also batches query additions and removals. The frontend adds and removes queries with their own
statements, so a request only contains topology events.

**Fixed issue:** the original handles a removed node by pairing its upstream and downstream nodes and looking for channels between them.
The channels run through the removed node, so none are found and nothing is re-placed. The port handles a removed node like the removal of
the links from its upstream nodes; the operators on the removed node are unreachable and get re-placed. Plans on the removed node cannot
be drained and are dropped.

**Deviation:** if the incremental amendment of a query fails partially, the original records the failed change log entries and retries them
with the next request. The frontend may not send another request for the query, so the port redeploys the query holistically instead.

**Deviation:** the deployment phase waits until the drained plans terminated before it removes them from the global execution plan (with a
timeout). The original removes them right after injecting the marker.

**Deviation:** a source drained by a marker emits no final watermark, unlike a source at the end of its stream. The stream continues over
the new path; a final watermark would advance the watermark of the downstream operators to infinity, and its sequence number would collide
with the data that arrives over the new path.

### Concurrent amendments

As in the original, the affected queries of a request are amended in parallel by a pool of threads
(`IncrementalPlacementConfiguration::placementAmendmentThreadCount`; one thread processes them serially). The amendments synchronize on
the capacity of the workers:
- **Optimistic (OCC):** the placement uses a snapshot of the available slots; afterwards the slots are occupied atomically per worker. If
  a concurrent amendment took them, the addition fails and the query is redeployed holistically (see request processing).
- **Pessimistic (strict 2PL):** the removal locks the workers whose slots it releases, the addition locks all candidate workers before
  placing. Locks are acquired in a fixed order with try-lock and exponential back-off, like the original, and released after the slots are
  occupied.

**Deviation:** the original locks topology nodes, which also protects the topology structure. In the port, topology changes are applied
before the amendments start, so only the slots and the worker locks are synchronized.
