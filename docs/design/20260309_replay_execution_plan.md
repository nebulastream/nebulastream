# The Problem
NebulaStream already implements part of the replay paper's lifecycle foundation, but it does not yet implement the replay-execution model described in Section 6 of `Replay Paper.pdf`.
The current code already supports replay-aware recording selection, worker-side recording manifests, retention garbage collection, recording lifecycle state, and staged recording-epoch rollout.
The worker-side storage substrate exists in `nes-common/src/ReplayStorage.cpp`, `nes-physical-operators/src/StoreOperatorHandler.cpp`, and `nes-sources/src/BinaryStoreSource.cpp`.
The coordinator-side lifecycle substrate exists in `nes-query-optimizer/include/RecordingCatalog.hpp`, `nes-frontend/src/QueryManager/RecordingLifecycleManager.cpp`, and `nes-frontend/src/QueryManager/QueryManager.cpp`.
The remaining gap is that replay execution still uses a global `TIME_TRAVEL_READ` alias instead of a replay-specific plan over concrete maintained recordings.

- (**P1**) The system has no first-class replay request surface.
  The SQL parser supports `REPLAYABLE ... WITH HISTORY ... AND REPLAY LATENCY ...`, but it does not expose a first-class `REPLAY <Query> ...` execution statement.
- (**P2**) The current replay read path is alias-based instead of plan-based.
  `TimeTravelReadResolver` rewrites `TIME_TRAVEL_READ` to a single concrete file path behind the global alias instead of selecting concrete recordings for a specific replay request.
- (**P3**) The replay source is not interval-aware.
  `BinaryStoreSource` opens one file, pins all manifest segments, and scans from the beginning of the file.
  It does not accept replay interval bounds, selected segment identifiers, or warm-up versus emit boundaries.
- (**P4**) The runtime has no replay execution protocol.
  The paper requires a `PREPARE -> FAST_FORWARD -> EMIT -> CLEANUP` replay state machine, but the current runtime only runs normal query execution.
- (**P5**) The runtime has no checkpoint-based replay state restoration.
  The paper relies on restoring stateful operators from a checkpoint before replaying the warm-up interval, but the current codebase does not expose replay-oriented checkpoint selection or restore hooks.
- (**P6**) The coordinator does not persist enough replay-planning metadata.
  `ReplayableQueryMetadata` stores the original plan, optimized plan, replay specification, and selected recording identifiers, but it does not store enough replay-boundary metadata to derive a replay suffix plan directly.
- (**P7**) Event-time replay semantics are not fully specified by the current source format.
  The current manifest stores segment watermark ranges, which is enough for readiness and retention, but not obviously enough to guarantee interval-exact replay semantics for all plans.

# Goals
- (**G1**) We add a first-class replay request path that addresses **P1** and **P2**.
  A replay request must target a specific replayable query and a bounded historical interval without depending on a global alias.
- (**G2**) We add replay planning over concrete maintained recordings to address **P2** and **P6**.
  The coordinator must derive a replay plan by choosing usable recordings that satisfy retention, readiness, and structural compatibility for the requested interval.
- (**G3**) We make the replay source interval-aware to address **P3**.
  The worker must be able to pin and scan only the selected recording segments that cover the replay request.
- (**G4**) We implement the paper's replay execution protocol to address **P4**.
  Replay execution must have explicit preparation, warm-up, emit, and cleanup phases.
- (**G5**) We deliver a practical first version before checkpoint restoration to address **P4** and stage **P5**.
  The first implementation should support replay for plans that can be warmed up by scanning retained history, while clearly rejecting unsupported replay requests.
- (**G6**) We define the missing event-time contract to address **P7**.
  Replay execution must have an explicit rule for how event time and output gating are reconstructed during warm-up and emit.

# Non-Goals
- (**NG1**) This document does not redesign the already implemented recording selection algorithm for `REPLAYABLE` query registration.
  The focus here is replay execution after recordings already exist.
- (**NG2**) This document does not replace the existing lifecycle and retention work.
  The plan assumes the current manifest, retention, lifecycle, and epoch-rollout implementation as a base.
- (**NG3**) This document does not promise arbitrary ad-hoc retroaction replay in the first implementation phase.
  The first target is replay for already registered replayable queries.
- (**NG4**) This document does not require immediate implementation of checkpoint-based recovery.
  We plan that capability explicitly as a later phase.

# Alternatives
## A1: Keep `TIME_TRAVEL_READ` as the replay interface and improve it incrementally

This alternative keeps replay execution centered on the current alias-based source resolution.
We would only add a few source parameters and keep the rest of the execution path unchanged.

Advantages:

- It minimizes short-term parser and coordinator work.
- It reuses the current `TimeTravelReadResolver` and `BinaryStoreSource` flow.

Disadvantages:

- It does not satisfy **G1** because replay requests would still depend on a global alias.
- It does not satisfy **G2** because the coordinator still would not plan replay against concrete maintained recordings for a specific request.
- It makes concurrent replays harder because the alias is global instead of replay-specific.

We reject `A1` because it preserves the main mismatch between the paper's execution model and the current implementation.

## A2: Add direct per-recording replay reads, but no replay planner

This alternative lets users or internal code target a concrete recording identifier directly.
The coordinator would not derive replay plans from the optimized replayable query graph.

Advantages:

- It removes the global alias from the hot path.
- It is simpler than a full replay planner.

Disadvantages:

- It does not satisfy **G2** because it pushes replay-boundary selection out of the optimizer and coordinator.
- It does not solve replay for queries that require multiple maintained recordings or suffix recomputation.
- It makes correctness depend on external selection logic that the current system does not expose.

We reject `A2` because it only solves file targeting, not replay execution.

## A3: Implement replay as a first-class coordinator workflow over maintained recordings

This alternative treats replay execution as its own planning and execution path.
The coordinator derives a replay plan from replayable-query metadata and maintained recordings, and the worker executes replay sources over interval-specific pinned segments.

Advantages:

- It satisfies **G1** through **G6**.
- It aligns with the paper's replay planner and replay execution protocol.
- It builds directly on the lifecycle and segmented-storage work already present in the repository.

Disadvantages:

- It requires new parser, coordinator, and source/runtime code paths.
- It requires an explicit staged plan for checkpoint restoration and event-time handling.

We choose `A3` because it is the only alternative that closes the actual replay-execution gap.

# Solution Background
The repository already implements several prerequisites for the replay paper's execution section.
`StoreOperatorHandler` writes segment metadata with watermark bounds and retention metadata.
`ReplayStorage` persists and mutates recording manifests, retention summaries, lifecycle summaries, and pin counts.
`RecordingCatalog`, `RecordingLifecycleManager`, and `QueryManager` already mirror worker runtime status into coordinator metadata and already stage recording epochs for active replayable queries.
This existing work means the next step is not more lifecycle plumbing.
The next step is to turn that substrate into a replay planner and replay execution protocol.

# Our Proposed Solution
We implement replay execution in phases.
Each phase is independently testable and builds directly on the code that already exists.

## Phase 1: First-class replay request model

We add a replay request surface in the parser and frontend.
The initial request shape should target a replayable query identifier and a bounded historical interval.
The frontend creates a `ReplayExecution` object that tracks the request, selected recordings, pinned segments, internal query identifiers, and execution state.

This phase addresses **G1**.

## Phase 2: Persist replay-planning metadata at registration time

We extend coordinator metadata so replayable queries retain enough information to derive replay suffix plans later.
The current `ReplayableQueryMetadata` should be extended with replay-boundary metadata derived from `RecordingSelectionResult`.
At minimum, we need to persist the selected recording boundaries and the active-query rewrite information that identifies which cut edges were materialized.

This phase addresses **G2**.

## Phase 3: Replay planner over maintained recordings

We add a replay planner in the frontend or optimizer layer.
For a replay request, the planner computes usable recordings by checking:

- exact structural compatibility with the required replay boundary,
- lifecycle state `READY`,
- retained-range coverage for the requested interval,
- fill watermark coverage for the replay end,
- and worker-visible availability of the concrete recording file.

The first implementation should support replay plans that read one or more maintained recordings and re-execute only the downstream suffix.
The planner should reject requests that require unsupported state restoration or unsupported replay boundaries.

This phase addresses **G2** and **G5**.

## Phase 4: Interval-aware replay source and selective pinning

We extend `BinaryStoreSource` and the replay-storage helpers so a replay source can:

- open a concrete recording without using the global alias,
- pin only the manifest segments selected for the replay request,
- seek to the first selected segment,
- stop after the replay end segment,
- and expose replay-specific scan metrics.

The source config should accept replay parameters such as concrete `recording_id`, `segment_ids`, `scan_start_ms`, `scan_end_ms`, and replay mode.
The storage helpers should gain a segment-selection API instead of only `pinBinaryStoreSegments(filePath)` for the whole manifest.

This phase addresses **G3** and is the main worker-side prerequisite for the execution protocol.

## Phase 5: Replay execution protocol without checkpoints

We implement the first end-to-end replay execution state machine in the coordinator and runtime:

1. `PREPARE`.
   The coordinator validates the replay request, builds the replay plan, pins the selected recording segments, and registers an internal replay query.
2. `FAST_FORWARD`.
   The replay query scans from the warm-up start and suppresses outputs before the replay start boundary.
3. `EMIT`.
   The replay query emits outputs for the requested interval and stops after the replay end boundary.
4. `CLEANUP`.
   The coordinator tears down replay-only execution state and unpins the selected segments.

The first version should explicitly support only replay plans whose state can be reconstructed by replaying the retained warm-up interval.
Unsupported plans must fail with a precise error instead of silently producing partial results.

This phase addresses **G4** and **G5**.

## Phase 6: Event-time contract and output gating

We define how replay reconstructs event-time behavior.
The first implementation should use the same event-time semantics as the serving plan whenever the replay suffix already contains a watermark assigner derived from payload fields.
If that guarantee is not available at a replay boundary, we must either inject a compatible watermark assigner during replay planning or reject the replay request.

We also add explicit replay output gating based on replay start and replay end watermarks.
This avoids emitting outputs that belong to warm-up only.

This phase addresses **G6**.

## Phase 7: Checkpoint-aware replay restoration

After the non-checkpoint replay path works, we add checkpoint-aware restoration for stateful suffixes.
The replay planner selects the newest compatible checkpoint at or before the replay start.
The runtime restores operator state from that checkpoint, then fast-forwards only the remaining warm-up interval before emit begins.

This phase closes the remaining gap to the paper's Section 6 protocol.

## Transitional compatibility

We keep `TIME_TRAVEL_READ` during the migration as a compatibility path for existing tests and simple store-read workflows.
We should treat it as a legacy replay shim rather than as the final replay execution interface.
Once the first-class replay path exists, `TIME_TRAVEL_READ` can resolve by internally creating a direct recording-targeted replay source instead of going through a mutable global alias.

# Implementation Order
We should implement the work in the following order.

1. Add `ReplayStatement` parsing and frontend statement handling.
2. Extend replayable-query metadata with replay-boundary information needed for replay planning.
3. Add replay planner feasibility checks for maintained recordings.
4. Add interval-aware segment selection and selective pinning APIs in `ReplayStorage`.
5. Extend `BinaryStoreSource` with replay-specific scan configuration and bounded scanning.
6. Add coordinator-side `ReplayExecutionManager` and the `PREPARE -> FAST_FORWARD -> EMIT -> CLEANUP` state machine.
7. Add rejection paths for unsupported replay plans that would require missing checkpoint support.
8. Define and implement the event-time replay contract.
9. Add checkpoint-aware replay restoration as a follow-up phase.

# Test Plan
We should add tests in the same order as the implementation.

1. Unit tests for replay-planner feasibility checks over maintained recordings.
2. Unit tests for segment selection, selective pinning, and unpinning in `ReplayStorage`.
3. Unit tests for bounded `BinaryStoreSource` scans over selected segments.
4. Frontend tests for replay request registration, failure handling, and execution-state transitions.
5. Integration tests for replay during recording-epoch rollout.
6. Integration tests for replay while retention GC runs on expired but pinned segments.
7. Follow-up tests for checkpoint-based restore once Phase 7 lands.

# Proof of Concept
The existing implementation already provides a useful proof-of-concept base.
The repository can already select recording boundaries, create and upgrade recordings, derive readiness from retained history, pin retained segments, and stage new recording epochs without destroying the active epoch immediately.
The first replay-execution proof of concept should therefore aim to demonstrate one narrower claim.
The system should be able to replay a registered replayable query over a bounded interval by selecting `READY` recordings, pinning only the needed segments, fast-forwarding through a warm-up interval, and emitting outputs only for the requested interval.

# Summary
The replay paper's execution section expects replay-specific planning and execution over maintained recordings.
The current NebulaStream codebase already implements much of the storage and lifecycle substrate needed for that model, but replay execution itself still depends on a global alias and a whole-file replay source.
We therefore propose a phased implementation that starts with first-class replay requests, replay-boundary metadata, and interval-aware replay sources, then adds a replay execution protocol, and finally adds checkpoint-aware restoration.
This plan reuses the storage and lifecycle work that is already present and closes the remaining gap between the paper's replay-execution model and the current implementation.

# Open Questions
- Which exact replay request syntax should we expose first for registered replayable queries.
- Whether replay planning should live in the frontend, in the optimizer, or in a dedicated replay-planning component that depends on both.
- Whether segment-level watermark metadata is sufficient for all replay boundaries, or whether we also need tuple-level event-time metadata in the store format.
- Which operators should be considered replayable in Phase 5 without checkpoint support.
- Whether `TIME_TRAVEL_READ` should remain externally visible after first-class replay lands or become an internal compatibility mechanism only.

# Sources and Further Reading
- `Replay Paper.pdf`
- `docs/design/20260308_replay_recording_lifecycle.md`
- `nes-common/src/ReplayStorage.cpp`
- `nes-frontend/src/Statements/TimeTravelReadResolver.cpp`
- `nes-frontend/src/QueryManager/RecordingLifecycleManager.cpp`
- `nes-frontend/src/QueryManager/QueryManager.cpp`
- `nes-query-optimizer/include/RecordingCatalog.hpp`
- `nes-sources/src/BinaryStoreSource.cpp`
- `nes-physical-operators/src/StoreOperatorHandler.cpp`
