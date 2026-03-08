# The Problem

NebulaStream now accepts the paper-style replay SQL surface, but the runtime still implements only a thin recording abstraction.
The control plane tracks recordings as static entries with identifiers, file paths, structural fingerprints, requested retention, representations, and owner queries in `nes-query-optimizer/include/RecordingCatalog.hpp`.
The worker runtime reports only aggregate replay metrics such as total recording bytes and file count in `nes-single-node-worker/interface/WorkerStatus.hpp`.
The physical store appends raw rows or compressed segments, but the segment header contains only decoded and encoded sizes in `nes-common/include/Replay/BinaryStoreFormat.hpp` and `nes-physical-operators/src/StoreOperatorHandler.cpp`.
The replay source reads store files sequentially without a manifest, segment index, or time-based pruning in `nes-sources/src/BinaryStoreSource.cpp`.

This leaves four concrete gaps.

- (**P1**) The system models retention only as optimizer metadata and cost estimation.
  The runtime does not enforce a bounded retention horizon, so stores do not provide true rolling-window behavior.
- (**P2**) The system has no lifecycle state for recordings.
  The coordinator cannot distinguish between a newly installed recording, a recording that is still filling, a ready recording, and a draining recording.
- (**P3**) Replay-aware redeployment is destructive.
  `QueryManager` persists selected recordings immediately and updates the global `TIME_TRAVEL_READ` alias eagerly, while `StatementHandler` redeploys active replay queries by stopping, unregistering, registering, and restarting them.
- (**P4**) The system cannot protect in-flight replays during retention changes or redeployments.
  The file format has no segment-level metadata or pin counters, and the coordinator has no epoch or successor model.

# Goals

- (**G1**) We implement physical rolling-window retention for stores to address **P1**.
  A configured replay retention window must correspond to actual retained data on the worker, not only to optimizer estimates.
- (**G2**) We introduce explicit recording lifecycle state to address **P2**.
  The coordinator and workers must be able to represent at least `NEW`, `INSTALLING`, `FILLING`, `READY`, `DRAINING`, and `DELETED`.
- (**G3**) We replace destructive replay redeployments with staged epoch rollout to address **P3**.
  The system must install a new recording epoch, wait until it is ready, activate it, and only then retire the old epoch.
- (**G4**) We add replay-safe pinning and garbage collection to address **P4**.
  A replay must be able to pin the concrete data it reads so that rolling-window eviction and redeployments cannot invalidate it.
- (**G5**) We expose enough worker-visible metadata to reason about readiness and retained history.
  The coordinator must observe per-recording state, retained range, storage usage, and successor relationships.

# Non-Goals

- (**NG1**) This document does not redesign the replay-read SQL syntax.
  The current read path still uses the existing alias-based mechanism, and this document focuses on runtime lifecycle and storage behavior.
- (**NG2**) This document does not require a new payload encoding for recorded tuples.
  We can keep the existing raw-row and compressed-segment payloads and add lifecycle metadata beside them.
- (**NG3**) This document does not attempt to support arbitrary historical indexing in the first phase.
  The initial target is bounded retention and safe replay over the retained horizon.
- (**NG4**) This document does not address unrelated optimizer changes outside replay recording selection and lifecycle integration.

# Alternatives

## A1: Keep append-only stores and treat retention as a logical promise

This alternative keeps the current store format and coordinator behavior.
The optimizer continues to reason about retention windows, but the worker never removes expired data.

Advantages:

- It preserves the current simple file layout.
- It minimizes implementation effort in the short term.

Disadvantages:

- It does not satisfy **G1** because no worker enforces a rolling window.
- It does not satisfy **G4** because replay safety is still based on a global alias instead of pinned data.
- It lets storage usage diverge from the configured retention horizon.

We reject `A1` because it preserves the core mismatch between the paper model and the runtime.

## A2: Implement rolling windows by truncating or rewriting a single store file

This alternative keeps one file per recording and removes expired data by rewriting the file or truncating the head.

Advantages:

- It keeps the external artifact model simple.
- It could avoid introducing multiple files per recording in the first iteration.

Disadvantages:

- It complicates concurrent replay because readers and writers contend on the same mutable file.
- It makes pinning expensive because the system would need copy-out or rewrite barriers.
- It does not naturally support readiness metadata, successor links, or segment-level garbage collection.

We reject `A2` because it makes safe replay and lifecycle transitions harder than necessary.

## A3: Treat recordings as segmented artifacts with explicit manifests and lifecycle metadata

This alternative keeps the current segmented payload idea, but promotes segments to managed artifacts.
Each recording stores segment metadata, retained-range metadata, lifecycle state, and successor information.
The worker evicts expired segments that are not pinned, and the coordinator activates new epochs only after they are ready.

Advantages:

- It satisfies **G1** through explicit segment-level rolling retention.
- It satisfies **G2** and **G3** by giving the coordinator concrete lifecycle state and readiness signals.
- It satisfies **G4** because pinning becomes a property of concrete segments, not of a global alias.

Disadvantages:

- It introduces new manifest and status plumbing across worker and coordinator code.
- It requires event-time or watermark propagation into the recording path.

We choose `A3` because it is the only option that aligns runtime behavior with the lifecycle model from the paper.

# Solution Background

The replay paper's recording lifecycle section assumes that a recording is a managed artifact with lifecycle states, a fill watermark, segment metadata, and successor relationships.
The current code already has two useful building blocks.
First, the optimizer already attaches replay requirements through `ReplaySpecification` and chooses between create, reuse, and upgrade decisions.
Second, the store path already supports segmented payloads for compressed recordings.
The proposed solution extends these two building blocks into a runtime model that the coordinator and workers can manage explicitly.

# Our Proposed Solution

We represent each recording as an independently managed artifact with explicit lifecycle metadata and a bounded retained history.
The worker remains the source of truth for segment-level retention and readiness.
The coordinator remains the source of truth for query ownership, epoch activation, and cross-recording transitions.

## Recording Metadata Model

We extend `RecordingEntry` in `RecordingCatalog` with lifecycle state, epoch, successor recording identifier, fill watermark, and a retained-range summary.
We extend `ReplayableQueryMetadata` with an active epoch and an optional pending epoch.
We stop deleting recordings immediately when the last owner disappears.
Instead, the coordinator transitions them to `DRAINING` and deletes them only after worker-side garbage collection has removed all unpinned retained data.

## Worker Manifest and Segment Metadata

We keep the existing payload file format for recorded bytes.
We add a manifest or sidecar index per recording that tracks segment identifiers, byte offsets, sizes, event-time lower and upper bounds, and pin counts.
The worker appends new segments to the payload and appends segment metadata to the manifest in the same lifecycle operation.
The worker computes a recording `fillWatermark` from segment continuity and stores it in the manifest summary.

## Rolling-Window Support for Stores

Rolling-window support becomes a worker responsibility, not only an optimizer estimate.
Each recording receives an effective retention window in milliseconds.
The worker evaluates every segment against that retention window using event-time bounds.
If a segment falls completely outside the retained horizon and no replay has pinned it, the worker marks it evictable and removes it during garbage collection.
If a segment is still pinned, the worker defers eviction until the replay releases it.

This is the first implementation phase because the rest of the lifecycle depends on it.
Without segment-level time bounds, the system cannot tell when a recording has retained enough history to become `READY`.

## Worker Status and Coordinator Visibility

We add per-recording runtime status to the worker status surface.
Each status entry reports the recording identifier, lifecycle state, configured retention, retained range, fill watermark, segment count, storage bytes, and successor identifier.
The existing aggregate replay metrics remain available as derived summaries.
The coordinator polls this status and mirrors it into `RecordingCatalog`.

## Lifecycle Manager

We introduce a coordinator-side `RecordingLifecycleManager`.
This component drives the state machine `NEW -> INSTALLING -> FILLING -> READY -> DRAINING -> DELETED`.
It owns alias updates, activation decisions, and garbage-collection eligibility instead of letting `QueryManager::registerQuery()` update `TIME_TRAVEL_READ` eagerly.
It also decides when a pending epoch can replace the active epoch.

## Epoch-Based Redeployment

We replace stop-unregister-register-restart replay redeployments with staged epoch rollout.
The coordinator installs a new recording epoch alongside the active one.
The worker starts filling the new recording while the previous epoch remains active.
Once the new epoch reports `READY`, the coordinator activates it and marks the old epoch `DRAINING`.
The old epoch remains available until no replay pins it and no owner requires it.

## Replay Pinning

We add pin and unpin operations on concrete recordings and segments.
A replay request pins the segments it will read before the worker begins serving data.
The worker refuses to garbage-collect pinned segments.
This pinning model is also the bridge from the current alias-based replay source to a future direct recording-targeted replay API.

## Implementation Order

We implement the solution in the following order.

1. Add event-time or watermark propagation into the recording path.
2. Add worker manifest files and segment metadata.
3. Add worker-side rolling-window garbage collection.
4. Add per-recording worker status and extend `RecordingCatalog`.
5. Add the coordinator `RecordingLifecycleManager`.
6. Replace destructive replay redeployment with epoch-based rollout.
7. Add replay pinning and direct replay targeting.

# Proof of Concept

The current segmented payload path provides a low-risk proof-of-concept entry point.
We can first extend compressed recordings with a sidecar manifest while leaving tuple payload encoding unchanged.
The proof of concept should demonstrate three properties.

- A worker can compute a retained range and fill watermark from appended segments.
- A worker can evict expired unpinned segments and report reduced retained history.
- The coordinator can keep an old recording epoch active until a new one reports `READY`.

# Summary

The current implementation already supports replay-aware planning, recording selection, and a paper-aligned SQL surface, but it does not yet implement the paper's recording lifecycle semantics.
The main missing piece is that retention remains logical while the store runtime remains append-only.
We therefore propose to treat recordings as managed segmented artifacts with manifests, lifecycle state, rolling-window garbage collection, epoch rollout, and replay pinning.
This solution addresses **P1** through **P4** and gives the system a concrete path from the current implementation to the paper model.

# Open Questions

- Which event-time signal should the worker use to determine segment bounds and readiness for operators that currently expose only tuple payloads.
- Should the worker write one payload file per recording with a sidecar manifest, or one payload file per segment with a directory-level manifest.
- How much of the current `TIME_TRAVEL_READ` alias flow should remain during the transition toward direct recording-targeted replay reads.
- Which component should own background garbage-collection scheduling on the worker.

# Sources and Further Reading

- `Replay Paper.pdf` in the repository root workspace.
- `nes-query-optimizer/include/RecordingCatalog.hpp`
- `nes-frontend/src/QueryManager/QueryManager.cpp`
- `nes-frontend/src/Statements/StatementHandler.cpp`
- `nes-common/include/Replay/BinaryStoreFormat.hpp`
- `nes-physical-operators/src/StoreOperatorHandler.cpp`
- `nes-sources/src/BinaryStoreSource.cpp`
- `nes-single-node-worker/interface/WorkerStatus.hpp`
- `nes-common/src/ReplayStorage.cpp`
