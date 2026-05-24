/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

/// Owning byte payload of a single spilled slice (the value side of the store).
/// Distinct from the non-owning std::span<const std::byte> accepted by put().
using SpillRecord = std::vector<std::byte>;

/// Partition index of a spilled blob within one (sliceEnd, workerThreadId) state.
/// Partition-at-spill (grace-hash) splits the per-worker map into P self-contained
/// blobs by `hash % P`; each is addressed by a distinct partition id. uint16_t
/// (P <= 65535) is ample for an edge device and keeps the RocksDB key narrow.
/// Defaults to 0 throughout, the single-partition (P==1) state, so non-partitioned
/// callers are source-compatible.
using PartitionId = std::uint16_t;

/// Disk-side dual of the in-memory slice store: a key/value store for spilled
/// window slices, keyed by (SliceEnd, WorkerThreadId, PartitionId).
///
/// A slice is uniquely identified by its end timestamp (SliceEnd = Timestamp);
/// there is no SliceId type in NebulaStream. Per-thread state is keyed by
/// WorkerThreadId (the uint32_t-backed thread identity), NOT a per-node id.
///
/// Phase 1 (skeleton): only the contract exists. The concrete RocksDB-backed
/// implementation lands in a later phase; NullSpillBackend is the no-op default.
class SpillBackend
{
public:
    SpillBackend() = default;
    virtual ~SpillBackend() = default;

    /// Polymorphic base: copy and move are deleted so the backend is only ever used
    /// through a pointer/reference (e.g. unique_ptr<SpillBackend>), preventing slicing.
    SpillBackend(const SpillBackend&) = delete;
    SpillBackend& operator=(const SpillBackend&) = delete;
    SpillBackend(SpillBackend&&) = delete;
    SpillBackend& operator=(SpillBackend&&) = delete;

    /// Persist the bytes of the slice ending at `sliceEnd` for (`workerThreadId`, `partition`).
    /// The key is (SliceEnd, WorkerThreadId, PartitionId); `partition` defaults to 0, the
    /// single-partition state used by every non-partitioned caller.
    /// TODO(future): an optional WriteBatch overload would let the P puts of one worker form a
    /// single atomic durable group (L5 future hardening); not required here.
    virtual void put(SliceEnd sliceEnd, WorkerThreadId workerThreadId, std::span<const std::byte> record, PartitionId partition = 0) = 0;

    /// Retrieve the bytes previously persisted for (sliceEnd, workerThreadId, partition),
    /// or std::nullopt if nothing is stored for that key. `partition` defaults to 0.
    /// TODO(Phase 3): the owning SpillRecord return forces a copy out of the backend's
    /// buffer (e.g. RocksDB pinned slice). Revisit for a move-only / pinned-view return
    /// before the RocksDB backend lands, since this signature is its public contract.
    virtual std::optional<SpillRecord> get(SliceEnd sliceEnd, WorkerThreadId workerThreadId, PartitionId partition = 0) = 0;

    /// Remove every per-thread record for the slice ending at `sliceEnd`.
    /// Idempotent: erasing a slice with no stored records is a well-defined no-op.
    virtual void erase(SliceEnd sliceEnd) = 0;
};

}
