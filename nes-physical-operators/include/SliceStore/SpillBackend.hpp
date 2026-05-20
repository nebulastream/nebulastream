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

/// Disk-side dual of the in-memory slice store: a key/value store for spilled
/// window slices, keyed by (SliceEnd, WorkerThreadId).
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

    /// Persist the bytes of the slice ending at `sliceEnd` for `workerThreadId`.
    virtual void put(SliceEnd sliceEnd, WorkerThreadId workerThreadId, std::span<const std::byte> record) = 0;

    /// Retrieve the bytes previously persisted for (sliceEnd, workerThreadId),
    /// or std::nullopt if nothing is stored for that key.
    /// TODO(Phase 3): the owning SpillRecord return forces a copy out of the backend's
    /// buffer (e.g. RocksDB pinned slice). Revisit for a move-only / pinned-view return
    /// before the RocksDB backend lands, since this signature is its public contract.
    virtual std::optional<SpillRecord> get(SliceEnd sliceEnd, WorkerThreadId workerThreadId) = 0;

    /// Remove every per-thread record for the slice ending at `sliceEnd`.
    /// Idempotent: erasing a slice with no stored records is a well-defined no-op.
    virtual void erase(SliceEnd sliceEnd) = 0;
};

}
