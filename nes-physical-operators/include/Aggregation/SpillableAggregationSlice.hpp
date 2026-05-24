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

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>
#include <HashMapSlice.hpp>

namespace NES
{

/// A spillable variant of AggregationSlice: one ChainedHashMap per worker thread (indexed
/// workerThreadId % numberOfHashMaps), whose maps can be evicted to a SpillBackend and rebuilt
/// on demand. It mirrors AggregationSlice's accessors because AggregationSlice is `final` and
/// cannot be subclassed; like it, this subclasses HashMapSlice directly.
///
/// spill() serializes every resident map (Phase 2 SliceSerializer), stores it under
/// (sliceEnd, workerThreadId), and frees the in-memory map so its TupleBuffers return to the
/// BufferManager. unspill() rebuilds the maps from the backend (the on-disk copy is kept until
/// the slice is erased). Callers must only spill closed slices and must unspill before access;
/// the owning slice store (a later phase) enforces this and serialises concurrent access — this
/// class is not thread-safe on its own.
class SpillableAggregationSlice final : public HashMapSlice
{
public:
    SpillableAggregationSlice(
        SliceStart sliceStart, SliceEnd sliceEnd, const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs, uint64_t numberOfHashMaps);

    /// Returns the worker's hashmap pointer (for the Nautilus executable). Precondition: resident.
    [[nodiscard]] HashMap* getHashMapPtr(WorkerThreadId workerThreadId) const override;
    /// Returns the worker's hashmap, lazily creating it. Precondition: resident.
    [[nodiscard]] HashMap* getHashMapPtrOrCreate(WorkerThreadId workerThreadId) override;

    /// True while the maps live in memory; false after spill() until unspill().
    [[nodiscard]] bool isResident() const;

    /// Total tuples across all worker maps. While spilled, returns the count snapshotted at spill
    /// time instead of the freed maps. Overrides the virtual HashMapSlice::getNumberOfTuples, so the
    /// spill-aware count is returned even through a HashMapSlice& (e.g. the store's budget accounting).
    [[nodiscard]] uint64_t getNumberOfTuples() const override;

    /// Serializes every resident map to `backend` and frees it. No-op if already spilled.
    ///
    /// `numberOfPartitions == 1` (the default) keeps the historic single-blob path: each worker map is
    /// serialized whole and written under partition 0, byte-identical to the pre-partitioning behaviour.
    /// `numberOfPartitions > 1` grace-hash-splits each worker map by `hash % P` into P self-contained
    /// blobs (SliceSerializer::partitionedSerialize) and writes each non-empty blob under its partition
    /// id; empty partition blobs are skipped at write (SKIP-EMPTY, L3). A worker's map is only freed
    /// after all of that worker's puts succeed, mirroring the per-worker order of the P==1 path (L5).
    void spill(SpillBackend& backend, uint64_t numberOfPartitions = 1);

    /// Streaming, partition-granular read-out that LEAVES THE SLICE SPILLED (a 1/P-resident emit path,
    /// the dual of partition-at-spill). For the fixed `partition`, deserializes that partition's blob for
    /// every worker (whole, no filtering) and hands the collected per-worker maps to `emit`, which takes
    /// OWNERSHIP of them (they are freshly deserialized and NOT slice-owned, L8). The slice is not
    /// modified and `resident` stays false, so successive partitions can be streamed one at a time while
    /// only 1/P of the slice's state is materialised at once.
    ///
    /// Dependency (2d): the caller guarantees the slice is already spilled before invoking this — hence
    /// the `!resident` precondition. Workers whose partition blob is absent (skip-empty at write)
    /// contribute nothing for that partition.
    void streamEmitByPartition(
        SpillBackend& backend,
        AbstractBufferProvider& bufferProvider,
        uint64_t numberOfPartitions,
        PartitionId partition,
        const std::function<void(std::vector<std::unique_ptr<HashMap>>)>& emit) const;

    /// Rebuilds the spilled maps from `backend`, renting pages from `bufferProvider`. No-op if resident.
    ///
    /// `numberOfPartitions == 1` (the default) keeps the historic single-blob path. `numberOfPartitions
    /// > 1` merges all P partition blobs of each worker back into one map: because a group key's hash is
    /// fixed it lands in exactly one partition, so keys are disjoint across partitions and the merge is a
    /// pure insert with no per-key aggregate-combine. Absent partition blobs (skip-empty) are skipped.
    void unspill(SpillBackend& backend, AbstractBufferProvider& bufferProvider, uint64_t numberOfPartitions = 1);

private:
    /// Deserializes `partition`'s blob into `base` if present, then merges every other partition blob of
    /// `worker` into `base` by replaying its entries (keys are disjoint across partitions, so this is a
    /// pure insert). Used by unspill() for the P>1 full-window rebuild.
    void mergePartitionsInto(
        std::unique_ptr<HashMap>& base,
        SpillBackend& backend,
        AbstractBufferProvider& bufferProvider,
        WorkerThreadId worker,
        uint64_t numberOfPartitions) const;

    bool resident = true;
    /// Tuple count captured at spill() time, so getNumberOfTuples() works while non-resident.
    uint64_t spilledTupleCount = 0;
};

}
