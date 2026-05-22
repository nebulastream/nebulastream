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
    [[nodiscard]] HashMap* getHashMapPtr(WorkerThreadId workerThreadId) const;
    /// Returns the worker's hashmap, lazily creating it. Precondition: resident.
    [[nodiscard]] HashMap* getHashMapPtrOrCreate(WorkerThreadId workerThreadId);

    /// True while the maps live in memory; false after spill() until unspill().
    [[nodiscard]] bool isResident() const;

    /// Total tuples across all worker maps. While spilled, returns the count snapshotted at spill
    /// time instead of the freed maps. Shadows HashMapSlice::getNumberOfTuples (non-virtual); callers
    /// holding a SpillableAggregationSlice get this spill-aware variant.
    [[nodiscard]] uint64_t getNumberOfTuples() const;

    /// Serializes every resident map to `backend` and frees it. No-op if already spilled.
    void spill(SpillBackend& backend);

    /// Rebuilds the spilled maps from `backend`, renting pages from `bufferProvider`. No-op if resident.
    void unspill(SpillBackend& backend, AbstractBufferProvider& bufferProvider);

private:
    bool resident = true;
    /// Tuple count captured at spill() time, so getNumberOfTuples() works while non-resident.
    uint64_t spilledTupleCount = 0;
};

}
