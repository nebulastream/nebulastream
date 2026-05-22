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

#include <Aggregation/SpillableAggregationSlice.hpp>

#include <cstdint>
#include <limits>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceSerializer.hpp>
#include <SliceStore/SpillBackend.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>

namespace NES
{

namespace
{
/// Entry stride of a ChainedHashMap built from these args (matches the constructor's computation).
uint64_t entrySizeFor(const CreateNewHashMapSliceArgs& args)
{
    return sizeof(ChainedHashMapEntry) + args.keySize + args.valueSize;
}
}

SpillableAggregationSlice::SpillableAggregationSlice(
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    const CreateNewHashMapSliceArgs& createNewHashMapSliceArgs,
    const uint64_t numberOfHashMaps)
    : HashMapSlice(sliceStart, sliceEnd, createNewHashMapSliceArgs, numberOfHashMaps, 1)
{
}

HashMap* SpillableAggregationSlice::getHashMapPtr(const WorkerThreadId workerThreadId) const
{
    INVARIANT(resident, "getHashMapPtr called on a spilled slice; unspill() first");
    const auto pos = workerThreadId % hashMaps.size();
    return hashMaps[pos].get();
}

HashMap* SpillableAggregationSlice::getHashMapPtrOrCreate(const WorkerThreadId workerThreadId)
{
    INVARIANT(resident, "getHashMapPtrOrCreate called on a spilled slice; unspill() first");
    const auto pos = workerThreadId % hashMaps.size();

    if (hashMaps.at(pos) == nullptr)
    {
        hashMaps.at(pos) = std::make_unique<ChainedHashMap>(
            createNewHashMapSliceArgs.keySize,
            createNewHashMapSliceArgs.valueSize,
            createNewHashMapSliceArgs.numberOfBuckets,
            createNewHashMapSliceArgs.pageSize);
    }
    return hashMaps[pos].get();
}

bool SpillableAggregationSlice::isResident() const
{
    return resident;
}

uint64_t SpillableAggregationSlice::getNumberOfTuples() const
{
    /// While spilled the maps are freed, so report the snapshot instead of dereferencing them.
    return resident ? HashMapSlice::getNumberOfTuples() : spilledTupleCount;
}

void SpillableAggregationSlice::spill(SpillBackend& backend)
{
    if (!resident)
    {
        return;
    }
    /// Worker positions are encoded into the spill key as WorkerThreadId (uint32_t).
    INVARIANT(
        hashMaps.size() <= std::numeric_limits<WorkerThreadId::Underlying>::max(),
        "number of hashmaps {} exceeds the WorkerThreadId range",
        hashMaps.size());

    const uint64_t entrySize = entrySizeFor(createNewHashMapSliceArgs);
    uint64_t totalTuples = 0;
    for (uint64_t pos = 0; pos < hashMaps.size(); ++pos)
    {
        if (hashMaps[pos] == nullptr)
        {
            continue; /// worker never created a map — nothing to spill
        }
        auto* const chained = dynamic_cast<ChainedHashMap*>(hashMaps[pos].get());
        INVARIANT(chained != nullptr, "SpillableAggregationSlice expects ChainedHashMap-backed maps");

        totalTuples += chained->getNumberOfTuples();
        const SpillRecord record = SliceSerializer::serialize(
            *chained, entrySize, createNewHashMapSliceArgs.pageSize, createNewHashMapSliceArgs.numberOfBuckets);
        backend.put(getSliceEnd(), WorkerThreadId(static_cast<WorkerThreadId::Underlying>(pos)), record);
        hashMaps[pos].reset(); /// free the map; its TupleBuffers return to the BufferManager
    }
    /// An all-empty slice writes no records but still becomes non-resident; unspill() then restores nothing.
    spilledTupleCount = totalTuples;
    resident = false;
}

void SpillableAggregationSlice::unspill(SpillBackend& backend, AbstractBufferProvider& bufferProvider)
{
    if (resident)
    {
        return;
    }
    INVARIANT(
        hashMaps.size() <= std::numeric_limits<WorkerThreadId::Underlying>::max(),
        "number of hashmaps {} exceeds the WorkerThreadId range",
        hashMaps.size());
    for (uint64_t pos = 0; pos < hashMaps.size(); ++pos)
    {
        if (auto record = backend.get(getSliceEnd(), WorkerThreadId(static_cast<WorkerThreadId::Underlying>(pos))); record.has_value())
        {
            hashMaps[pos] = SliceSerializer::deserialize(*record, bufferProvider);
        }
    }
    resident = true;
}

}
