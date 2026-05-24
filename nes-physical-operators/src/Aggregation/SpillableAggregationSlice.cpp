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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceSerializer.hpp>
#include <SliceStore/SpillBackend.hpp>
#include <Util/Logger/Logger.hpp>
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

/// Replays every entry of `src` into `dst` by re-inserting (hash, payload) — the same entry-iteration +
/// insertEntry/memcpy that SliceSerializer::deserialize uses to replay a record into a fresh map (see
/// SliceSerializer.cpp: `insertEntry(hash, &bufferProvider)` then memcpy of the payload bytes). Used to
/// merge sibling partition blobs into one base map on the unspill() P>1 path. Keys across partitions are
/// disjoint (a key's hash fixes its partition), so this never needs an aggregate-combine — it is a pure
/// insert. `entrySize` describes the shared map layout; the payload is the entry minus its header.
void replayEntriesInto(ChainedHashMap& dst, const ChainedHashMap& src, const uint64_t entrySize, AbstractBufferProvider& bufferProvider)
{
    const uint64_t payloadSize = entrySize - sizeof(ChainedHashMapEntry);
    if (src.getNumberOfTuples() == 0)
    {
        return;
    }
    for (uint64_t chain = 0; chain < src.getNumberOfChains(); ++chain)
    {
        for (const auto* entry = src.getStartOfChain(chain); entry != nullptr; entry = entry->next)
        {
            auto* const inserted = static_cast<ChainedHashMapEntry*>(dst.insertEntry(entry->hash, &bufferProvider));
            /// insertEntry rents a page from the bufferProvider; it returns null on an exhausted pool. The
            /// memcpy below would dereference that null, so guard here. This is a resource/contract violation
            /// on a constrained device, not user input, so INVARIANT (terminate) is the right NES idiom.
            INVARIANT(inserted != nullptr, "insertEntry returned null: bufferProvider exhausted during partition merge");
            const auto* const srcPayload = reinterpret_cast<const std::byte*>(entry) + sizeof(ChainedHashMapEntry);
            auto* const dstPayload = reinterpret_cast<std::byte*>(inserted) + sizeof(ChainedHashMapEntry);
            std::memcpy(dstPayload, srcPayload, payloadSize);
        }
    }
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

void SpillableAggregationSlice::spill(SpillBackend& backend, const uint64_t numberOfPartitions)
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
    NES_DEBUG("spill: sliceEnd={} workers={} P={}", getSliceEnd().getRawValue(), hashMaps.size(), numberOfPartitions);
    uint64_t totalTuples = 0;
    for (uint64_t pos = 0; pos < hashMaps.size(); ++pos)
    {
        if (hashMaps[pos] == nullptr)
        {
            continue; /// worker never created a map — nothing to spill
        }
        auto* const chained = dynamic_cast<ChainedHashMap*>(hashMaps[pos].get());
        INVARIANT(chained != nullptr, "SpillableAggregationSlice expects ChainedHashMap-backed maps");
        const auto worker = WorkerThreadId(static_cast<WorkerThreadId::Underlying>(pos));

        totalTuples += chained->getNumberOfTuples();

        if (numberOfPartitions == 1)
        {
            /// L1 fast-path: byte-identical to the pre-partitioning behaviour — one whole-map blob under
            /// the default partition 0.
            const SpillRecord record = SliceSerializer::serialize(
                *chained, entrySize, createNewHashMapSliceArgs.pageSize, createNewHashMapSliceArgs.numberOfBuckets);
            backend.put(getSliceEnd(), worker, record);
        }
        else
        {
            /// P>1: grace-hash-split the worker map into P self-contained blobs and write each non-empty
            /// one under its partition id. partitionedSerialize returns exactly P records; record[p]'s
            /// own numEntries header is its entry count, so a zero-entry partition is detected without a
            /// second walk (SKIP-EMPTY at write, L3).
            const std::vector<SpillRecord> records = SliceSerializer::partitionedSerialize(
                *chained, entrySize, createNewHashMapSliceArgs.pageSize, createNewHashMapSliceArgs.numberOfBuckets, numberOfPartitions);
            uint64_t putCount = 0;
            for (uint64_t p = 0; p < records.size(); ++p)
            {
                /// A partition blob with zero entries is exactly the HEADER_SIZE bytes; skip writing it.
                if (records[p].size() <= SliceSerializer::HEADER_SIZE)
                {
                    continue;
                }
                backend.put(getSliceEnd(), worker, records[p], static_cast<PartitionId>(p));
                ++putCount;
            }
            NES_TRACE("spill: worker={} wrote {}/{} partition blobs", pos, putCount, numberOfPartitions);
        }

        /// L5 ordering: free this worker's map only after its put(s) above have succeeded, mirroring the
        /// per-worker order of the historic path. A throw mid-loop leaves earlier on-disk blobs orphaned
        /// but harmless (overwritten on retry / dropped by erase at GC).
        hashMaps[pos].reset(); /// free the map; its TupleBuffers return to the BufferManager
    }
    /// An all-empty slice writes no records but still becomes non-resident; unspill() then restores nothing.
    spilledTupleCount = totalTuples;
    resident = false;
}

void SpillableAggregationSlice::streamEmitByPartition(
    SpillBackend& backend,
    AbstractBufferProvider& bufferProvider,
    const uint64_t numberOfPartitions,
    const PartitionId partition,
    const std::function<void(std::vector<std::unique_ptr<HashMap>>)>& emit) const
{
    /// This is the spilled-streaming read-out: the caller (2d) guarantees the slice is already spilled
    /// before invoking it, so a resident slice here is a contract violation, not user input.
    PRECONDITION(!resident, "streamEmitByPartition called on a resident slice; it reads spilled state only");
    PRECONDITION(
        static_cast<uint64_t>(partition) < numberOfPartitions,
        "partition {} out of range for P={}",
        static_cast<uint64_t>(partition),
        numberOfPartitions);

    /// `numberOfPartitions` is consumed only by the range PRECONDITION above; the loop below reads each
    /// worker's blob for the single fixed `partition` index directly from the backend and never iterates over
    /// partitions, so it needs only `partition` — not P. (It is NOT an unused parameter.)
    ///
    /// Collect this partition's blob for every worker (deserialized whole, no filtering). A worker whose
    /// blob is absent (skip-empty at write, or never populated) simply contributes nothing.
    std::vector<std::unique_ptr<HashMap>> collected;
    for (uint64_t pos = 0; pos < hashMaps.size(); ++pos)
    {
        const auto worker = WorkerThreadId(static_cast<WorkerThreadId::Underlying>(pos));
        if (auto record = backend.get(getSliceEnd(), worker, partition); record.has_value())
        {
            /// deserialize returns unique_ptr<ChainedHashMap>; the implicit upcast to unique_ptr<HashMap>
            /// is safe because HashMap has a virtual destructor.
            collected.push_back(SliceSerializer::deserialize(*record, bufferProvider));
        }
    }

    NES_TRACE(
        "streamEmitByPartition: sliceEnd={} partition={} materialized {} worker maps (slice stays spilled)",
        getSliceEnd().getRawValue(),
        static_cast<uint64_t>(partition),
        collected.size());

    /// Hand ownership to the consumer; the slice is NOT modified and stays spilled (resident == false).
    emit(std::move(collected));
}

void SpillableAggregationSlice::mergePartitionsInto(
    std::unique_ptr<HashMap>& base,
    SpillBackend& backend,
    AbstractBufferProvider& bufferProvider,
    const WorkerThreadId worker,
    const uint64_t numberOfPartitions) const
{
    const uint64_t entrySize = entrySizeFor(createNewHashMapSliceArgs);
    for (uint64_t p = 0; p < numberOfPartitions; ++p)
    {
        auto record = backend.get(getSliceEnd(), worker, static_cast<PartitionId>(p));
        if (!record.has_value())
        {
            continue; /// absent partition blob (skip-empty at write, or never populated)
        }
        auto materialized = SliceSerializer::deserialize(*record, bufferProvider);
        if (base == nullptr)
        {
            /// First present partition becomes the base map for this worker.
            base = std::move(materialized);
            continue;
        }
        /// Subsequent partitions hold disjoint keys, so merge by replaying their entries into the base.
        auto* const baseChained = dynamic_cast<ChainedHashMap*>(base.get());
        INVARIANT(baseChained != nullptr, "SpillableAggregationSlice expects ChainedHashMap-backed maps");
        replayEntriesInto(*baseChained, *materialized, entrySize, bufferProvider);
    }
}

void SpillableAggregationSlice::unspill(SpillBackend& backend, AbstractBufferProvider& bufferProvider, const uint64_t numberOfPartitions)
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
        const auto worker = WorkerThreadId(static_cast<WorkerThreadId::Underlying>(pos));
        if (numberOfPartitions == 1)
        {
            /// L1 fast-path: a single whole-map blob under partition 0, byte-identical to the historic path.
            if (auto record = backend.get(getSliceEnd(), worker); record.has_value())
            {
                hashMaps[pos] = SliceSerializer::deserialize(*record, bufferProvider);
            }
        }
        else
        {
            /// P>1: merge all P partition blobs of this worker back into one map (keys are disjoint).
            mergePartitionsInto(hashMaps[pos], backend, bufferProvider, worker, numberOfPartitions);
        }
    }
    resident = true;
}

}
