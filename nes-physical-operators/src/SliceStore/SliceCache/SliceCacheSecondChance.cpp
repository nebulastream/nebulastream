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
#include <SliceStore/SliceCache/SliceCacheSecondChance.hpp>

#include <cstdint>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <SliceStore/SliceCache/SliceCache.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/val_bool.hpp>
#include <nautilus/val_ptr.hpp>
#include <val_arith.hpp>

namespace NES
{
SliceCacheSecondChance::SliceCacheSecondChance(const uint64_t numberOfEntries, const uint64_t sizeOfEntry)
    : SliceCache{numberOfEntries, sizeOfEntry}
{
}

SliceCacheSecondChance::SliceCacheSecondChance(const SliceCacheSecondChance& other) : SliceCache{other.numberOfEntries, other.sizeOfEntry}
{
}

std::unique_ptr<SliceCache> SliceCacheSecondChance::clone() const
{
    return std::make_unique<SliceCacheSecondChance>(*this);
}

uint64_t SliceCacheSecondChance::getCacheMemorySize() const
{
    return SliceCache::getCacheMemorySize() + (numberOfWorkerThreads * sizeof(uint64_t));
}

SliceCacheSecondChance::EntryFound SliceCacheSecondChance::searchInCache(
    const nautilus::val<SliceCacheEntrySecondChance*>& threadLocalStart, const nautilus::val<Timestamp>& timestamp)
{
    /// We assume that a timestamp is in the cache, if the timestamp is in the range of the slice, e.g., sliceStart <= timestamp < sliceEnd.
    for (nautilus::val<uint64_t> i = 0; i < numberOfEntries; ++i)
    {
        nautilus::val<SliceCacheEntrySecondChance*> sliceCacheEntry = threadLocalStart + i;
        const auto sliceStart = nautilus::val<Timestamp>{sliceCacheEntry.get(&SliceCacheEntry::sliceStart)};
        const auto sliceEnd = nautilus::val<Timestamp>{sliceCacheEntry.get(&SliceCacheEntry::sliceEnd)};
        if (sliceStart <= timestamp && timestamp < sliceEnd)
        {
            return {.sliceCacheEntry = sliceCacheEntry, .foundInCache = true};
        }
    }
    return {.sliceCacheEntry = nullptr, .foundInCache = false};
}

nautilus::val<SliceCacheEntry::DataStructure> SliceCacheSecondChance::getDataStructureRef(
    const nautilus::val<Timestamp>& timestamp,
    const nautilus::val<WorkerThreadId>& workerThreadId,
    const SliceCacheReplaceEntry& replaceEntry)
{
    /// We must use SliceCacheEntrySecondChance* for all pointer arithmetic, because the entries
    /// in memory are SliceCacheEntrySecondChance objects (40 bytes), not SliceCacheEntry (32 bytes).
    const nautilus::val<uint64_t> offset{numberOfWorkerThreads * numberOfEntries};
    const auto replacementIndexRaw = static_cast<nautilus::val<uint64_t*>>(
        static_cast<nautilus::val<SliceCacheEntrySecondChance*>>(nautilus::val<SliceCacheEntry*>{startOfSliceCache}) + offset);

    /// Each worker thread uses its own partition of the cache to avoid data races.
    const nautilus::val<SliceCacheEntrySecondChance*> threadLocalStart
        = static_cast<nautilus::val<SliceCacheEntrySecondChance*>>(nautilus::val<SliceCacheEntry*>{startOfSliceCache})
        + workerThreadId.convertToValue() * numberOfEntries;
    const nautilus::val<uint64_t*>& replacementIndexBase{replacementIndexRaw};
    nautilus::val<uint64_t*> replacementIndexPtr = replacementIndexBase + workerThreadId.convertToValue();

    /// We check if the timestamp is already in the cache.
    if (auto [sliceCacheEntryToReplace, foundInCache] = searchInCache(threadLocalStart, timestamp); foundInCache)
    {
        sliceCacheEntryToReplace.get(&SliceCacheEntrySecondChance::secondChanceBit) = nautilus::val<bool>{true};
        return nautilus::val<SliceCacheEntry*>{sliceCacheEntryToReplace}.get(&SliceCacheEntry::dataStructure);
    }

    /// If this is not the case, we iterate through the cache until we have find a slice that has the second chance bit set to false.
    /// If we find such a slice, we set the second chance bit to true, replace the slice and return the data structure.
    /// We must start at the current replacement index, as we have to replace the oldest entry.
    nautilus::val<uint64_t> replacementIndex = *replacementIndexPtr;
    for (nautilus::val<uint64_t> i = 0; i < 2 * numberOfEntries; ++i)
    {
        nautilus::val<SliceCacheEntrySecondChance*> sliceCacheEntryToReplace = threadLocalStart + replacementIndex;
        const nautilus::val<bool> secondChanceBit = sliceCacheEntryToReplace.get(&SliceCacheEntrySecondChance::secondChanceBit);
        if (secondChanceBit == nautilus::val<bool>{false})
        {
            break;
        }
        sliceCacheEntryToReplace.get(&SliceCacheEntrySecondChance::secondChanceBit) = nautilus::val<bool>{false};
        replacementIndex = (replacementIndex + 1) % numberOfEntries;
    }
    /// Increment the replacement to give the current replaced entry not the first victim next miss
    *replacementIndexPtr = (replacementIndex + 1) % numberOfEntries;

    /// Replacing the slice and returning the data structure.
    nautilus::val<SliceCacheEntrySecondChance*> sliceCacheEntryToReplace = threadLocalStart + replacementIndex;
    sliceCacheEntryToReplace.get(&SliceCacheEntrySecondChance::secondChanceBit) = nautilus::val<bool>{true};
    replaceEntry(nautilus::val<SliceCacheEntry*>{threadLocalStart + replacementIndex});
    return nautilus::val<SliceCacheEntry*>{threadLocalStart + replacementIndex}.get(&SliceCacheEntry::dataStructure);
}

}
