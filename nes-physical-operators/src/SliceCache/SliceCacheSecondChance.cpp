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
#include <SliceCache/SliceCacheSecondChance.hpp>

#include <cstdint>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <SliceCache/SliceCache.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/val_bool.hpp>
#include <nautilus/val_ptr.hpp>
#include <val_arith.hpp>

namespace NES
{
SliceCacheSecondChance::SliceCacheSecondChance(const uint64_t numberOfEntries, const uint64_t sizeOfEntry)
    : SliceCache{numberOfEntries, sizeOfEntry}, replacementIndexRaw(nullptr)
{
}

SliceCacheSecondChance::SliceCacheSecondChance(const SliceCacheSecondChance& other)
    : SliceCache{other.numberOfEntries, other.sizeOfEntry}, replacementIndexRaw(nullptr)
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

void SliceCacheSecondChance::setStartOfEntries(SliceCacheEntry* startOfEntries)
{
    SliceCache::setStartOfEntries(startOfEntries);
    /// The replacement indices are stored right after all cache entries (for all threads) in the same memory buffer.
    replacementIndexRaw = reinterpret_cast<uint64_t*>(
        reinterpret_cast<SliceCacheEntrySecondChance*>(startOfEntries) + (numberOfWorkerThreads * numberOfEntries));
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
            return {.pos = i, .foundInCache = true};
        }
    }
    return {.pos = 0, .foundInCache = false};
}

nautilus::val<int8_t*> SliceCacheSecondChance::getDataStructureRef(
    const nautilus::val<Timestamp>& timestamp,
    const nautilus::val<WorkerThreadId>& workerThreadId,
    const SliceCacheReplaceEntry& replaceEntry)
{
    /// We must use SliceCacheEntrySecondChance* for all pointer arithmetic, because the entries
    /// in memory are SliceCacheEntrySecondChance objects (40 bytes), not SliceCacheEntry (32 bytes).
    const nautilus::val<SliceCacheEntrySecondChance*> startOfEntries{reinterpret_cast<SliceCacheEntrySecondChance*>(startOfAllEntries)};

    /// Each worker thread uses its own partition of the cache to avoid data races.
    const nautilus::val<SliceCacheEntrySecondChance*> threadLocalStart = startOfEntries + workerThreadId.convertToValue() * numberOfEntries;
    const nautilus::val<uint64_t*> replacementIndexBase{replacementIndexRaw};
    nautilus::val<uint64_t*> replacementIndexPtr = replacementIndexBase + workerThreadId.convertToValue();

    /// We check if the timestamp is already in the cache.
    if (const auto [pos, foundInCache] = searchInCache(threadLocalStart, timestamp); foundInCache)
    {
        nautilus::val<SliceCacheEntrySecondChance*> sliceCacheEntryToReplace = threadLocalStart + pos;
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
    *replacementIndexPtr = replacementIndex;

    /// Replacing the slice and returning the data structure.
    nautilus::val<SliceCacheEntrySecondChance*> sliceCacheEntryToReplace = threadLocalStart + replacementIndex;
    sliceCacheEntryToReplace.get(&SliceCacheEntrySecondChance::secondChanceBit) = nautilus::val<bool>{true};
    replaceEntry(nautilus::val<SliceCacheEntry*>{threadLocalStart + replacementIndex});
    return nautilus::val<SliceCacheEntry*>{threadLocalStart + replacementIndex}.get(&SliceCacheEntry::dataStructure);
}

}
