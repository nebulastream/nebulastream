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

#include <SliceCache/SliceCacheLRU.hpp>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>

namespace NES
{
SliceCacheLRU::SliceCacheLRU(
    const nautilus::val<OperatorHandler*>& operatorHandler,
    const uint64_t numberOfEntries,
    const uint64_t sizeOfEntry,
    const nautilus::val<int8_t*>& startOfEntries,
    const nautilus::val<uint64_t*>& hitsRef,
    const nautilus::val<uint64_t*>& missesRef)
    : SliceCache(operatorHandler, numberOfEntries, sizeOfEntry, startOfEntries, hitsRef, missesRef)
{
}

nautilus::val<uint64_t*> SliceCacheLRU::getAgeBit(const nautilus::val<uint64_t>& pos)
{
    const auto sliceCacheEntry = startOfEntries + pos * sizeOfEntry;
    const auto ageBitRef = Nautilus::Util::getMemberRef(sliceCacheEntry, &SliceCacheEntryLRU::ageBit);
    return ageBitRef;
}

nautilus::val<int8_t*>
SliceCacheLRU::getDataStructureRef(const nautilus::val<Timestamp>& timestamp, const SliceCache::SliceCacheReplacement& replacementFunction)
{
    /// First, we have to increment all age bits by one.
    nautilus::val<uint64_t> maxAge = 0;
    nautilus::val<uint64_t> maxAgeIndex = 0;
    for (nautilus::val<uint64_t> i = 0; i < numberOfEntries; ++i)
    {
        auto ageBit = getAgeBit(i);
        const auto newAgeBit = nautilus::val<uint64_t>(*ageBit) + nautilus::val<uint64_t>(1);
        *ageBit = newAgeBit;
        if (newAgeBit > maxAge)
        {
            maxAge = newAgeBit;
            maxAgeIndex = i;
        }
    }

    /// Second, we check if the timestamp is already in the cache. If this is the case, we reset its age bit
    if (const auto dataStructurePos = SliceCache::searchInCache(timestamp); dataStructurePos != SliceCache::NOT_FOUND)
    {
        incrementNumberOfHits();
        auto ageBit = getAgeBit(dataStructurePos);
        const auto newAgeBit = nautilus::val<uint64_t>(0);
        *ageBit = newAgeBit;
        return getDataStructure(dataStructurePos);
    }

    /// If the timestamp is not in the cache, we have a cache miss.
    incrementNumberOfMisses();

    /// Third, we have to replace the entry with the highest age bit, as we are in the LRU cache.
    /// Additionally, we have to reset the age bit of the replaced entry.
    const nautilus::val<SliceCacheEntry*> sliceCacheEntryToReplace = startOfEntries + maxAgeIndex * sizeOfEntry;
    const auto dataStructure = replacementFunction(sliceCacheEntryToReplace, maxAgeIndex);
    *getAgeBit(maxAgeIndex) = 0;
    return dataStructure;
}
}
