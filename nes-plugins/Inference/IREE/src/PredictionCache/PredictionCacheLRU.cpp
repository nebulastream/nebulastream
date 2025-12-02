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

#include <PredictionCache/PredictionCacheLRU.hpp>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>

namespace NES
{
PredictionCacheLRU::PredictionCacheLRU(
    const nautilus::val<OperatorHandler*>& operatorHandler,
    const uint64_t numberOfEntries,
    const uint64_t sizeOfEntry,
    const nautilus::val<int8_t*>& startOfEntries,
    const nautilus::val<uint64_t*>& hitsRef,
    const nautilus::val<uint64_t*>& missesRef,
    const nautilus::val<size_t>& inputSize)
    : PredictionCache(operatorHandler, numberOfEntries, sizeOfEntry, startOfEntries, hitsRef, missesRef, inputSize)
{
}

nautilus::val<uint64_t*> PredictionCacheLRU::getAgeBit(const nautilus::val<uint64_t>& pos)
{
    const auto PredictionCacheEntry = startOfEntries + pos * sizeOfEntry;
    const auto ageBitRef = Nautilus::Util::getMemberRef(PredictionCacheEntry, &PredictionCacheEntryLRU::ageBit);
    return ageBitRef;
}

void PredictionCacheLRU::updateValues(const PredictionCache::PredictionCacheUpdate& updateFunction)
{
    nautilus::val<uint64_t> maxAge = 0;
    nautilus::val<uint64_t> maxAgeIndex = 0;
    for (nautilus::val<uint64_t> i = 0; i < numberOfEntries; ++i)
    {
        auto ageBit = getAgeBit(i);
        const auto newAgeBit = nautilus::val<uint64_t>(*ageBit) + nautilus::val<uint64_t>(1);
        if (newAgeBit > maxAge)
        {
            maxAge = newAgeBit;
            maxAgeIndex = i;
        }
    }

    const nautilus::val<PredictionCacheEntry*> PredictionCacheEntryToReplace = startOfEntries + maxAgeIndex * sizeOfEntry;
    updateFunction(PredictionCacheEntryToReplace, maxAgeIndex);
}

nautilus::val<uint64_t> PredictionCacheLRU::updateKeys(const nautilus::val<std::byte*>& record, const PredictionCache::PredictionCacheUpdate& updateFunction)
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
    if (const auto dataStructurePos = PredictionCache::searchInCache(record); dataStructurePos != PredictionCache::NOT_FOUND)
    {
        incrementNumberOfHits();
        auto ageBit = getAgeBit(dataStructurePos);
        const auto newAgeBit = nautilus::val<uint64_t>(0);
        *ageBit = newAgeBit;
        return dataStructurePos;
    }

    /// If the timestamp is not in the cache, we have a cache miss.
    incrementNumberOfMisses();

    /// Third, we have to replace the entry with the highest age bit, as we are in the LRU cache.
    /// Additionally, we have to reset the age bit of the replaced entry.
    const nautilus::val<PredictionCacheEntry*> PredictionCacheEntryToReplace = startOfEntries + maxAgeIndex * sizeOfEntry;
    updateFunction(PredictionCacheEntryToReplace, maxAgeIndex);
    *getAgeBit(maxAgeIndex) = 0;
    return nautilus::val<uint64_t>(NOT_FOUND);
}

nautilus::val<std::vector<std::byte>*>
PredictionCacheLRU::getDataStructureRef(const nautilus::val<std::byte*>& record, const PredictionCache::PredictionCacheReplacement& replacementFunction)
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
    if (const auto dataStructurePos = PredictionCache::searchInCache(record); dataStructurePos != PredictionCache::NOT_FOUND)
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
    const nautilus::val<PredictionCacheEntry*> PredictionCacheEntryToReplace = startOfEntries + maxAgeIndex * sizeOfEntry;
    const auto dataStructure = replacementFunction(PredictionCacheEntryToReplace, maxAgeIndex);
    *getAgeBit(maxAgeIndex) = 0;
    return dataStructure;
}
}
