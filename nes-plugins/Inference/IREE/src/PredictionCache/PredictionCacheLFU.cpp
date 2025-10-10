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

#include <PredictionCache/PredictionCacheLFU.hpp>

namespace NES
{
PredictionCacheLFU::PredictionCacheLFU(
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

nautilus::val<int8_t*>
PredictionCacheLFU::getDataStructureRef(const nautilus::val<std::byte*>& record, const PredictionCache::PredictionCacheReplacement& replacementFunction)
{
    /// First, we check if the timestamp is already in the cache.
    if (const auto dataStructurePos = PredictionCache::searchInCache(record); dataStructurePos != PredictionCache::NOT_FOUND)
    {
        incrementNumberOfHits();
        auto frequency = getFrequency(dataStructurePos);
        const auto newFrequency = nautilus::val<uint64_t>(*frequency) + nautilus::val<uint64_t>(1);
        *frequency = newFrequency;
        return getDataStructure(dataStructurePos);
    }

    /// Second, if this is not the case, we need to find the item with the lowest frequency.
    incrementNumberOfMisses();
    nautilus::val<uint64_t> minFrequency = UINT64_MAX;
    nautilus::val<uint64_t> minFrequencyIndex = 0;
    for (nautilus::val<uint64_t> i = 0; i < numberOfEntries; ++i)
    {
        nautilus::val<uint64_t> frequency{*getFrequency(i)};
        if (frequency < minFrequency)
        {
            minFrequency = frequency;
            minFrequencyIndex = i;
        }
    }

    /// Third, we have to replace the entry at the minFrequencyIndex
    const nautilus::val<PredictionCacheEntry*> PredictionCacheEntryToReplace = startOfEntries + minFrequencyIndex * sizeOfEntry;
    const auto dataStructure = replacementFunction(PredictionCacheEntryToReplace, minFrequency);
    *getFrequency(minFrequencyIndex) = 1;
    return dataStructure;

}

nautilus::val<uint64_t*> PredictionCacheLFU::getFrequency(const nautilus::val<uint64_t>& pos)
{
    const auto PredictionCacheEntry = startOfEntries + pos * sizeOfEntry;
    const auto frequencyRef = Nautilus::Util::getMemberRef(PredictionCacheEntry, &PredictionCacheEntryLFU::frequency);
    return frequencyRef;
}

}