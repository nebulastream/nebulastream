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

#include <PredictionCache/PredictionCacheFIFO.hpp>

#include <cstdint>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <PredictionCache/PredictionCache.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/val.hpp>

namespace NES
{
PredictionCacheFIFO::PredictionCacheFIFO(
    const nautilus::val<OperatorHandler*>& operatorHandler,
    const uint64_t numberOfEntries,
    const uint64_t sizeOfEntry,
    const nautilus::val<int8_t*>& startOfEntries,
    const nautilus::val<uint64_t*>& hitsRef,
    const nautilus::val<uint64_t*>& missesRef,
    const nautilus::val<size_t>& inputSize)
    : PredictionCache(operatorHandler, numberOfEntries, sizeOfEntry, startOfEntries, hitsRef, missesRef, inputSize), replacementIndex(0)
{
}

void PredictionCacheFIFO::updateValues(const PredictionCache::PredictionCacheUpdate& updateFunction)
{
    const nautilus::val<PredictionCacheEntry*> predictionCacheEntryToReplace = startOfEntries + replacementIndex * sizeOfEntry;
    updateFunction(predictionCacheEntryToReplace, replacementIndex);
}

nautilus::val<uint64_t> PredictionCacheFIFO::updateKeys(const nautilus::val<std::byte*>& record, const PredictionCache::PredictionCacheUpdate& updateFunction)
{
    /// First, we check if the timestamp is already in the cache.
    if (const auto dataStructurePos = PredictionCache::searchInCache(record); dataStructurePos != PredictionCache::NOT_FOUND)
    {
        incrementNumberOfHits();
        return dataStructurePos;
    }

    /// If the timestamp is not in the cache, we have a cache miss.
    incrementNumberOfMisses();

    /// As we are in the FIFO cache, we need to replace the oldest entry with the new one.
    const nautilus::val<PredictionCacheEntry*> predictionCacheEntryToReplace = startOfEntries + replacementIndex * sizeOfEntry;
    updateFunction(predictionCacheEntryToReplace, replacementIndex);

    /// Before returning the data structure, we need to update the replacement index.
    replacementIndex = (replacementIndex + 1) % numberOfEntries;
    return nautilus::val<uint64_t>(NOT_FOUND);
}

nautilus::val<std::vector<std::byte>*>
PredictionCacheFIFO::getDataStructureRef(const nautilus::val<std::byte*>& record, const PredictionCache::PredictionCacheReplacement& replacementFunction)
{
    /// First, we check if the timestamp is already in the cache.
    if (const auto dataStructurePos = PredictionCache::searchInCache(record); dataStructurePos != PredictionCache::NOT_FOUND)
    {
        incrementNumberOfHits();
        return getDataStructure(dataStructurePos);
    }

    /// If the timestamp is not in the cache, we have a cache miss.
    incrementNumberOfMisses();

    /// As we are in the FIFO cache, we need to replace the oldest entry with the new one.
    const nautilus::val<PredictionCacheEntry*> predictionCacheEntryToReplace = startOfEntries + replacementIndex * sizeOfEntry;
    const auto dataStructure = replacementFunction(predictionCacheEntryToReplace, replacementIndex);

    /// Before returning the data structure, we need to update the replacement index.
    replacementIndex = (replacementIndex + 1) % numberOfEntries;
    return dataStructure;
}
}
