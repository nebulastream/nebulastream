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

#include <IREEInferenceLocalState.hpp>
#include <PredictionCache/PredictionCache.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>

namespace NES
{
PredictionCache::PredictionCache(
    const nautilus::val<OperatorHandler*>& operatorHandler,
    const uint64_t numberOfEntries,
    const uint64_t sizeOfEntry,
    const nautilus::val<int8_t*>& startOfEntries,
    const nautilus::val<uint64_t*>& hitsRef,
    const nautilus::val<uint64_t*>& missesRef,
    const nautilus::val<size_t>& inputSize)
    : IREEInferenceLocalState(operatorHandler)
    , startOfEntries(startOfEntries)
    , numberOfEntries(numberOfEntries)
    , sizeOfEntry(sizeOfEntry)
    , numberOfHits(hitsRef)
    , numberOfMisses(missesRef)
    , inputSize(inputSize)
{
}

void PredictionCache::incrementNumberOfHits()
{
    auto currentNumberOfHits = static_cast<nautilus::val<uint64_t>>(*numberOfHits);
    currentNumberOfHits = currentNumberOfHits + 1;
    nautilus::invoke(+[](uint64_t n){ std::cout << "Hits: " << n << '\n'; }, currentNumberOfHits);
    *numberOfHits = currentNumberOfHits;
}

void PredictionCache::incrementNumberOfMisses()
{
    auto currentNumberOfMisses = static_cast<nautilus::val<uint64_t>>(*numberOfMisses);
    currentNumberOfMisses = currentNumberOfMisses + 1;
    nautilus::invoke(+[](uint64_t n){ std::cout << "Misses: " << n << '\n'; }, currentNumberOfMisses);
    *numberOfMisses = currentNumberOfMisses;
}

nautilus::val<std::byte*> PredictionCache::getRecord(const nautilus::val<uint64_t>& pos)
{
    const auto predictionCacheEntry = startOfEntries + pos * sizeOfEntry;
    const auto recordRef = Nautilus::Util::getMemberRef(predictionCacheEntry, &PredictionCacheEntry::record);
    auto record = Nautilus::Util::readValueFromMemRef<int8_t*>(recordRef);
    return nautilus::val<std::byte*>(record);
}

nautilus::val<int8_t*> PredictionCache::getDataStructure(const nautilus::val<uint64_t>& pos)
{
    const auto predictionCacheEntry = startOfEntries + pos * sizeOfEntry;
    const auto dataStructureRef = Nautilus::Util::getMemberRef(predictionCacheEntry, &PredictionCacheEntry::dataStructure);
    auto dataStructure = Nautilus::Util::readValueFromMemRef<int8_t*>(dataStructureRef);
    return dataStructure;
}

nautilus::val<bool> PredictionCache::foundRecord(const nautilus::val<uint64_t>& pos, const nautilus::val<std::byte*>& candidateRecord)
{
    const auto cacheRecord = getRecord(pos);
    auto isEqual = nautilus::invoke(+[](std::byte* candidate, std::byte* cache, size_t size)
    {
        if (cache != nullptr)
        {
            return std::memcmp(candidate, cache, size) == 0;
        }
        return false;
    }, candidateRecord, cacheRecord, nautilus::val(inputSize));
    if (isEqual)
    {
        return true;
    }
    return false;
}

nautilus::val<uint64_t> PredictionCache::searchInCache(const nautilus::val<std::byte*>& record)
{
    for (nautilus::val<uint64_t> i = 0; i < numberOfEntries; i = i + 1)
    {
        if (foundRecord(i, record))
        {
            return i;
        }
    }
    return nautilus::val<uint64_t>(NOT_FOUND);
}
}
