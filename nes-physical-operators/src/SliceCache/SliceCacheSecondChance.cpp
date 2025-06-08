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

#include <Nautilus/DataTypes/DataTypesUtil.hpp>

namespace NES
{
SliceCacheSecondChance::SliceCacheSecondChance(
    const nautilus::val<OperatorHandler*>& operatorHandler,
    const uint64_t numberOfEntries,
    const uint64_t sizeOfEntry,
    const nautilus::val<int8_t*>& startOfEntries,
    const nautilus::val<uint64_t*>& hitsRef,
    const nautilus::val<uint64_t*>& missesRef)
    : SliceCacheFIFO(operatorHandler, numberOfEntries, sizeOfEntry, startOfEntries, hitsRef, missesRef)
{
}

nautilus::val<bool*> SliceCacheSecondChance::getSecondChanceBit(const nautilus::val<uint64_t>& pos)
{
    const auto sliceCacheEntry = startOfEntries + pos * sizeOfEntry;
    const auto secondChanceBitRef = Nautilus::Util::getMemberRef(sliceCacheEntry, &SliceCacheEntrySecondChance::secondChanceBit);
    return secondChanceBitRef;
}


nautilus::val<int8_t*> SliceCacheSecondChance::getDataStructureRef(
    const nautilus::val<Timestamp>& timestamp, const SliceCache::SliceCacheReplacement& replacementFunction)
{
    /// First, we check if the timestamp is already in the cache.
    if (const auto dataStructurePos = SliceCache::searchInCache(timestamp); dataStructurePos != SliceCache::NOT_FOUND)
    {
        incrementNumberOfHits();
        auto secondChanceBit = getSecondChanceBit(dataStructurePos);
        *secondChanceBit = true;
        return getDataStructure(dataStructurePos);
    }

    /// If this is not the case, we iterate through the cache until we have find a slice that has the second chance bit set to false.
    /// If we find such a slice, we set the second chance bit to true, replace the slice and return the data structure.
    /// We must start at the current replacement index, as we have to replace the oldest entry.
    incrementNumberOfMisses();
    auto secondChanceBit = getSecondChanceBit(replacementIndex);
    while (*secondChanceBit == true)
    {
        *secondChanceBit = false;
        replacementIndex = (replacementIndex + 1) % numberOfEntries;
        secondChanceBit = getSecondChanceBit(replacementIndex);
    }

    /// Replacing the slice and returning the data structure.
    const nautilus::val<SliceCacheEntry*> sliceCacheEntryToReplace = startOfEntries + replacementIndex * sizeOfEntry;
    const auto dataStructure = replacementFunction(sliceCacheEntryToReplace, replacementIndex);
    *secondChanceBit = true;
    return getDataStructure(replacementIndex);
}

}
