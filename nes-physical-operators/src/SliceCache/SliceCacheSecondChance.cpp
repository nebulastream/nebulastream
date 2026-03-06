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
#include <Nautilus/Interface/TimestampRef.hpp>
#include <SliceCache/SliceCache.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_bool.hpp>
#include <nautilus/val_ptr.hpp>
#include <nautilus/val_std.hpp>

namespace NES
{
SliceCacheSecondChance::SliceCacheSecondChance(const uint64_t numberOfEntries, const uint64_t sizeOfEntry)
    : SliceCache(numberOfEntries, sizeOfEntry)
{
}

SliceCacheSecondChance::EntryFound SliceCacheSecondChance::searchInCache(const nautilus::val<Timestamp>& timestamp)
{
    /// We assume that a timestamp is in the cache, if the timestamp is in the range of the slice, e.g., sliceStart <= timestamp < sliceEnd.
    for (nautilus::val<uint64_t> i = 0; i < numberOfEntries; ++i)
    {
        // Ask PMG if it is not fine that I use auto. But then I can not use .get() as nautilus::val<Timestamp::Underlying&> sliceEnd
        nautilus::val<SliceCacheEntry*> sliceCacheEntry = startOfEntries + i;
        const auto sliceStart = nautilus::val<Timestamp>{sliceCacheEntry.get(&SliceCacheEntry::sliceStart)};
        const auto sliceEnd = nautilus::val<Timestamp>{sliceCacheEntry.get(&SliceCacheEntry::sliceEnd)};
        if (sliceStart <= timestamp && timestamp < sliceEnd)
        {
            return {i, true};
        }
    }
    return {0, false};
}

nautilus::val<int8_t*>
SliceCacheSecondChance::getDataStructureRef(const nautilus::val<Timestamp>& timestamp, const SliceCacheReplaceEntry& replaceEntry)
{
    /// First, we check if the timestamp is already in the cache.
    if (const auto [pos, foundInCache] = searchInCache(timestamp); foundInCache)
    {
        // not the nicest looking code
        nautilus::val<SliceCacheEntrySecondChance*> sliceCacheEntryToReplace = startOfEntries + pos;
        sliceCacheEntryToReplace.set(&SliceCacheEntrySecondChance::secondChanceBit, true);
        return sliceCacheEntryToReplace.get(&SliceCacheEntry::dataStructure);
    }

    /// If this is not the case, we iterate through the cache until we have find a slice that has the second chance bit set to false.
    /// If we find such a slice, we set the second chance bit to true, replace the slice and return the data structure.
    /// We must start at the current replacement index, as we have to replace the oldest entry.
    for (nautilus::val<uint64_t> i = 0; i < 2 * numberOfEntries; ++i)
    {
        nautilus::val<SliceCacheEntrySecondChance*> sliceCacheEntryToReplace = startOfEntries + replacementIndex;
        const nautilus::val<bool> secondChanceBit = sliceCacheEntryToReplace.get(&SliceCacheEntrySecondChance::secondChanceBit);
        if (secondChanceBit == nautilus::val<bool>{false})
        {
            break;
        }
        sliceCacheEntryToReplace.set(&SliceCacheEntrySecondChance::secondChanceBit, false);
        replacementIndex = (replacementIndex + 1) % numberOfEntries;
    }

    /// Replacing the slice and returning the data structure.
    nautilus::val<SliceCacheEntrySecondChance*> sliceCacheEntryToReplace = startOfEntries + replacementIndex;
    sliceCacheEntryToReplace.set(&SliceCacheEntrySecondChance::secondChanceBit, true);
    replaceEntry(nautilus::val<SliceCacheEntry*>{startOfEntries + replacementIndex});
    return nautilus::val<SliceCacheEntry*>{startOfEntries + replacementIndex}.get(&SliceCacheEntry::dataStructure);
    // talk with PMG, as it is not possible to use sliceCacheEntryToReplace.set(&SliceCacheEntrySecondChance::sliceStart, newCacheEntry.get(&SliceCacheEntrySecondChance::sliceStart))
}

}
