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

#include <SliceCache/SliceCache.hpp>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>

namespace NES
{
SliceCache::SliceCache(
    const nautilus::val<OperatorHandler*>& operatorHandler,
    const uint64_t numberOfEntries,
    const uint64_t sizeOfEntry,
    const nautilus::val<int8_t*>& startOfEntries,
    const nautilus::val<uint64_t*>& hitsRef,
    const nautilus::val<uint64_t*>& missesRef)
    : WindowOperatorBuildLocalState(operatorHandler)
    , startOfEntries(startOfEntries)
    , numberOfEntries(numberOfEntries)
    , sizeOfEntry(sizeOfEntry)
    , numberOfHits(hitsRef)
    , numberOfMisses(missesRef)
{
}

void SliceCache::incrementNumberOfHits()
{
    auto currentNumberOfHits = static_cast<nautilus::val<uint64_t>>(*numberOfHits);
    currentNumberOfHits = currentNumberOfHits + 1;
    *numberOfHits = currentNumberOfHits;
}

void SliceCache::incrementNumberOfMisses()
{
    auto currentNumberOfMisses = static_cast<nautilus::val<uint64_t>>(*numberOfMisses);
    currentNumberOfMisses = currentNumberOfMisses + 1;
    *numberOfMisses = currentNumberOfMisses;
}

nautilus::val<Timestamp> SliceCache::getSliceStart(const nautilus::val<uint64_t>& pos)
{
    const auto sliceCacheEntry = startOfEntries + pos * sizeOfEntry;
    const auto timestampRef = Nautilus::Util::getMemberRef(sliceCacheEntry, &SliceCacheEntry::sliceStart);
    const auto sliceStart = Nautilus::Util::readValueFromMemRef<Timestamp::Underlying>(timestampRef);
    return nautilus::val<Timestamp>(sliceStart);
}

nautilus::val<Timestamp> SliceCache::getSliceEnd(const nautilus::val<uint64_t>& pos)
{
    const auto sliceCacheEntry = startOfEntries + pos * sizeOfEntry;
    const auto timestampRef = Nautilus::Util::getMemberRef(sliceCacheEntry, &SliceCacheEntry::sliceEnd);
    const auto sliceEnd = Nautilus::Util::readValueFromMemRef<Timestamp::Underlying>(timestampRef);
    return nautilus::val<Timestamp>(sliceEnd);
}

nautilus::val<int8_t*> SliceCache::getDataStructure(const nautilus::val<uint64_t>& pos)
{
    const auto sliceEntry = startOfEntries + pos * sizeOfEntry;
    const auto dataStructure
        = nautilus::invoke(+[](const SliceCacheEntry* sliceCacheEntry) { return sliceCacheEntry->dataStructure; }, sliceEntry);
    return dataStructure;
}

nautilus::val<bool> SliceCache::foundSlice(const nautilus::val<uint64_t>& pos, const nautilus::val<Timestamp>& timestamp)
{
    const auto sliceStart = getSliceStart(pos);
    const auto sliceEnd = getSliceEnd(pos);
    if (sliceStart <= timestamp && timestamp < sliceEnd)
    {
        return true;
    }
    return false;
}

nautilus::val<uint64_t> SliceCache::searchInCache(const nautilus::val<Timestamp>& timestamp)
{
    /// We assume that a timestamp is in the cache, if the timestamp is in the range of the slice, e.g., sliceStart <= timestamp < sliceEnd.
    for (nautilus::val<uint64_t> i = 0; i < numberOfEntries; i = i + 1)
    {
        if (foundSlice(i, timestamp))
        {
            return i;
        }
    }
    return nautilus::val<uint64_t>(NOT_FOUND);
}
}
