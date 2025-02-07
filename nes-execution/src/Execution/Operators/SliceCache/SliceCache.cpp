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

#include <Execution/Operators/SliceCache/SliceCache.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>

namespace NES::Runtime::Execution
{
SliceCache::SliceCache(
    const uint64_t numberOfEntries,
    const uint64_t sizeOfEntry,
    const nautilus::val<int8_t*>& startOfEntries,
    const nautilus::val<int8_t*>& startOfDataEntry)
    : startOfEntries(startOfEntries)
    , startOfDataEntry(startOfDataEntry)
    , numberOfEntries(numberOfEntries)
    , sizeOfEntry(sizeOfEntry)
    , sliceStart(Timestamp::INVALID_VALUE)
    , sliceEnd(Timestamp::INVALID_VALUE)
    , numberOfHits(0)
    , numberOfMisses(0)
{
}

nautilus::val<Timestamp> SliceCache::getSliceStart(const nautilus::val<uint64_t>& pos)
{
    const auto sliceCacheEntry = startOfEntries + pos * sizeOfEntry;
    const auto timestampRef = Nautilus::Util::getMemberRef(sliceCacheEntry, &SliceCacheEntry::sliceStart);
    const auto sliceStart = Nautilus::Util::readValueFromMemRef<Timestamp::Underlying>(timestampRef);
    return sliceStart;
}

nautilus::val<Timestamp> SliceCache::getSliceEnd(const nautilus::val<uint64_t>& pos)
{
    const auto sliceCacheEntry = startOfEntries + pos * sizeOfEntry;
    const auto timestampRef = Nautilus::Util::getMemberRef(sliceCacheEntry, &SliceCacheEntry::sliceEnd);
    const auto sliceEnd = Nautilus::Util::readValueFromMemRef<Timestamp::Underlying>(timestampRef);
    return sliceEnd;
}

nautilus::val<int8_t*> SliceCache::getDataStructure(const nautilus::val<uint64_t>& pos)
{
    // const auto dataStructure = startOfDataEntry + pos * sizeOfEntry;
    const auto sliceEntry = startOfEntries + pos * sizeOfEntry;
    const auto dataStructure
        = nautilus::invoke(+[](const SliceCacheEntry* sliceCacheEntry) { return sliceCacheEntry->dataStructure; }, sliceEntry);
    return dataStructure;
}

nautilus::val<int8_t*> SliceCache::searchInCache(const nautilus::val<Timestamp>& timestamp)
{
    /// We assume that a timestamp is in the cache, if the timestamp is in the range of the slice, e.g., sliceStart <= timestamp < sliceEnd.
    for (nautilus::val<uint64_t> i = 0; i < numberOfEntries; i = i + 1)
    {
        sliceStart = getSliceStart(i);
        sliceEnd = getSliceEnd(i);
        if (sliceStart <= timestamp && timestamp < sliceEnd)
        {
            ++numberOfHits;
            return getDataStructure(i);
        }
    }
    return nullptr;
}
}