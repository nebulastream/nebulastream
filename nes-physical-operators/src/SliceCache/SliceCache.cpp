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
#include <SliceCache/SliceCacheNone.hpp>
#include <SliceCache/SliceCacheSecondChance.hpp>

namespace NES
{
SliceCache::SliceCache(const uint64_t numberOfEntries, const uint64_t sizeOfEntry)
    : startOfEntries(nullptr), numberOfEntries(numberOfEntries), sizeOfEntry(sizeOfEntry)
{
}

std::unique_ptr<SliceCache> SliceCache::createSliceCache(const SliceCacheConfiguration& sliceCacheConfiguration)
{
    if (sliceCacheConfiguration.enableSliceCache.getValue())
    {
        return std::make_unique<SliceCacheSecondChance>(sliceCacheConfiguration.numberOfEntries.getValue(), sizeof(SliceCacheEntrySecondChance));
    }
    return std::make_unique<SliceCacheNone>();
}

nautilus::val<Timestamp> SliceCache::getSliceStart(const nautilus::val<uint64_t>& pos)
{
    const auto sliceCacheEntry = startOfEntries + pos * sizeOfEntry;
    const auto timestampRef = getMemberRef(sliceCacheEntry, &SliceCacheEntry::sliceStart);
    const auto sliceStart = readValueFromMemRef<Timestamp::Underlying>(timestampRef);
    return nautilus::val<Timestamp>{sliceStart};
}

nautilus::val<Timestamp> SliceCache::getSliceEnd(const nautilus::val<uint64_t>& pos)
{
    const auto sliceCacheEntry = startOfEntries + pos * sizeOfEntry;
    const auto timestampRef = getMemberRef(sliceCacheEntry, &SliceCacheEntry::sliceEnd);
    const auto sliceEnd = readValueFromMemRef<Timestamp::Underlying>(timestampRef);
    return nautilus::val<Timestamp>{sliceEnd};
}

void SliceCache::setStartOfEntries(const nautilus::val<int8_t*>& startOfEntries)
{
    this->startOfEntries = startOfEntries;
}

uint64_t SliceCache::getCacheMemorySize() const
{
    return numberOfEntries * sizeOfEntry;
}

nautilus::val<int8_t*> SliceCache::getDataStructure(const nautilus::val<uint64_t>& pos)
{
    const auto sliceEntry = startOfEntries + pos * sizeOfEntry;
    const auto dataStructureRef = getMemberRef(sliceEntry, &SliceCacheEntry::dataStructure);
    auto dataStructure = readValueFromMemRef<int8_t*>(dataStructureRef);
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
    return nautilus::val<uint64_t>{NOT_FOUND};
}
}
