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

#include <SliceCache/SliceCache2Q.hpp>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <nautilus/std/cstring.h>

namespace NES
{
SliceCache2Q::SliceCache2Q(
    const nautilus::val<OperatorHandler*>& operatorHandler,
    const uint64_t numberOfEntries,
    const uint64_t sizeOfEntry,
    const nautilus::val<int8_t*>& startOfEntries,
    const nautilus::val<uint64_t*>& hitsRef,
    const nautilus::val<uint64_t*>& missesRef,
    const nautilus::val<int8_t*>& startOfFifoEntries,
    const nautilus::val<int8_t*>& startOfLRUEntries,
    const uint64_t fifoQueueSize,
    const uint64_t lruQueueSize)
    : SliceCache(operatorHandler, numberOfEntries, sizeOfEntry, startOfEntries, hitsRef, missesRef)
    , fifoQueueSize(fifoQueueSize)
    , lruQueueSize(lruQueueSize)
    , startOfFifoEntries(startOfFifoEntries)
    , startOfLRUEntries(startOfLRUEntries)
    , fifoReplacementIndex(0)
{
}

void SliceCache2Q::moveSliceCacheEntryToLRUQueue(const nautilus::val<uint64_t>& fifoPos, const nautilus::val<uint64_t>& lruPos)
{
    /// Moving by copying the data from the fifoPos to the lruPos
    const auto fifoPosRef = startOfFifoEntries + (fifoPos * sizeOfEntry);
    const auto lruPosRef = startOfLRUEntries + (lruPos * sizeOfEntry);
    nautilus::memcpy(lruPosRef, fifoPosRef, sizeOfEntry);

    /// Resetting the age bit
    auto ageBit = getAgeBit(lruPos);
    *ageBit = nautilus::val<uint64_t>(0);
}

nautilus::val<int8_t*>
SliceCache2Q::getDataStructureRef(const nautilus::val<Timestamp>& timestamp, const SliceCache::SliceCacheReplacement& replacementFunction)
{
    /// First, we have to increment all age bits by one.
    nautilus::val<uint64_t> maxAge = 0;
    nautilus::val<uint64_t> maxAgeIndex = 0;
    for (nautilus::val<uint64_t> i = 0; i < lruQueueSize; ++i)
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


    /// Second, we check if the timestamp is already in the lru queue. If this is the case, we reset its age bit
    for (nautilus::val<uint64_t> i = 0; i < lruQueueSize; i = i + 1)
    {
        /// We assume that a timestamp is in the cache, if the timestamp is in the range of the slice, e.g., sliceStart <= timestamp < sliceEnd.
        if (foundSlice(i, timestamp))
        {
            incrementNumberOfHits();
            auto ageBit = getAgeBit(i);
            const auto newAgeBit = nautilus::val<uint64_t>(0);
            *ageBit = newAgeBit;
            return getDataStructure(i);
        }
    }

    /// Third, we check if the timestamp is already in the fifo queue. We start our search after the last position belonging to the LRU queue.
    /// If we find it in the fifo queue, we move it into the LRU queue
    for (nautilus::val<uint64_t> i = lruQueueSize; i < lruQueueSize + fifoQueueSize; i = i + 1)
    {
        /// We assume that a timestamp is in the cache, if the timestamp is in the range of the slice, e.g., sliceStart <= timestamp < sliceEnd.
        if (foundSlice(i, timestamp))
        {
            incrementNumberOfHits();
            moveSliceCacheEntryToLRUQueue(i, maxAgeIndex);
            return getDataStructure(i);
        }
    }

    /// Fourth, the timestamp is in neither cache. Thus, we need to insert it into the FIFO queue.
    /// If the timestamp is not in the cache, we have a cache miss.
    incrementNumberOfMisses();
    const nautilus::val<uint64_t> fifoReplacementOffset = fifoReplacementIndex * sizeOfEntry;
    const nautilus::val<SliceCacheEntry*> sliceCacheEntryToReplace = startOfFifoEntries + fifoReplacementOffset;
    const auto dataStructure = replacementFunction(sliceCacheEntryToReplace, lruQueueSize + fifoReplacementIndex);

    /// Before returning the data structure, we need to update the replacement index.
    fifoReplacementIndex = (fifoReplacementIndex + 1) % fifoQueueSize;
    return dataStructure;
}

nautilus::val<uint64_t*> SliceCache2Q::getAgeBit(const nautilus::val<uint64_t>& pos)
{
    const auto sliceCacheEntry = startOfLRUEntries + pos * sizeOfEntry;
    const auto ageBitRef = Nautilus::Util::getMemberRef(sliceCacheEntry, &SliceCacheEntry2Q::ageBit);
    return ageBitRef;
}
}
