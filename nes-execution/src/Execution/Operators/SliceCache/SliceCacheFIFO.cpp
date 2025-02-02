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

#include <cstdint>
#include <Execution/Operators/SliceCache/SliceCacheFIFO.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <nautilus/val.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Execution/Operators/SliceCache/SliceCache.hpp>
#include <nautilus/static.hpp>

namespace NES::Runtime::Execution
{
SliceCacheFIFO::SliceCacheFIFO(const uint64_t numberOfEntries, const uint64_t sizeOfEntry, const nautilus::val<int8_t*>& startOfEntries, const nautilus::val<int8_t*>& startOfDataEntry)
    : SliceCache(numberOfEntries, sizeOfEntry, startOfEntries, startOfDataEntry)
    , replacementIndex(0)
{
}

nautilus::val<int8_t*> SliceCacheFIFO::getDataStructureRef(
    const nautilus::val<Timestamp>& timestamp,
    const SliceCache::SliceCacheReplacement& replacementFunction)
{
    /// First, we check if the timestamp is already in the cache.
    if (const auto dataStructure = SliceCache::searchInCache(timestamp); dataStructure != nullptr)
    {
        return dataStructure;
    }

    /// If the timestamp is not in the cache, we have a cache miss.
    ++numberOfMisses;

    /// As we are in the FIFO cache, we need to replace the oldest entry with the new one.
    const nautilus::val<SliceCacheEntry*> sliceCacheEntryToReplace = startOfEntries + replacementIndex * sizeOfEntry;
    const auto dataStructure = replacementFunction(sliceCacheEntryToReplace);

    /// Before returning the data structure, we need to update the replacement index.
    replacementIndex = (replacementIndex + 1) % numberOfEntries;
    return dataStructure;
}
}