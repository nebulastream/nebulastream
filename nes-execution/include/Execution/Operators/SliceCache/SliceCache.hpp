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

#pragma once
#include <functional>
#include <Execution/Operators/OperatorState.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES::Runtime::Execution
{
/// Represents the C++ struct that is stored in the operator handler vector
struct SliceCacheEntry
{
    SliceCacheEntry(Timestamp slice_start, Timestamp slice_end)
        : sliceStart(std::move(slice_start)), sliceEnd(std::move(slice_end)), dataStructure(nullptr)
    {
    }

    virtual ~SliceCacheEntry() = default;
    Timestamp sliceStart;
    Timestamp sliceEnd;
    int8_t* dataStructure;
};

class SliceCache : public Operators::OperatorState
{
public:
    explicit SliceCache(
        uint64_t numberOfEntries,
        uint64_t sizeOfEntry,
        const nautilus::val<int8_t*>& startOfEntries,
        const nautilus::val<int8_t*>& startOfDataEntry);
    ~SliceCache() override = default;

    using SliceCacheReplacement = std::function<nautilus::val<int8_t*>(const nautilus::val<SliceCacheEntry*>& sliceCacheEntryToReplace)>;

    virtual nautilus::val<int8_t*>
    getDataStructureRef(const nautilus::val<Timestamp>& timestamp, const SliceCache::SliceCacheReplacement& replacementFunction) = 0;

protected:
    virtual nautilus::val<Timestamp> getSliceStart(const nautilus::val<uint64_t>& pos);
    virtual nautilus::val<Timestamp> getSliceEnd(const nautilus::val<uint64_t>& pos);
    virtual nautilus::val<int8_t*> getDataStructure(const nautilus::val<uint64_t>& pos);

    /// Helper function to search for a timestamp in the cache. If the timestamp is found, a pointer to the cache entry is returned. Otherwise, nullptr is returned.
    nautilus::val<int8_t*> searchInCache(const nautilus::val<Timestamp>& timestamp);

    /// Members for iterating over the cache
    nautilus::val<int8_t*> startOfEntries;
    nautilus::val<int8_t*> startOfDataEntry; /// This stores the first data entry in the cache
    nautilus::val<uint64_t> numberOfEntries;
    uint64_t sizeOfEntry;

    /// Members for the current entry. We assume that each entry has a fixed size and the layout is the same as in the SliceCacheEntry struct.
    nautilus::val<Timestamp> sliceStart;
    nautilus::val<Timestamp> sliceEnd;

    /// Statistics of how many hits and misses we have. This is useful to see how efficient the cache is by comparing the hits and misses for a clairvoyant cache.
    nautilus::val<uint64_t> numberOfHits;
    nautilus::val<uint64_t> numberOfMisses;
};
}
