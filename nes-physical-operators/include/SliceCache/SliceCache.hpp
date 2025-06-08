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
#include <WindowBuildPhysicalOperator.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Nautilus/Util.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES
{
/// Represents the C++ struct that is stored in the operator handler vector
struct SliceCacheEntry
{
    SliceCacheEntry(Timestamp sliceStart, Timestamp sliceEnd)
        : sliceStart(std::move(sliceStart)), sliceEnd(std::move(sliceEnd)), dataStructure(nullptr)
    {
    }

    virtual ~SliceCacheEntry() = default;
    Timestamp sliceStart;
    Timestamp sliceEnd;
    int8_t* dataStructure;
};
/// Represents the C++ struct that is stored in the operator handler vector before all SliceCacheEntry structs
struct HitsAndMisses
{
    uint64_t hits;
    uint64_t misses;
};

class SliceCache : public WindowOperatorBuildLocalState
{
public:
    explicit SliceCache(
        const nautilus::val<OperatorHandler*>& operatorHandler,
        const uint64_t numberOfEntries,
        const uint64_t sizeOfEntry,
        const nautilus::val<int8_t*>& startOfEntries,
        const nautilus::val<uint64_t*>& hitsRef,
        const nautilus::val<uint64_t*>& missesRef);
    ~SliceCache() override = default;

    using SliceCacheReplacement = std::function<nautilus::val<int8_t*>(const nautilus::val<SliceCacheEntry*>& sliceCacheEntryToReplace, const nautilus::val<uint64_t>& replacementIndex)>;
    virtual nautilus::val<int8_t*>
    getDataStructureRef(const nautilus::val<Timestamp>& timestamp, const SliceCache::SliceCacheReplacement& replacementFunction) = 0;

    virtual nautilus::val<Timestamp> getSliceStart(const nautilus::val<uint64_t>& pos);
    virtual nautilus::val<Timestamp> getSliceEnd(const nautilus::val<uint64_t>& pos);
protected:
    virtual nautilus::val<int8_t*> getDataStructure(const nautilus::val<uint64_t>& pos);
    void incrementNumberOfHits();
    void incrementNumberOfMisses();

    /// Helper function to search for a timestamp in the cache. If the timestamp is found, the position is returned, otherwise, we return UINT64_MAX
    static constexpr uint64_t NOT_FOUND = UINT64_MAX;
    nautilus::val<uint64_t> searchInCache(const nautilus::val<Timestamp>& timestamp);

    /// Helper function to check if a timestamp is in the cache. We assume a timestamp is in the cache if it is in the range [sliceStart, sliceEnd).
    nautilus::val<bool> foundSlice(const nautilus::val<uint64_t>& pos, const nautilus::val<Timestamp>& timestamp);

    /// Members for iterating over the cache
    nautilus::val<int8_t*> startOfEntries;
    nautilus::val<uint64_t> numberOfEntries;
    nautilus::val<uint64_t> sizeOfEntry;

private:
    /// Statistics of how many hits and misses we have. This is useful to see how efficient the cache is by comparing the hits and misses for a clairvoyant cache.
    nautilus::val<uint64_t*> numberOfHits;
    nautilus::val<uint64_t*> numberOfMisses;
};
}
