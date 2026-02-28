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
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Nautilus/Util.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <SliceCacheConfiguration.hpp>

namespace NES
{


/// Represents the C++ struct that is stored in the operator handler vector
struct SliceCacheEntry
{
    /// As we are doing everything in Nautilus, we do not care about the initialization of these values
    SliceCacheEntry() : sliceStart(0), sliceEnd(0), dataStructure(nullptr) { }

    virtual ~SliceCacheEntry() = default;
    Timestamp sliceStart;
    Timestamp sliceEnd;
    int8_t* dataStructure;
};

class SliceCache
{
public:
    explicit SliceCache(uint64_t numberOfEntries, uint64_t sizeOfEntry);
    virtual ~SliceCache() = default;

    static std::unique_ptr<SliceCache> createSliceCache(const SliceCacheConfiguration& sliceCacheConfiguration);

    // not happy with the naming of const SliceCacheReplacement& addNewItem
    using SliceCacheReplacement = std::function<nautilus::val<int8_t*>()>;
    virtual nautilus::val<int8_t*> getDataStructureRef(const nautilus::val<Timestamp>& timestamp, const SliceCacheReplacement& newCacheItem)
        = 0;

    nautilus::val<Timestamp> getSliceStart(const nautilus::val<uint64_t>& pos);
    nautilus::val<Timestamp> getSliceEnd(const nautilus::val<uint64_t>& pos);
    void setStartOfEntries(const nautilus::val<int8_t*>& startOfEntries);
    uint64_t getCacheMemorySize() const;

protected:
    virtual nautilus::val<int8_t*> getDataStructure(const nautilus::val<uint64_t>& pos);

    /// Helper function to search for a timestamp in the cache. If the timestamp is found, the position is returned, otherwise, we return UINT64_MAX
    /// Due to nautilus not supporting optional or expect.
    static constexpr uint64_t NOT_FOUND = UINT64_MAX;
    nautilus::val<uint64_t> searchInCache(const nautilus::val<Timestamp>& timestamp);

    /// Helper function to check if a timestamp is in the cache. We assume a timestamp is in the cache if it is in the range [sliceStart, sliceEnd).
    nautilus::val<bool> foundSlice(const nautilus::val<uint64_t>& pos, const nautilus::val<Timestamp>& timestamp);

    /// Members for iterating over the cache
    nautilus::val<int8_t*> startOfEntries;
    uint64_t numberOfEntries;
    uint64_t sizeOfEntry;
};
}
