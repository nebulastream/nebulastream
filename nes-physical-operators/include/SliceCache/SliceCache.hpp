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
#include <SliceCacheConfiguration.hpp>
#include <val.hpp>
#include <val_ptr.hpp>
#include <val_std.hpp>

namespace NES
{


/// Represents the C++ struct that is stored in the operator handler vector
struct SliceCacheEntry
{
    /// As we are doing everything in Nautilus, we do not care about the initialization of these values
    // might not be 100% true anymore
    SliceCacheEntry() : sliceStart(0), sliceEnd(0), dataStructure(nullptr) { }

    SliceCacheEntry(const Timestamp::Underlying sliceStart, const Timestamp::Underlying sliceEnd, int8_t* dataStructure)
        : sliceStart(sliceStart), sliceEnd(sliceEnd), dataStructure(dataStructure)
    {
    }

    virtual ~SliceCacheEntry() = default;
    // talk with PMG, if we can not also support classes that have a val<> wrapper
    // problem is also the val<Timestamp&> so the reference stuff
    // Timestamp sliceStart;
    // Timestamp sliceEnd;
    Timestamp::Underlying sliceStart;
    Timestamp::Underlying sliceEnd;
    int8_t* dataStructure;
};

class SliceCache
{
public:
    explicit SliceCache(uint64_t numberOfEntries, uint64_t sizeOfEntry);
    virtual ~SliceCache() = default;
    static std::unique_ptr<SliceCache> createSliceCache(const SliceCacheConfiguration& sliceCacheConfiguration);
    using SliceCacheReplaceEntry = std::function<void(const nautilus::val<SliceCacheEntry*>&)>;
    virtual nautilus::val<int8_t*>
    getDataStructureRef(const nautilus::val<Timestamp>& timestamp, const SliceCacheReplaceEntry& replaceEntry) = 0;
    virtual void setStartOfEntries(SliceCacheEntry* startOfEntries);
    virtual uint64_t getCacheMemorySize() const;

protected:
    /// The pointer to the startOfEntries is constant, as we create the memory once and do not change it during the query runtime
    SliceCacheEntry* startOfEntriesRaw;
    uint64_t numberOfEntries;
    uint64_t sizeOfEntry;
};
}
