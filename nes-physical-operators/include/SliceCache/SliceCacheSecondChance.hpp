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
#include <SliceCache/SliceCache.hpp>

namespace NES
{
struct SliceCacheEntrySecondChance final : SliceCacheEntry
{
    /// Stores the second chance bit for each entry in the cache.
    bool secondChanceBit;
    ~SliceCacheEntrySecondChance() override = default;
};

class SliceCacheSecondChance final : public SliceCache
{
public:
    SliceCacheSecondChance(uint64_t numberOfEntries, uint64_t sizeOfEntry);
    ~SliceCacheSecondChance() override = default;
    nautilus::val<int8_t*>
    getDataStructureRef(const nautilus::val<Timestamp>& timestamp, const SliceCacheReplaceEntry& replaceEntry) override;
    /// Overrides to include space for the replacement index at the end of the cache entries.
    uint64_t getCacheMemorySize() const override;
    /// Overrides to also derive the replacement index pointer from the end of the cache entries.
    void setStartOfEntries(int8_t* startOfEntries) override;

private:
    /// Helper function to search for a timestamp in the cache. If the timestamp is found, the position is returned, otherwise, we return UINT64_MAX
    struct EntryFound
    {
        nautilus::val<uint64_t> pos;
        nautilus::val<bool> foundInCache;
    };

    EntryFound searchInCache(const nautilus::val<SliceCacheEntry*>& startOfEntries, const nautilus::val<Timestamp>& timestamp);

    /// Raw pointer to the replacement index stored after the cache entries.
    /// Like startOfEntriesRaw, stored as raw pointer so val can be created locally during tracing.
    uint64_t* replacementIndexRaw;
};

}
