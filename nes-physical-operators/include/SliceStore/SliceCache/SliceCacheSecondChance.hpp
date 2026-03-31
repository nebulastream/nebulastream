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
#include <cstdint>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <SliceStore/SliceCache/SliceCache.hpp>
#include <Time/Timestamp.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
struct SliceCacheEntrySecondChance final : SliceCacheEntry
{
    /// Stores the second chance bit for each entry in the cache.
    bool secondChanceBit;
};

static_assert(std::is_trivially_destructible_v<SliceCacheEntrySecondChance>);

/// Slice Cache that employs the second chance / clock, c.f., https://en.wikipedia.org/wiki/Page_replacement_algorithm#Clock
class SliceCacheSecondChance final : public SliceCache
{
public:
    SliceCacheSecondChance(uint64_t numberOfEntries, uint64_t sizeOfEntry);
    SliceCacheSecondChance(const SliceCacheSecondChance& other);
    ~SliceCacheSecondChance() override = default;
    [[nodiscard]] std::unique_ptr<SliceCache> clone() const override;
    nautilus::val<SliceCacheEntry::DataStructure> getDataStructureRef(
        const nautilus::val<Timestamp>& timestamp,
        const nautilus::val<WorkerThreadId>& workerThreadId,
        const SliceCacheReplaceEntry& replaceEntry) override;
    /// Overrides to include space for the per-thread replacement indices at the end of the cache entries.
    /// Memory layout: [Thread0 entries][Thread1 entries]...[ThreadN entries][Thread0 replIdx][Thread1 replIdx]...[ThreadN replIdx]
    [[nodiscard]] uint64_t getCacheMemorySize() const override;

private:
    /// Helper function to search for a timestamp in the cache. If the timestamp is found, the position is returned, otherwise, we return UINT64_MAX
    struct EntryFound
    {
        nautilus::val<SliceCacheEntrySecondChance*> sliceCacheEntry;
        nautilus::val<bool> foundInCache;
    };

    EntryFound
    searchInCache(const nautilus::val<SliceCacheEntrySecondChance*>& threadLocalStart, const nautilus::val<Timestamp>& timestamp);
};

}
