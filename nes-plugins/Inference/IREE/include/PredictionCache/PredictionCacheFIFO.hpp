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

#include <PredictionCache/PredictionCache.hpp>

namespace NES
{
struct PredictionCacheEntryFIFO : PredictionCacheEntry
{
    PredictionCacheEntryFIFO() = default;

    PredictionCacheEntryFIFO(const PredictionCacheEntryFIFO& other)
        : PredictionCacheEntry(other) {}

    PredictionCacheEntryFIFO&
    operator=(const PredictionCacheEntryFIFO& other)
    {
        if (this == &other) return *this;

        PredictionCacheEntry::operator=(other);
        return *this;
    }

    PredictionCacheEntryFIFO(PredictionCacheEntryFIFO&& other) noexcept
        : PredictionCacheEntry(std::move(other)) {}

    PredictionCacheEntryFIFO&
    operator=(PredictionCacheEntryFIFO&& other) noexcept
    {
        if (this == &other) return *this;

        PredictionCacheEntry::operator=(std::move(other));
        return *this;
    }

    ~PredictionCacheEntryFIFO() override = default;
};

class PredictionCacheFIFO : public PredictionCache
{
public:
    PredictionCacheFIFO(
        const nautilus::val<OperatorHandler*>& operatorHandler,
        const uint64_t numberOfEntries,
        const uint64_t sizeOfEntry,
        const nautilus::val<int8_t*>& startOfEntries,
        const nautilus::val<uint64_t*>& hitsRef,
        const nautilus::val<uint64_t*>& missesRef,
        const nautilus::val<size_t>& inputSize);
    ~PredictionCacheFIFO() override = default;
    nautilus::val<std::byte*>
    getDataStructureRef(const nautilus::val<std::byte*>& record, const PredictionCache::PredictionCacheReplacement& replacementFunction) override;
    nautilus::val<uint64_t> updateKeys(const nautilus::val<std::byte*>& record, const PredictionCache::PredictionCacheUpdate& updateFunction) override;
    void updateValues(const PredictionCache::PredictionCacheUpdate& updateFunction) override;

protected:
    /// Stores the index of the entry that should be replaced next
    nautilus::val<uint64_t> replacementIndex;
};
}
