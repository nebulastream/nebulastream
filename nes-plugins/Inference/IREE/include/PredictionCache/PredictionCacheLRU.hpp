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
struct PredictionCacheEntryLRU : PredictionCacheEntry
{
    /// Stores the age of each entry in the cache. With 32-bits, we can store 4 billion entries.
    uint64_t ageBit;
    ~PredictionCacheEntryLRU() override = default;
};

class PredictionCacheLRU final : public PredictionCache
{
public:
    PredictionCacheLRU(
        const nautilus::val<OperatorHandler*>& operatorHandler,
        const uint64_t numberOfEntries,
        const uint64_t sizeOfEntry,
        const nautilus::val<int8_t*>& startOfEntries,
        const nautilus::val<uint64_t*>& hitsRef,
        const nautilus::val<uint64_t*>& missesRef,
        const nautilus::val<size_t>& inputSize);
    ~PredictionCacheLRU() override = default;
    nautilus::val<int8_t*> getDataStructureRef(
        const nautilus::val<std::byte*>& record, const PredictionCache::PredictionCacheReplacement& replacementFunction) override;

private:
    nautilus::val<uint64_t*> getAgeBit(const nautilus::val<uint64_t>& pos);
};
}
