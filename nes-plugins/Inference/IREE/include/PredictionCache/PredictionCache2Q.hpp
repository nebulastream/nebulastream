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
struct PredictionCacheEntry2Q : PredictionCacheEntry
{
    /// Stores the age of each entry in the cache. With 32-bits, we can store 4 billion entries.
    uint64_t ageBit;
    ~PredictionCacheEntry2Q() override = default;
};

/// This slice cache uses the replacement algorithm of "2Q: A Low Overhead High Performance Buffer Management Replacement Algorithm" by Johnson et al.
/// Simplified it contains an LRU and a FIFO queue. In the memory area of the slice cache, we first store the LRU queue and then the FIFO queue.
class PredictionCache2Q final : public PredictionCache
{
public:
    PredictionCache2Q(
        const nautilus::val<OperatorHandler*>& operatorHandler,
        const uint64_t numberOfEntries,
        const uint64_t sizeOfEntry,
        const nautilus::val<int8_t*>& startOfEntries,
        const nautilus::val<uint64_t*>& hitsRef,
        const nautilus::val<uint64_t*>& missesRef,
        const nautilus::val<int8_t*>& startOfFifoEntries,
        const nautilus::val<int8_t*>& startOfLRUEntries,
        const uint64_t fifoQueueSize,
        const uint64_t lruQueueSize,
        const nautilus::val<size_t>& inputSize);
    ~PredictionCache2Q() override = default;
    nautilus::val<int8_t*> getDataStructureRef(
        const nautilus::val<std::byte*>& record, const PredictionCache::PredictionCacheReplacement& replacementFunction) override;

private:
    /// Moves the entry from fifoPos to lruPos and sets the age bit to 0
    void movePredictionCacheEntryToLRUQueue(const nautilus::val<uint64_t>& fifoPos, const nautilus::val<uint64_t>& lruPos);
    nautilus::val<uint64_t*> getAgeBit(const nautilus::val<uint64_t>& pos);

    // todo add docs to the members
    nautilus::val<uint64_t> fifoQueueSize;
    nautilus::val<uint64_t> lruQueueSize;
    nautilus::val<int8_t*> startOfFifoEntries;
    nautilus::val<int8_t*> startOfLRUEntries;
    nautilus::val<uint64_t> fifoReplacementIndex;
};

}
