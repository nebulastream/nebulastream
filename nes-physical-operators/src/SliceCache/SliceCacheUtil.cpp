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


#include <SliceCache/SliceCacheUtil.hpp>

#include <SliceCache/SliceCache2Q.hpp>
#include <SliceCache/SliceCacheFIFO.hpp>
#include <SliceCache/SliceCacheLRU.hpp>
#include <SliceCache/SliceCacheSecondChance.hpp>


namespace NES::Util
{
std::unique_ptr<SliceCache> createSliceCache(
    const Configurations::SliceCacheOptions& sliceCacheOptions,
    nautilus::val<OperatorHandler*> globalOperatorHandler,
    const nautilus::val<int8_t*>& startOfSliceEntries)
{
    /// Calculating the correct starting memory refs
    const auto hitsRef = startOfSliceEntries;
    const auto missesRef = hitsRef + nautilus::val<uint64_t>(sizeof(uint64_t));
    const auto sliceCacheEntries = startOfSliceEntries + nautilus::val<uint64_t>(sizeof(HitsAndMisses));
    const auto startOfDataEntry = nautilus::invoke(
        +[](const SliceCacheEntry* sliceCacheEntry)
        {
            const auto startOfDataStructure = &sliceCacheEntry->dataStructure;
            return startOfDataStructure;
        },
        sliceCacheEntries);


    switch (sliceCacheOptions.sliceCacheType)
    {
        case NES::Configurations::SliceCacheType::NONE:
            throw InvalidConfigParameter("SliceCacheType is None");
        case NES::Configurations::SliceCacheType::FIFO:
            return std::make_unique<SliceCacheFIFO>(
                globalOperatorHandler,
                sliceCacheOptions.numberOfEntries,
                sizeof(SliceCacheEntryFIFO),
                sliceCacheEntries,
                hitsRef,
                missesRef);
        case NES::Configurations::SliceCacheType::LRU:
            return std::make_unique<SliceCacheLRU>(
                globalOperatorHandler,
                sliceCacheOptions.numberOfEntries,
                sizeof(SliceCacheEntryLRU),
                sliceCacheEntries,
                hitsRef,
                missesRef);
        case NES::Configurations::SliceCacheType::SECOND_CHANCE:
            return std::make_unique<SliceCacheSecondChance>(
                globalOperatorHandler,
                sliceCacheOptions.numberOfEntries,
                sizeof(SliceCacheEntrySecondChance),
                sliceCacheEntries,
                hitsRef,
                missesRef);
        case NES::Configurations::SliceCacheType::TWO_QUEUES: {
            /// We want to have a minimum size of one for both queues
            if (sliceCacheOptions.numberOfEntries < 2)
            {
                throw InvalidConfigParameter("We require at least a cache minimum size of 2");
            }
            constexpr auto lruQueueSizePercentage = 0.2;
            const auto lruQueueSize
                = static_cast<uint64_t>(std::ceil((sliceCacheOptions.numberOfEntries - 2) * lruQueueSizePercentage)) + 1;
            const auto fifoQueueSize = sliceCacheOptions.numberOfEntries - lruQueueSize;

            if (lruQueueSize <= 0 or fifoQueueSize <= 0)
            {
                throw InvalidConfigParameter("We require a least one space for the LRU and one for the fifo!");
            }

            const auto startOfLRUEntries = sliceCacheEntries;
            const nautilus::val<uint64_t> fifoQueueStartOffset = lruQueueSize * sizeof(SliceCacheEntry2Q);
            const auto startOfFifoEntries = startOfLRUEntries + fifoQueueStartOffset;

            return std::make_unique<SliceCache2Q>(
                globalOperatorHandler,
                sliceCacheOptions.numberOfEntries,
                sizeof(SliceCacheEntry2Q),
                sliceCacheEntries,
                hitsRef,
                missesRef,
                startOfFifoEntries,
                startOfLRUEntries,
                fifoQueueSize,
                lruQueueSize);
        }
    }
    std::unreachable();
}

}
