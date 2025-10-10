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

#include <PredictionCache/PredictionCache2Q.hpp>
#include <PredictionCache/PredictionCacheFIFO.hpp>
#include <PredictionCache/PredictionCacheLFU.hpp>
#include <PredictionCache/PredictionCacheLRU.hpp>
#include <PredictionCache/PredictionCacheSecondChance.hpp>
#include <PredictionCache/PredictionCacheUtil.hpp>

namespace NES::Util
{
std::unique_ptr<PredictionCache> createPredictionCache(
    const Configurations::PredictionCacheOptions& predictionCacheOptions,
    nautilus::val<OperatorHandler*> globalOperatorHandler,
    const nautilus::val<int8_t*>& startOfEntries,
    const nautilus::val<size_t>& inputSize)
{
    /// Calculating the correct starting memory refs
    const auto hitsRef = startOfEntries;
    const auto missesRef = hitsRef + nautilus::val<uint64_t>(sizeof(uint64_t));
    const auto predictionCacheEntries = startOfEntries + nautilus::val<uint64_t>(sizeof(HitsAndMisses));
    const auto startOfDataEntry = nautilus::invoke(
        +[](const PredictionCacheEntry* predictionCacheEntry)
        {
            const auto startOfDataStructure = &predictionCacheEntry->dataStructure;
            return startOfDataStructure;
        },
        predictionCacheEntries);


    switch (predictionCacheOptions.predictionCacheType)
    {
        case NES::Configurations::PredictionCacheType::NONE:
            throw InvalidConfigParameter("PredictionCacheType is None");
        case NES::Configurations::PredictionCacheType::FIFO:
            return std::make_unique<PredictionCacheFIFO>(
                globalOperatorHandler,
                predictionCacheOptions.numberOfEntries,
                sizeof(PredictionCacheEntryFIFO),
                predictionCacheEntries,
                hitsRef,
                missesRef,
                inputSize);
        case NES::Configurations::PredictionCacheType::LFU:
            return std::make_unique<PredictionCacheLFU>(
                globalOperatorHandler,
                predictionCacheOptions.numberOfEntries,
                sizeof(PredictionCacheEntryLFU),
                predictionCacheEntries,
                hitsRef,
                missesRef,
                inputSize);
        case NES::Configurations::PredictionCacheType::LRU:
            return std::make_unique<PredictionCacheLRU>(
                globalOperatorHandler,
                predictionCacheOptions.numberOfEntries,
                sizeof(PredictionCacheEntryLRU),
                predictionCacheEntries,
                hitsRef,
                missesRef,
                inputSize);
        case NES::Configurations::PredictionCacheType::SECOND_CHANCE:
            return std::make_unique<PredictionCacheSecondChance>(
                globalOperatorHandler,
                predictionCacheOptions.numberOfEntries,
                sizeof(PredictionCacheEntrySecondChance),
                predictionCacheEntries,
                hitsRef,
                missesRef,
                inputSize);
        case NES::Configurations::PredictionCacheType::TWO_QUEUES: {
            /// We want to have a minimum size of one for both queues
            if (predictionCacheOptions.numberOfEntries < 2)
            {
                throw InvalidConfigParameter("We require at least a cache minimum size of 2");
            }
            constexpr auto lruQueueSizePercentage = 0.2;
            const auto lruQueueSize
                = static_cast<uint64_t>(std::ceil((predictionCacheOptions.numberOfEntries - 2) * lruQueueSizePercentage)) + 1;
            const auto fifoQueueSize = predictionCacheOptions.numberOfEntries - lruQueueSize;

            if (lruQueueSize <= 0 or fifoQueueSize <= 0)
            {
                throw InvalidConfigParameter("We require a least one space for the LRU and one for the fifo!");
            }

            const auto startOfLRUEntries = predictionCacheEntries;
            const nautilus::val<uint64_t> fifoQueueStartOffset = lruQueueSize * sizeof(PredictionCacheEntry2Q);
            const auto startOfFifoEntries = startOfLRUEntries + fifoQueueStartOffset;

            return std::make_unique<PredictionCache2Q>(
                globalOperatorHandler,
                predictionCacheOptions.numberOfEntries,
                sizeof(PredictionCacheEntry2Q),
                predictionCacheEntries,
                hitsRef,
                missesRef,
                startOfFifoEntries,
                startOfLRUEntries,
                fifoQueueSize,
                lruQueueSize,
                inputSize);
        }
    }
    std::unreachable();
}

}
