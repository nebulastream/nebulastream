/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Util/Logger.hpp>
#include <Windowing/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <Windowing/Watermark/WatermarkProcessor.hpp>
namespace NES::Windowing {

MultiOriginWatermarkProcessor::MultiOriginWatermarkProcessor(const uint64_t numberOfOrigins) : numberOfOrigins(numberOfOrigins) {}

std::shared_ptr<MultiOriginWatermarkProcessor> MultiOriginWatermarkProcessor::create(const uint64_t numberOfOrigins) {
    return std::make_shared<MultiOriginWatermarkProcessor>(numberOfOrigins);
}

void MultiOriginWatermarkProcessor::updateWatermark(WatermarkTs ts, SequenceNumber sequenceNumber, OriginId origenId) {
    std::unique_lock lock(watermarkLatch);
    // insert new local watermark processor if the id is not present in the map
    if (localWatermarkProcessor.find(origenId) == localWatermarkProcessor.end()) {
        localWatermarkProcessor[origenId] = std::make_unique<WatermarkProcessor>();
    }
    NES_ASSERT2_FMT(localWatermarkProcessor.size() <= numberOfOrigins,
                    "The watermark processor maintains watermarks from " << localWatermarkProcessor.size()
                                                                         << " origins but we only expected  " << numberOfOrigins);
    localWatermarkProcessor[origenId]->updateWatermark(ts, sequenceNumber);
}

WatermarkTs MultiOriginWatermarkProcessor::getCurrentWatermark() const {
    std::unique_lock lock(watermarkLatch);
    // check if we already registered each expected origin in the local watermark processor map
    if (localWatermarkProcessor.size() != numberOfOrigins) {
        return 0;
    }
    WatermarkTs maxWatermarkTs = UINT64_MAX;
    for (const auto& localWatermarkManager : localWatermarkProcessor) {
        maxWatermarkTs = std::min(maxWatermarkTs, localWatermarkManager.second->getCurrentWatermark());
    }
    return maxWatermarkTs;
}

}// namespace NES::Windowing