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

#include <NodeEngine/Transactional/WatermarkProcessor.hpp>
#include <Util/Logger.hpp>
namespace NES::NodeEngine::Transactional {

WatermarkProcessor::WatermarkProcessor(const uint64_t numberOfOrigins) : numberOfOrigins(numberOfOrigins) {
    for (int i = 0; i < numberOfOrigins; i++) {
        auto localWatermarkManager = std::make_unique<LocalWatermarkUpdater>();
        watermarkManagers.emplace_back(std::move(localWatermarkManager));
    }
}

std::shared_ptr<WatermarkProcessor> WatermarkProcessor::create(const uint64_t numberOfOrigins) {
    return std::make_shared<WatermarkProcessor>(numberOfOrigins);
}

void WatermarkProcessor::updateWatermark(WatermarkBarrier watermarkBarrier) {
    auto origenId = watermarkBarrier.getOrigin();
    NES_ASSERT2_FMT(numberOfOrigins <= watermarkManagers.size(),
                    "This origin is not valid: " << origenId << " max origins " << numberOfOrigins);
    watermarkManagers[origenId]->updateWatermark(watermarkBarrier);
}

WatermarkTs WatermarkProcessor::getCurrentWatermark() {
    WatermarkTs maxWatermarkTs = UINT64_MAX;
    for (auto& localWatermarkManager : watermarkManagers) {
        maxWatermarkTs = std::min(maxWatermarkTs, localWatermarkManager->getCurrentWatermark());
    }
    return maxWatermarkTs;
}

}// namespace NES::NodeEngine::Transactional