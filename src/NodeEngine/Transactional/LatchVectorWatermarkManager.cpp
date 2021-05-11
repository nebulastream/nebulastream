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
#include <NodeEngine/Transactional/LatchVectorWatermarkManager.hpp>
namespace NES::NodeEngine::Transactional {

LatchVectorWatermarkManager::LatchVectorWatermarkManager(uint64_t numberOfOrigins, uint64_t numberOfThreads)
    : numberOfOrigins(numberOfOrigins), numberOfThreads(numberOfThreads),
      individualWatermarks(numberOfOrigins * numberOfThreads) {}

WatermarkManagerPtr LatchVectorWatermarkManager::create(uint64_t numberOfOrigins, uint64_t numberOfThreads) {
    return std::make_shared<LatchVectorWatermarkManager>(numberOfOrigins, numberOfThreads);
}

void LatchVectorWatermarkManager::updateWatermark(TransactionId& transactionId, WatermarkTs watermarkTs) {
    std::scoped_lock lock(watermarkLatch);
    auto watermarkIndex = (transactionId.originId * numberOfOrigins) + transactionId.threadId;
    individualWatermarks[watermarkIndex] = watermarkTs;
}

WatermarkTs LatchVectorWatermarkManager::getCurrentWatermark(TransactionId&) {
    std::scoped_lock lock(watermarkLatch);
    WatermarkTs maxWatermarkTs = UINT64_MAX;
    for (auto currentWatermark : individualWatermarks) {
        maxWatermarkTs = std::min(maxWatermarkTs, currentWatermark);
    }
    return maxWatermarkTs;
}
void LatchVectorWatermarkManager::update(TransactionId&) {

}

}// namespace NES::NodeEngine::Transactional