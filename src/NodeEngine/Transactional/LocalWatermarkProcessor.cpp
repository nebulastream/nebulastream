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
#include <NodeEngine/Transactional/LocalWatermarkProcessor.hpp>
namespace NES::NodeEngine::Transactional {

LocalWatermarkProcessor::LocalWatermarkProcessor() : currentWatermark(0), currentTransactionId(0) {}

void LocalWatermarkProcessor::updateWatermark(WatermarkBarrier& watermarkBarrier) {
    std::scoped_lock lock(watermarkLatch);

    // emplace current watermark barrier in the update log
    updateLog.emplace(watermarkBarrier);
    // process all outstanding updates from the queue
    while (!updateLog.empty()) {
        auto nextWatermarkUpdate = updateLog.top();
        // the update log is sorted by the sequence number.
        // Thus, we only check if the next update is the one, which we expect.
        // This implies, that each watermark barrier has to be received.
        // If the system looses a watermark barrier, the watermark processor will make no progress.
        if (currentTransactionId + 1 != nextWatermarkUpdate.getSequenceNumber()) {
            // It is not the correct update, so we terminate here and can't further apply the next transaction.
            break;
        }
        // apply the current update
        this->currentTransactionId = nextWatermarkUpdate.getSequenceNumber();
        this->currentWatermark = nextWatermarkUpdate.getTs();
        updateLog.pop();
    }
}

WatermarkTs LocalWatermarkProcessor::getCurrentWatermark() {
    std::scoped_lock lock(watermarkLatch);
    return currentWatermark;
}

}// namespace NES::NodeEngine::Transactional