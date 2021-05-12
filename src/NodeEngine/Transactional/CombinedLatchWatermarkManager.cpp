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
#include <NodeEngine/Transactional/CombinedLatchWatermarkManager.hpp>
namespace NES::NodeEngine::Transactional {

CombinedLatchWatermarkManager::CombinedLatchWatermarkManager(uint64_t numberOfOrigins)
    : numberOfOrigins(numberOfOrigins), lastTransactionId(0), origins(numberOfOrigins) {
    for (auto i = 0; i < numberOfOrigins; i++) {
        origins[i] = std::make_tuple(0, 0);
    }
    lastTransactionId = 0;
}

WatermarkManagerPtr CombinedLatchWatermarkManager::create(uint64_t numberOfOrigins) {
    return std::make_shared<CombinedLatchWatermarkManager>(numberOfOrigins);
}

CombinedLatchWatermarkManager::Update::Update(TransactionId transactionId, WatermarkTs watermark)
    : transactionId(transactionId), watermark(watermark) {}

void CombinedLatchWatermarkManager::updateWatermark(TransactionId& transactionId, WatermarkTs watermarkTs) {
    std::scoped_lock lock(watermarkLatch);

    // emplace current watermark and transaction im queue of outstanding updates
    updateLog.emplace(transactionId, watermarkTs);
    // pull all outstanding updates from the queue
    while (!updateLog.empty()) {
        auto nextUpdate = updateLog.top();
        // outstanding updates is sorted by the transaction id.
        // Thus, we only check if the next update is the one, which we expect.
        if (lastTransactionId + 1 != nextUpdate.transactionId.id) {
            // It is not the correct update, so we terminate here and can't further apply the next transaction.
            break;
        }
        // apply the current update
        auto originId = nextUpdate.transactionId.originId;
        this->origins[originId] = std::make_tuple(nextUpdate.watermark, nextUpdate.transactionId.id);
        this->lastTransactionId = nextUpdate.transactionId.id;
        updateLog.pop();
    }
}

WatermarkTs CombinedLatchWatermarkManager::getCurrentWatermark(TransactionId&) {
    std::scoped_lock lock(watermarkLatch);
    WatermarkTs maxWatermarkTs = UINT64_MAX;
    for (auto origin : origins) {
        maxWatermarkTs = std::min(maxWatermarkTs, std::get<0>(origin));
    }
    return maxWatermarkTs;
}

}// namespace NES::NodeEngine::Transactional