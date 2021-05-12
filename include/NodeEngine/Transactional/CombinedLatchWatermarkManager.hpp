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

#ifndef NES_INCLUDE_NODEENGINE_TRANSACTIONAL_COMBINEDLATCHWATERMARKMANAGER_HPP_
#define NES_INCLUDE_NODEENGINE_TRANSACTIONAL_COMBINEDLATCHWATERMARKMANAGER_HPP_

#include <NodeEngine/Transactional/WatermarkManager.hpp>
#include <queue>
namespace NES::NodeEngine::Transactional {

/**
 * @brief This class implements a latch based watermark manager.
 * @assumptions This watermark manager assumes strictly monotonic transaction ids.
 * To handle out of order processing, it maintains a priority queue.
 */
class CombinedLatchWatermarkManager: public WatermarkManager {
  public:
    CombinedLatchWatermarkManager(uint64_t numberOfOrigins);
    static WatermarkManagerPtr create(uint64_t numberOfOrigins);

    /**
     * @brief In this implementation, update watermark adds the watermark ts and transaction as an update to the update queue.
     * Afterwards it applies all outstanding updates from the update queue.
     * To this end, it leverage the implicit sorting of the update queue.
     * @param transactionId
     * @param watermarkTs
     */
    void updateWatermark(TransactionId& transactionId, WatermarkTs watermarkTs) override;

    /**
     * @brief In this implementation, we just return the current watermark.
     * @param transactionId
     * @return WatermarkTs
     */
    WatermarkTs getCurrentWatermark(TransactionId& transactionId) override;

  private:
    class Update {
      public:
        Update(TransactionId transactionId, WatermarkTs watermark);
        TransactionId transactionId;
        WatermarkTs watermark;
    };
    struct CompareUpdates {
        bool operator()(Update const& p1, Update const& p2) {
            // return "true" if "p1" is ordered before "p2", for example:
            return p1.transactionId.id > p2.transactionId.id;
        }
    };
    std::mutex watermarkLatch;
    uint64_t numberOfOrigins;
    uint64_t lastTransactionId;
    std::vector<std::tuple<WatermarkTs, uint64_t>> origins;
    std::priority_queue<Update, std::vector<Update>, CompareUpdates> updateLog;
};

}// namespace NES::NodeEngine::Transactional

#endif//NES_INCLUDE_NODEENGINE_TRANSACTIONAL_COMBINEDLATCHWATERMARKMANAGER_HPP_
