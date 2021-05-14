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

#ifndef NES_INCLUDE_NODEENGINE_TRANSACTIONAL_LOCALWATERMARKPROCESSOR_HPP_
#define NES_INCLUDE_NODEENGINE_TRANSACTIONAL_LOCALWATERMARKPROCESSOR_HPP_

#include <NodeEngine/Transactional/WatermarkBarrier.hpp>
#include <queue>
#include <mutex>
namespace NES::NodeEngine::Transactional {

/**
 * @brief This class implements a local watermark processor.
 * It processes all WatermarkBarriers from one specific origin and applies all updates in sequential order.
 * @assumptions This watermark processor assumes strictly monotonic WatermarkBarrier sequence numbers.
 * To handle out of order processing, it stores in flight updates in a transaction log.
 */
class LocalWatermarkProcessor {
  public:
    explicit LocalWatermarkProcessor();

    /**
     * @brief In this implementation, update watermark processes a watermark barrier and applies all
     * outstanding updates from the transaction log.
     * To this end, it leverage the implicit sorting of the priority queue.
     * @param watermarkBarrier
     */
    void updateWatermark(WatermarkBarrier& watermarkBarrier);

    /**
     * @brief Returns the current watermark.
     * @return WatermarkTs
     */
    WatermarkTs getCurrentWatermark() const;

  private:
    struct WatermarkBarrierComparator{
        bool operator()(WatermarkBarrier const& wb1, WatermarkBarrier const& wb2) {
            // return "true" if "wb1" is ordered before "wb2", for example:
            return wb1.getSequenceNumber() > wb2.getSequenceNumber();
        }
    };
    mutable std::mutex watermarkLatch;
    WatermarkTs currentWatermark;
    BarrierSequenceNumber currentSequenceNumber;
    // Use a priority queue to keep track of all in flight transactions.
    std::priority_queue<WatermarkBarrier, std::vector<WatermarkBarrier>, WatermarkBarrierComparator> transactionLog;
};

}// namespace NES::NodeEngine::Transactional

#endif//NES_INCLUDE_NODEENGINE_TRANSACTIONAL_LOCALWATERMARKPROCESSOR_HPP_
