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

#ifndef NES_INCLUDE_WINDOWING_WATERMARK_WATERMARKPROCESSOR_HPP_
#define NES_INCLUDE_WINDOWING_WATERMARK_WATERMARKPROCESSOR_HPP_

#include <atomic>
#include <mutex>
#include <queue>

namespace NES {
using WatermarkTs = uint64_t;
using OriginId = uint64_t;
using SequenceNumber = uint64_t;
}// namespace NES

namespace NES::Windowing {

/**
 * @brief This class implements a watermark processor for a single origin.
 * It processes all watermark updates from one specific origin and applies all updates in sequential order.
 * @assumptions This watermark processor assumes strictly monotonic update sequence numbers.
 * To handle out of order processing, it stores in flight updates in a transaction log.
 */
class WatermarkProcessor {
  public:
    explicit WatermarkProcessor();

    /**
     * @brief In this implementation, update watermark processes a watermark barrier and applies all
     * outstanding updates from the transaction log.
     * To this end, it leverage the implicit sorting of the priority queue.
     * @param watermarkBarrier
     */
    void updateWatermark(WatermarkTs ts, SequenceNumber sequenceNumber);

    /**
     * @brief Returns the current watermark.
     * @return WatermarkTs
     */
    WatermarkTs getCurrentWatermark() const;

  private:
    struct WatermarkBarrierComparator {
        bool operator()(std::tuple<WatermarkTs, SequenceNumber> const& wb1, std::tuple<WatermarkTs, SequenceNumber> const& wb2) {
            // return "true" if "wb1" is ordered before "wb2", for example:
            return std::get<1>(wb1) > std::get<1>(wb2);
        }
    };
    mutable std::mutex watermarkLatch;
    std::atomic<WatermarkTs> currentWatermark{0};
    SequenceNumber currentSequenceNumber{0};
    // Use a priority queue to keep track of all in flight transactions.
    std::priority_queue<std::tuple<WatermarkTs, SequenceNumber>,
                        std::vector<std::tuple<WatermarkTs, SequenceNumber>>,
                        WatermarkBarrierComparator>
        transactionLog;
};

}// namespace NES::Windowing

#endif  // NES_INCLUDE_WINDOWING_WATERMARK_WATERMARKPROCESSOR_HPP_
