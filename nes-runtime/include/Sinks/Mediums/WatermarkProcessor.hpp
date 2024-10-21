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

#pragma once

#include <Identifiers/Identifiers.hpp>

#include <atomic>
#include <mutex>
#include <queue>

namespace NES::Windowing
{

/**
 * @brief This class implements a watermark processor for a single origin.
 * It processes all watermark updates from one specific origin and applies all updates in sequential order.
 * @assumptions This watermark processor assumes strictly monotonic update sequence numbers.
 * To handle out of order processing, it stores in flight updates in a transaction log.
 */
class WatermarkProcessor
{
public:
    explicit WatermarkProcessor();

    /**
     * @brief In this implementation, update watermark processes a watermark barrier and applies all
     * outstanding updates from the transaction log.
     * To this end, it leverage the implicit sorting of the priority queue.
     */
    void updateWatermark(Timestamp timestamp, SequenceNumber sequenceNumber);

    Timestamp getCurrentWatermark() const;

    /**
     * @brief Returns success if there are no tuples with smaller sequence number that haven't arrived yet than current seen last tuple
     * @return Success
     */
    bool isWatermarkSynchronized() const;

private:
    struct WatermarkBarrierComparator
    {
        bool operator()(
            std::tuple<Timestamp::Underlying, SequenceNumber::Underlying> const& wb1,
            std::tuple<Timestamp::Underlying, SequenceNumber::Underlying> const& wb2) const
        {
            /// return "true" if "wb1" is ordered before "wb2", for example:
            return std::get<1>(wb1) > std::get<1>(wb2);
        }
    };
    mutable std::mutex watermarkLatch;
    std::atomic<Timestamp::Underlying> currentWatermark = INITIAL_WATERMARK_TS_NUMBER.getRawValue();
    SequenceNumber::Underlying currentSequenceNumber{0};
    /// Use a priority queue to keep track of all in flight transactions.
    std::priority_queue<
        std::tuple<Timestamp::Underlying, SequenceNumber::Underlying>,
        std::vector<std::tuple<Timestamp::Underlying, SequenceNumber::Underlying>>,
        WatermarkBarrierComparator>
        transactionLog;
};

}
