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

#include <Util/libcuckoo/cuckoohash_map.hh>
#include <algorithm>
#include <assert.h>
#include <atomic>
#include <memory>
#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_LOCKFREEWATERMARKPROCESSOR_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_LOCKFREEWATERMARKPROCESSOR_HPP_

namespace NES::Experimental {
/**
 * @brief Implements a lock free watermark processor for a single origin.
 * It processes all watermark updates from one specific origin and applies all updates in sequential order.
 * @assumptions This watermark processor assumes strictly monotonic update sequence numbers.
 * @assumption This watermark processor maintains a fixed size log of inflight watermark updates.
 *
 * Implementation details:
 * This watermark processor stores in flight updates in a fixed size log, implemented with a cycling buffer.
 * We differentiate two cases:
 *
 * 1. If the sequenceNumber of the watermark updates is the next expected sequence number, i.e., currentWatermarkTuple.sequenceNumber + 1.
 * In this case we can update the currentWatermarkTuple as we received the next update.
 * To this end, we first iterate over the log and apply all updates as log as the sequence number is as expected.
 *
 * 2. If the sequenceNumber is different then the next expected sequence number.
 * In this case, we can't update the currentWatermarkTuple.
 * Instead, we store the update in the correct log position.
 *
 * @tparam logSize
 */
template<uint64_t logSize = 10000>
class LockFreeWatermarkProcessor {
    struct WatermarkTuple {
      public:
        uint64_t ts;
        uint64_t sequenceNumber;
    };

  public:
    /**
     * @brief Updates the watermark timestamp and emits the current watermark.
     * @param ts watermark timestamp
     * @param sequenceNumber
     * @return currentWatermarkTs
     */
    uint64_t updateWatermark(uint64_t ts, uint64_t sequenceNumber) {
        // A new sequence number has to be greater than the currentWatermarkTuple.sequenceNumber.
        assert(sequenceNumber > currentSequenceNumber);
        // If the diff between the sequence number and the current sequence number is >= logSize we have to wait till the log has enough space.
        // Otherwise, we would override content.
        uint64_t current = currentSequenceNumber.load();
        while (sequenceNumber - current >= logSize) {
            current = currentSequenceNumber.load();
        }
        // place the sequence number in the log at its designated position.
        auto logIndex = getLogIndex(sequenceNumber);
        log[logIndex] = {ts, sequenceNumber};
        // process the log
        processLog();
        return currentWatermarkTs;
    }

    uint64_t getCurrentWatermark() { return currentWatermarkTs; }

  private:
    void processLog() {
        // get the current sequence number and apply the update if we have the correct sequence number
        auto sequenceNumber = currentSequenceNumber.load();
        auto watermarkTs = currentWatermarkTs.load();
        auto nextSequenceNumber = sequenceNumber + 1;
        auto nextWatermarkTuple = readWatermarkTuple(nextSequenceNumber);
        while (nextWatermarkTuple.sequenceNumber == nextSequenceNumber) {
            currentSequenceNumber.compare_exchange_strong(sequenceNumber, nextWatermarkTuple.sequenceNumber);
            currentWatermarkTs.compare_exchange_strong(watermarkTs, nextWatermarkTuple.ts);
            sequenceNumber = currentSequenceNumber.load();
            nextSequenceNumber = sequenceNumber + 1;
            nextWatermarkTuple = readWatermarkTuple(nextSequenceNumber);
        };
    }

    WatermarkTuple readWatermarkTuple(uint64_t sequenceNumber) { return log[getLogIndex(sequenceNumber)]; }

    uint64_t getLogIndex(uint64_t sequenceNumber) { return sequenceNumber % logSize; }

    std::array<WatermarkTuple, logSize> log;
    std::atomic<uint64_t> currentSequenceNumber = 0;
    std::atomic<uint64_t> currentWatermarkTs = 0;
};


}// namespace NES::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_LOCKFREEWATERMARKPROCESSOR_HPP_