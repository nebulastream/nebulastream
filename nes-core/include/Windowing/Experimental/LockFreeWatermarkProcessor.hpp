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
 * #TODO add spin lock if the log is full, currently we just fail.
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
template<uint64_t logSize = 100>
class LockFreeWatermarkProcessor {
    struct WatermarkTuple {
      public:
        uint64_t ts;
        uint64_t sequenceNumber;
    };

  public:
    uint64_t updateWatermark(uint64_t ts, uint64_t sequenceNumber) {
        // A new sequence number has to be greater than the currentWatermarkTuple.sequenceNumber.
        assert(sequenceNumber > currentSequenceNumber);

        // We currently assume that the diff between sequence number and current sequence number will not exceed the log size.
        // Otherwise, we would override content in the log
        uint64_t current = currentSequenceNumber.load();
        while (sequenceNumber - current >= logSize) {
            current = currentSequenceNumber.load();
        }
        //assert(sequenceNumber - currentSequenceNumber < logSize);
        /**
        // Check if we got the next valid sequence number.
        // Only a single thread can have a matching sequence number, thus we know that no other thread can access the true case,
        // till we update the currentWatermarkTuple.sequenceNumber
        if (sequenceNumber == currentSequenceNumber + 1) {
            WatermarkTuple nextWatermarkTuple = {ts, sequenceNumber};
            // We first process the log to check if we can actually can apply multiple updates in one step.
            // We apply updates from the log as long as the value contains the next expected sequence number.
            while (readWatermarkTuple(nextWatermarkTuple.sequenceNumber + 1).sequenceNumber
                   == nextWatermarkTuple.sequenceNumber + 1) {
                nextWatermarkTuple = readWatermarkTuple(nextWatermarkTuple.sequenceNumber + 1);
            };
            // Set the watermark ts and sequence number atomically
            currentWatermarkTs = nextWatermarkTuple.ts;
            currentSequenceNumber = nextWatermarkTuple.sequenceNumber;
        } else {
            // The sequence number is not the next expected.
            // So we store it in the correct lock position.
            auto logIndex = getLogIndex(sequenceNumber);
            log[logIndex] = {ts, sequenceNumber};
        }
        */
        auto logIndex = getLogIndex(sequenceNumber);
        log[logIndex] = {ts, sequenceNumber};

        processLog();

        return currentWatermarkTs;
    }

    uint64_t getCurrentWatermark() { return currentWatermarkTs; }

  private:
    void processLog() {
        // get the current sequence number
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
