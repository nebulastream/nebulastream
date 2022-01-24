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

#include <algorithm>
#include <assert.h>
#include <atomic>
#include <memory>
#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_SEQUENCE_LOG_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_SEQUENCE_LOG_HPP_

namespace NES::Experimental {
/**
 * @brief Implements a lock free log of sequence numbers.
 * @tparam logSize
 */
template<uint64_t logSize = 10000>
class SequenceLog {

  public:
    uint64_t append(uint64_t sequenceNumber) {
        // A new sequence number has to be greater than the sequenceNumber.
        assert(sequenceNumber > currentSequenceNumber);
        // If the diff between the sequence number and the current sequence number is >= logSize we have to wait till the log has enough space.
        // Otherwise, we would override content.
        uint64_t current = currentSequenceNumber.load();
        while (sequenceNumber - current >= logSize) {
            current = currentSequenceNumber.load();
            processLog();
        }
        // place the sequence number in the log at its designated position.
        auto logIndex = getLogIndex(sequenceNumber);
        log[logIndex] = sequenceNumber;
        // process the log
        processLog();
        return currentSequenceNumber;
    }

  private:
    void processLog() {
        // get the current sequence number and apply the update if we have the correct sequence number
        auto sequenceNumber = currentSequenceNumber.load();
        auto nextSequenceNumber = sequenceNumber + 1;
        auto nextSequenceNumberInLog = readSequenceNumber(nextSequenceNumber);
        while (nextSequenceNumberInLog == nextSequenceNumber) {
            currentSequenceNumber.compare_exchange_strong(sequenceNumber, nextSequenceNumberInLog);
            sequenceNumber = currentSequenceNumber.load();
            nextSequenceNumber = sequenceNumber + 1;
            nextSequenceNumberInLog = readSequenceNumber(nextSequenceNumber);
        };
    }

    uint64_t readSequenceNumber(uint64_t sequenceNumber) { return log[getLogIndex(sequenceNumber)]; }

    uint64_t getLogIndex(uint64_t sequenceNumber) { return sequenceNumber % logSize; }

    std::array<uint64_t ,logSize> log;
    std::atomic<uint64_t> currentSequenceNumber = 0;
};

}// namespace NES::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_SEQUENCE_LOG_HPP_
