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

#ifndef NES_SEQUENCENUMBERTRACKERUNIT_H
#define NES_SEQUENCENUMBERTRACKERUNIT_H

#include <Util/BufferStorageUnit.hpp>
#include <queue>
namespace NES::Runtime {
/**
 * @brief The Sequence Number Tracker Unit class encapsulates for a given origin id a pair<highest linearly increased sequence number, priority queue with tuples that came out of order>
 */
class SequenceNumberTrackerUnit {
  public:
    /**
     * @brief Constructor, which creates new sequence number tracker unit with the highest sequence number 0 and a new tracker priority queue
     * @param sequenceNumberTrackerPriorityQueue priority queue with tuples that came out of order and need to be stored until the linear order of arrival tuples is restored
     * @return sequence number tracker unit
     */
    SequenceNumberTrackerUnit()
        : currentHighestSequenceNumber(0),
          SequenceNumberTrackerPriorityQueue(std::priority_queue<BufferStorageUnitPtr,
                                                                 std::vector<BufferStorageUnitPtr>,
                                                                 std::greater<BufferStorageUnitPtr>>()) {}

    /**
     * @brief Getter for a current highest linearly increasing sequence number for a given origin id
     * @return sequence number
     */
    uint64_t getCurrentHighestSequenceNumber() const;

    /**
     * @brief Getter for a current highest linearly increasing sequence number for a given origin id
     * @return sequence number
     */
    std::priority_queue<BufferStorageUnitPtr, std::vector<BufferStorageUnitPtr>, std::greater<BufferStorageUnitPtr>>&
    getSequenceNumberTrackerPriorityQueue();

    /**
     * @brief Set the current highest linearly increasing sequence number to the passed parameter
     * @param currentHighestSequenceNumber current highest linearly increasing sequence number
     */
    void setCurrentHighestSequenceNumber(uint64_t currentHighestSequenceNumber);

  private:
    uint64_t currentHighestSequenceNumber;
    std::priority_queue<BufferStorageUnitPtr, std::vector<BufferStorageUnitPtr>, std::greater<BufferStorageUnitPtr>>
        SequenceNumberTrackerPriorityQueue;
    friend bool operator<(const std::shared_ptr<SequenceNumberTrackerUnit>& lhs,
                          const std::shared_ptr<SequenceNumberTrackerUnit>& rhs) {
        return lhs->currentHighestSequenceNumber < rhs->currentHighestSequenceNumber;
    }
    friend bool operator>(const std::shared_ptr<SequenceNumberTrackerUnit>& lhs,
                          const std::shared_ptr<SequenceNumberTrackerUnit>& rhs) {
        return lhs->currentHighestSequenceNumber > rhs->currentHighestSequenceNumber;
    }
};

using SequenceNumberTrackerUnitPtr = std::shared_ptr<SequenceNumberTrackerUnit>;
}// namespace NES::Runtime

#endif//NES_SEQUENCENUMBERTRACKERUNIT_H
