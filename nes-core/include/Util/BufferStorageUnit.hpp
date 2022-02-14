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

#include <queue>
#include <unordered_map>

#ifndef NES_INCLUDE_UTIL_BUFFERSTORAGEUNIT_HPP_
#define NES_INCLUDE_UTIL_BUFFERSTORAGEUNIT_HPP_

namespace NES::Runtime {

using TupleBufferPriorityQueue =
    std::priority_queue<TupleBufferPtr, std::vector<TupleBufferPtr>, std::greater<TupleBufferPtr>>;

/**
 * @brief The Buffer Storage Unit class encapsulates a pair<tuple id, pointer to the tuple>
 */
class BufferStorageUnit {
  public:
    /**
     * @brief Constructor, which creates map of nes partition id to sorted queue of tuple buffers
     * @return buffer storage unit
     */
    BufferStorageUnit(uint64_t nesPartitionId, TupleBufferPriorityQueue queue) {
        nesPartitionToTupleBufferQueueMapping.at(nesPartitionId) = std::move(queue);
    }

    /**
     * @brief Getter for map of nes partition to tuple buffers
     * @return map of nes partition to tuple buffers
     */
    std::unordered_map<uint64_t, TupleBufferPriorityQueue> getNesPartitionToTupleBufferQueueMapping() const { return nesPartitionToTupleBufferQueueMapping; }

  private:
    std::unordered_map<uint64_t, TupleBufferPriorityQueue> nesPartitionToTupleBufferQueueMapping;
};

using BufferStorageUnitPtr = std::shared_ptr<BufferStorageUnit>;
}// namespace NES::Runtime
#endif  // NES_INCLUDE_UTIL_BUFFERSTORAGEUNIT_HPP_
