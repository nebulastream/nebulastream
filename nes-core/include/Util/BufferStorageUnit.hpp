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

#include <Network/NesPartition.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <queue>
#include <unordered_map>

#ifndef NES_INCLUDE_UTIL_BUFFERSTORAGEUNIT_HPP_
#define NES_INCLUDE_UTIL_BUFFERSTORAGEUNIT_HPP_

namespace NES::Runtime {

using TupleBufferPriorityQueue =
    std::priority_queue<TupleBuffer, std::vector<TupleBuffer>, std::greater<TupleBuffer>>;

/**
 * @brief The Buffer Storage Unit class encapsulates a pair<tuple id, pointer to the tuple>
 */
class BufferStorageUnit {
  public:
    /**
     * @brief Constructor, which creates map of nes partition id to sorted queue of tuple buffers
     * @return buffer storage unit
     */
    BufferStorageUnit(Network::PartitionId nesPartitionId, TupleBufferPriorityQueue queue) {
        nesPartitionToTupleBufferQueueMapping[nesPartitionId] = std::move(queue);
    }

    /**
     * @brief inserts buffer for a given nes partition id
     * @param nesPartitionId destination id
     * @param buffer tuple buffer
     */
    void insert(Network::PartitionId nesPartitionId, TupleBuffer buffer);

    /**
     * @brief trims all tuple buffers smaller than a given timetsamp
     * @param timestamp
     * @return success
     */
    bool trim(uint64_t timestamp);

    /**
     * @brief Return current storage size
     * @return Current storage size
     */
    size_t getSize() const;

    /**
     * @brief Return the size of queue with a given nes partition id
     * @param nesPartitionId destination id
     * @return Given queue size
     */
    size_t getQueueSize(Network::PartitionId nesPartitionId) const;

    /**
     * @brief Return top element of the queue
     * @param nesPartitionId destination id
     * @return buffer storage unit
     */
    std::optional<NES::Runtime::TupleBuffer> getTopElementFromQueue(Network::PartitionId nesPartitionId) const {
        auto iteratorNesPartition = nesPartitionToTupleBufferQueueMapping.find(nesPartitionId);
        if (iteratorNesPartition == nesPartitionToTupleBufferQueueMapping.end()) {
            NES_ERROR("BufferStorage: No queue with nes partition id " << nesPartitionId << "was found");
            return std::optional<TupleBuffer>();
        }
        else {
            return iteratorNesPartition->second.top();
        }
    }

  private:
    mutable std::mutex mutex;
    std::unordered_map<Network::PartitionId, TupleBufferPriorityQueue> nesPartitionToTupleBufferQueueMapping;
};

using BufferStorageUnitPtr = std::shared_ptr<BufferStorageUnit>;
}// namespace NES::Runtime
#endif  // NES_INCLUDE_UTIL_BUFFERSTORAGEUNIT_HPP_
