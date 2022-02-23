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

#ifndef NES_INCLUDE_RUNTIME_BUFFERSTORAGE_HPP_
#define NES_INCLUDE_RUNTIME_BUFFERSTORAGE_HPP_

#include <Runtime/AbstractBufferStorage.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <optional>
#include <queue>
#include <unordered_map>

namespace NES::Runtime {

struct BufferSorter : public std::greater<TupleBuffer> {
    bool operator()(const TupleBuffer& lhs, const TupleBuffer& rhs) {
        return lhs.getWatermark() > rhs.getWatermark();
    }
};

using TupleBufferPriorityQueue =
    std::priority_queue<TupleBuffer, std::vector<TupleBuffer>, BufferSorter>;

/**
 * @brief The Buffer Storage Unit class encapsulates a nesPartitionId and a sorted queue of TupleBuffers
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
    std::optional<NES::Runtime::TupleBuffer> getTopElementFromQueue(Network::PartitionId nesPartitionId) const;

  private:
    mutable std::mutex mutex;
    std::unordered_map<Network::PartitionId, TupleBufferPriorityQueue> nesPartitionToTupleBufferQueueMapping;
};

using BufferStorageUnitPtr = std::shared_ptr<BufferStorageUnit>;

/**
 * @brief The Buffer Storage class stores tuples inside a queue and trims it when the right acknowledgement is received
 */
class BufferStorage : public AbstractBufferStorage {

  public:
    /**
     * @brief Constructor, which creates a buffer storage
     */
    BufferStorage() = default;

    /**
     * @brief Inserts a tuple buffer for a given nes partition id and query id
     * @param queryId id of the pipeline
     * @param nesPartitionId destination id
     * @param bufferPtr pointer to the buffer that will be stored
     */
    void insertBuffer(QueryId queryId, Network::PartitionId nesPartitionId, NES::Runtime::TupleBuffer bufferPtr) override;

    /**
     * @brief Deletes all tuple buffers which watermark timestamp is smaller than the given timestamp
     * @param queryId id of current query
     * @param timestamp max timestamp of current epoch
     * @return true in case of a success trimming
     */
    bool trimBuffer(QueryId queryId, uint64_t timestamp) override;

    /**
     * @brief Return current storage size
     * @return Current storage size
     */
    size_t getStorageSize() const override;

    /**
     * @brief Return the size of queue with a given queue id and nes partition id
     * @param queryId id of the pipeline
     * @param nesPartitionId destination id
     * @return Given queue size
     */
    size_t getQueueSize(QueryId queryId, Network::PartitionId nesPartitionId) const;

    /**
     * @brief Return top element of the queue
     * @param queryId id of the pipeline
     * @param nesPartitionId destination id
     * @return buffer storage unit
     */
    std::optional<NES::Runtime::TupleBuffer> getTopElementFromQueue(QueryId queryId, Network::PartitionId nesPartitionId) const;

  private:
    std::unordered_map<QueryId, BufferStorageUnitPtr> buffers;
    mutable std::mutex mutex;
};

using BufferStoragePtr = std::shared_ptr<Runtime::BufferStorage>;

}// namespace NES::Runtime

#endif  // NES_INCLUDE_RUNTIME_BUFFERSTORAGE_HPP_
