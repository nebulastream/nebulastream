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
#include <Util/BufferSequenceNumber.hpp>
#include <Util/BufferStorageUnit.hpp>
#include <queue>
#include <unordered_map>

namespace NES::Runtime {

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
    void insertBuffer(uint64_t queryId, uint64_t nesPartitionId, NES::Runtime::TupleBufferPtr bufferPtr) override;

    /**
     * @brief Deletes all tuple buffers which watermark timestamp is smaller than the given timestamp
     * @param queryId id of current query
     * @param timestamp max timestamp of current epoch
     * @return true in case of a success trimming
     */
    bool trimBuffer(uint64_t queryId, uint64_t timestamp) override;

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
    size_t getQueueSizeForGivenQueryAndNesPartition(uint64_t queryId, uint64_t nesPartitionId) const;

    /**
     * @brief Return top element of the queue
     * @param queryId id of the pipeline
     * @param nesPartitionId destination id
     * @return buffer storage unit
     */
    NES::Runtime::TupleBufferPtr getTopElementFromQueue(uint64_t queryId, uint64_t nesPartitionId) const;

  private:
    std::unordered_map<uint64_t, BufferStorageUnitPtr> buffers;
    mutable std::mutex mutex;
};

using BufferStoragePtr = std::shared_ptr<Runtime::BufferStorage>;

}// namespace NES::Runtime

#endif  // NES_INCLUDE_RUNTIME_BUFFERSTORAGE_HPP_
