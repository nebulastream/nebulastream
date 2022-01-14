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

#ifndef NES_INCLUDE_RUNTIME_BUFFER_STORAGE_HPP_
#define NES_INCLUDE_RUNTIME_BUFFER_STORAGE_HPP_

#include <Runtime/AbstractBufferStorage.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/BufferSequenceNumber.hpp>
#include <Util/BufferStorageUnit.hpp>
#include <Util/SequenceNumberTrackerUnit.hpp>
#include <queue>
#include <unordered_map>

namespace NES::Runtime {

using BufferStorageQueue = std::queue<BufferStorageUnitPtr>;

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
     * @brief Inserts a pair id, buffer link to the buffer storage queue
     * @param id id of the buffer
     * @param bufferPtr pointer to the buffer that will be stored
     */
    void insertBuffer(BufferSequenceNumber id, NES::Runtime::TupleBuffer bufferPtr) override;

    /**
     * @brief Deletes all pair<id,buffer> link which sequence number smaller than passed
     * sequence number from the buffer storage queue with the same origin id
     * @param id id of the buffer
     * @return true in case of a success trimming
     */
    bool trimBuffer(BufferSequenceNumber id) override;

    /**
     * @brief Return current storage size
     * @return Current storage size
     */
    size_t getStorageSize() const override;

    /**
     * @brief Return the size of queue with a given origin Id
     * @param queueId the id of the queue
     * @return Given queue size
     */
    size_t getStorageSizeForQueue(uint64_t queueId) const;

    /**
     * @brief Return top element of the queue
     * @param queueId the id of the queue
     * @return buffer storage unit
     */
    BufferStorageUnitPtr getTopElementFromQueue(uint64_t queueId) const;

    /**
     * @brief Return the sequence number tracker structure
     * @return sequence number tracker
     */
    std::unordered_map<uint64_t, SequenceNumberTrackerUnitPtr>& getSequenceNumberTracker();

  private:
    std::unordered_map<uint64_t, BufferStorageQueue> buffers;
    std::unordered_map<uint64_t, SequenceNumberTrackerUnitPtr> sequenceNumberTracker;
    mutable std::mutex mutex;
};
using BufferStoragePtr = std::shared_ptr<Runtime::BufferStorage>;

}// namespace NES::Runtime

#endif// NES_INCLUDE_RUNTIME_BUFFER_STORAGE_HPP_
