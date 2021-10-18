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

#ifndef NES_BUFFERSTORAGE_H
#define NES_BUFFERSTORAGE_H

#include <Runtime/TupleBuffer.hpp>
#include <Util/BufferSequenceNumber.hpp>
#include <Util/BufferStorageUnit.hpp>
#include <queue>

namespace NES::Runtime {

/**
 * @brief The Buffer Storage class stores tuples inside a queue and trims it when the right acknowledgement is received
 */
class BufferStorage {

  public:
    /**
     * @brief Inserts a pair id, buffer link to the buffer storage queue
     * @param mutex mutex to make the method thread safe
     * @param id id of the buffer
     * @param bufferPtr pointer to the buffer that will be stored
     */
    void insertBuffer(BufferSequenceNumber id, NES::Runtime::TupleBuffer bufferPtr);
    /**
     * @brief Deletes a pair id, buffer link from the buffer storage queue with a given id
     * @param mutex mutex to make the method thread safe
     * @param id id of the buffer
     */
    bool trimBuffer(BufferSequenceNumber id);
    /**
     * @brief Return current storage size
     * @param mutex mutex to make the method thread safe
     * @return Current storage size
     */
    size_t getStorageSize() const;
    /**
     * @brief Return given origin Id queue size
     * @param mutex mutex to make the method thread safe
     * @return Given queue size
     */
    size_t getQueueSize(uint64_t originId) const;
    /**
     * @brief Return top element of the queue
     * @param mutex mutex to make the method thread safe
     * @return buffer storage unit
     */
    BufferStorageUnit getTopelementFromQueue(uint64_t originId) const;

  private:
    std::map<uint64_t , std::priority_queue<BufferStorageUnit, std::vector<BufferStorageUnit>, std::greater<BufferStorageUnit>>> buffers;
    mutable std::mutex mutex;
};

}// namespace NES

#endif//NES_BUFFERSTORAGE_H


