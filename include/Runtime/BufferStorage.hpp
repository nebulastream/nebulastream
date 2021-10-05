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
#include <queue>


typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

namespace NES {

/**
 * @brief The Buffer Storage class stores tuples inside a queue and trims it when the right acknowledgement is received
 */
class BufferStorage {
  private:
    std::queue<std::pair<uint64_t, Runtime::TupleBuffer>> buffer;
    mutable std::mutex mutex;

  public:
    /**
     * @brief Inserts a pair id, buffer link to the buffer storage queue
     * @param mutex mutex to make the method thread safe
     * @param id id of the buffer
     * @param bufferPtr pointer to the buffer that will be stored
     */
    void insertBuffer(uint128_t id, NES::Runtime::TupleBuffer bufferPtr);
    /**
     * @brief Deletes a pair id, buffer link from the buffer storage queue with a given id
     * @param mutex mutex to make the method thread safe
     * @param id id of the buffer
     */
    bool trimBuffer(uint128_t id);
    /**
     * @brief Return current queue size
     * @param mutex mutex to make the method thread safe
     * @return Current queue size
     */
    size_t getStorageSize() const;
};

}// namespace NES

#endif//NES_BUFFERSTORAGE_H


