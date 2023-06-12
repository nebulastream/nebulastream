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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_ABSTRACTBUFFERSTORAGE_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_ABSTRACTBUFFERSTORAGE_HPP_

#include <Runtime/TupleBuffer.hpp>
#include <cstddef>

namespace NES::Runtime {

/**
 * @brief The Abstract buffer storage class to backup tuple buffers that are passing through node engine
 */
class AbstractBufferStorage {
  public:
    virtual ~AbstractBufferStorage() noexcept = default;

    /**
     * @brief Inserts a pair id, buffer link to the buffer storage
     * @param queryId id of current query
     * @param bufferPtr pointer to the buffer that will be stored
     */
    virtual void insertBuffer(NES::Runtime::TupleBuffer bufferPtr) = 0;

    /**
     * @brief Deletes q pair<id,buffer> from buffer storage
     * @param timestamp max timestamp of current epoch
     */
    virtual void trimBuffer(uint64_t timestamp) = 0;

    /**
     * @brief Return current storage size
     * @return Current storage size
     */
    virtual size_t getStorageSize() const = 0;
};
}// namespace NES::Runtime
#endif // NES_RUNTIME_INCLUDE_RUNTIME_ABSTRACTBUFFERSTORAGE_HPP_
