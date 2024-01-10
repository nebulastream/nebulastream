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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_BUFFERSTORAGE_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_BUFFERSTORAGE_HPP_

#include <Runtime/AbstractBufferStorage.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <mutex>
#include <optional>
#include <queue>
#include <unordered_map>
namespace NES::Runtime {

struct BufferSorter : public std::greater<TupleBuffer> {
    bool operator()(const TupleBuffer& lhs, const TupleBuffer& rhs) { return lhs.getWatermark() > rhs.getWatermark(); }
};

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
     * @brief Inserts a tuple buffer for a given nes partition
     * @param nesPartition destination
     * @param bufferPtr pointer to the buffer that will be stored
     */
    void insertBuffer(NES::Runtime::TupleBuffer bufferPtr) override;

    /**
     * @brief Deletes all tuple buffers which watermark timestamp is smaller than the given timestamp
     * @param NesPartition destination
     * @param timestamp max timestamp of current epoch
     */
    void trimBuffer(uint64_t timestamp) override;

    /**
     * @brief Returns current storage size
     * @return Current storage size
     */
    size_t getStorageSize() const override;

    /**
     * @brief Returns top element of the queue
     * @return buffer storage unit
     */
    std::optional<NES::Runtime::TupleBuffer> getTopElementFromQueue() const;

    /**
     * @brief Removes the top element from the queue
     */
    void removeTopElementFromQueue();

  private:
    std::priority_queue<TupleBuffer, std::vector<TupleBuffer>, BufferSorter> storage;
};

using BufferStoragePtr = std::shared_ptr<Runtime::BufferStorage>;

}// namespace NES::Runtime

#endif // NES_RUNTIME_INCLUDE_RUNTIME_BUFFERSTORAGE_HPP_
