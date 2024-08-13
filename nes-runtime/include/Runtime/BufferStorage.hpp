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

#pragma once

#include <mutex>
#include <optional>
#include <queue>
#include <unordered_map>
#include <Runtime/AbstractBufferStorage.hpp>
#include <Runtime/TupleBuffer.hpp>
namespace NES::Runtime
{

struct BufferSorter : public std::greater<TupleBuffer>
{
    bool operator()(const TupleBuffer& lhs, const TupleBuffer& rhs) { return lhs.getWatermark() > rhs.getWatermark(); }
};

/**
 * @brief The Buffer Storage class stores tuples inside a queue and trims it when the right acknowledgement is received
 */
class BufferStorage : public AbstractBufferStorage
{
public:
    BufferStorage() = default;

    void insertBuffer(NES::Runtime::TupleBuffer bufferPtr) override;

    /// Deletes all tuple buffers which watermark timestamp is smaller than the given timestamp
    void trimBuffer(uint64_t timestamp) override;

    [[nodiscard]] size_t getStorageSize() const override;

    [[nodiscard]] std::optional<NES::Runtime::TupleBuffer> getTopElementFromQueue() const;

    void removeTopElementFromQueue();

private:
    std::priority_queue<TupleBuffer, std::vector<TupleBuffer>, BufferSorter> storage;
};

using BufferStoragePtr = std::shared_ptr<Runtime::BufferStorage>;

} /// namespace NES::Runtime
