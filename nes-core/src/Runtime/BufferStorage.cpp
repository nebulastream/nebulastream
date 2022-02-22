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

#include <Runtime/BufferStorage.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime {

void BufferStorage::insertBuffer(QueryId queryId, Network::PartitionId nesPartitionId, NES::Runtime::TupleBuffer buffer) {
    std::unique_lock<std::mutex> lock(mutex);
    auto iterator = this->buffers.find(queryId);
    if (iterator == this->buffers.end()) {
        auto queue = TupleBufferPriorityQueue();
        NES_TRACE("BufferStorage: Insert tuple with query id " << queryId << "and nes partition id" << nesPartitionId << " into buffer storage");
        queue.push(buffer);
        this->buffers[queryId] = std::make_shared<BufferStorageUnit>(nesPartitionId, queue);
    } else {
        NES_TRACE("BufferStorage: Insert tuple with query id " << queryId << "and nes partition id" << nesPartitionId << " into buffer storage");
        iterator->second->insert(nesPartitionId, buffer);
    }
}

bool BufferStorage::trimBuffer(QueryId queryId, uint64_t timestamp) {
    std::unique_lock<std::mutex> lock(mutex);
    NES_TRACE("BufferStorage: Trying to delete tuple with query id " << queryId << " and timestamp " << timestamp);
    auto iteratorQueryId = this->buffers.find(queryId);
    if (iteratorQueryId != this->buffers.end()) {
        return iteratorQueryId->second->trim(timestamp);
    }
    return false;
}

size_t BufferStorage::getStorageSize() const {
    std::unique_lock<std::mutex> lock(mutex);
    size_t size = 0;
    for (auto& iteratorQueryId : buffers) {
        size += iteratorQueryId.second->getSize();
    }
    return size;
}

size_t BufferStorage::getQueueSize(QueryId queryId, Network::PartitionId nesPartitionId) const {
    std::unique_lock<std::mutex> lock(mutex);
    auto iteratorQueryId = this->buffers.find(queryId);
    if (iteratorQueryId == this->buffers.end()) {
        NES_ERROR("BufferStorage: No queue with query id " << queryId << "was found");
        return 0;
    } else {
        return iteratorQueryId->second->getQueueSize(nesPartitionId);
    }
}

std::optional<TupleBuffer> BufferStorage::getTopElementFromQueue(QueryId queryId, Network::PartitionId nesPartitionId) const {
    std::unique_lock<std::mutex> lock(mutex);
    auto iteratorQueryId = this->buffers.find(queryId);
    if (iteratorQueryId == this->buffers.end()) {
        NES_ERROR("BufferStorage: No queue with query id " << queryId << "was found");
        return std::optional<TupleBuffer>();
    } else {
        return iteratorQueryId->second->getTopElementFromQueue(nesPartitionId);
    }
}

}// namespace NES::Runtime