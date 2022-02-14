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
#include <queue>

namespace NES::Runtime {

void BufferStorage::insertBuffer(uint64_t queryId, uint64_t nesPartitionId, NES::Runtime::TupleBufferPtr bufferPtr) {
    std::unique_lock<std::mutex> lock(mutex);
    auto iterator = this->buffers.find(queryId);
    if (iterator == this->buffers.end()) {
        auto queue = TupleBufferPriorityQueue();
        NES_TRACE("BufferStorage: Insert tuple with query id " << queryId << "and nes partition id" << nesPartitionId << " into buffer storage");
        queue.push(bufferPtr);
        this->buffers[queryId] = std::make_shared<BufferStorageUnit>(nesPartitionId, queue);
    } else {
        NES_TRACE("BufferStorage: Insert tuple with query id " << queryId << "and nes partition id" << nesPartitionId << " into buffer storage");
        iterator->second->getNesPartitionToTupleBufferQueueMapping().at(nesPartitionId).push(bufferPtr);
    }
}

bool BufferStorage::trimBuffer(uint64_t queryId, uint64_t timestamp) {
    std::unique_lock<std::mutex> lock(mutex);
    NES_TRACE("BufferStorage: Trying to delete tuple with query id " << queryId << " and timestamp " << timestamp);
    auto iteratorQueryId = this->buffers.find(queryId);
    if (iteratorQueryId != this->buffers.end()) {
        for (auto& iteratorNesPartition : iteratorQueryId->second->getNesPartitionToTupleBufferQueueMapping()) {
            auto& queue = iteratorNesPartition.second;
            if (!queue.empty()) {
                auto topElementWatermark = queue.top()->getWatermark();
                while (!queue.empty() && topElementWatermark < timestamp) {
                    NES_TRACE("BufferStorage: Delete tuple with watermark" << topElementWatermark);
                    queue.pop();
                    topElementWatermark = queue.top()->getWatermark();
                }
                return true;
            }
        }
    }
    return false;
}

size_t BufferStorage::getStorageSize() const {
    std::unique_lock<std::mutex> lock(mutex);
    size_t size = 0;
    for (auto& iteratorQueryId : buffers) {
        for (auto& iteratorNesPartition : iteratorQueryId.second->getNesPartitionToTupleBufferQueueMapping()) {
            size += iteratorNesPartition.second.size();
        }
    }
    return size;
}

size_t BufferStorage::getQueueSizeForGivenQueryAndNesPartition(uint64_t queryId, uint64_t nesPartitionId) const {
    std::unique_lock<std::mutex> lock(mutex);
    auto iteratorQueryId = this->buffers.find(queryId);
    if (iteratorQueryId == this->buffers.end()) {
        NES_ERROR("BufferStorage: No queue with query id " << queryId << "was found");
        return 0;
    } else {
        auto nesPartitionToQueueMapping = iteratorQueryId->second->getNesPartitionToTupleBufferQueueMapping();
        auto iteratorNesPartition = nesPartitionToQueueMapping.find(nesPartitionId);
        if (iteratorNesPartition == nesPartitionToQueueMapping.end()) {
            NES_ERROR("BufferStorage: No queue with nes partition id " << nesPartitionId << "was found");
            return 0;
        }
        else {
            return iteratorNesPartition->second.size();
        }
    }
}

NES::Runtime::TupleBufferPtr BufferStorage::getTopElementFromQueue(uint64_t queryId, uint64_t nesPartitionId) const {
    std::unique_lock<std::mutex> lock(mutex);
    auto iteratorQueryId = this->buffers.find(queryId);
    if (iteratorQueryId == this->buffers.end()) {
        NES_ERROR("BufferStorage: No queue with query id " << queryId << "was found");
        return nullptr;
    } else {
        auto nesPartitionToQueueMapping = iteratorQueryId->second->getNesPartitionToTupleBufferQueueMapping();
        auto iteratorNesPartition = nesPartitionToQueueMapping.find(nesPartitionId);
        if (iteratorNesPartition == nesPartitionToQueueMapping.end()) {
            NES_ERROR("BufferStorage: No queue with nes partition id " << nesPartitionId << "was found");
            return nullptr;
        }
        else {
            return iteratorNesPartition->second.top();
        }
    }
}

}// namespace NES::Runtime