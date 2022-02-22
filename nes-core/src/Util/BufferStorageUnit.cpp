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

#include <Util/BufferStorageUnit.hpp>

namespace NES::Runtime {
void BufferStorageUnit::insert(Network::PartitionId nesPartitionId, TupleBuffer buffer) {
    std::unique_lock<std::mutex> lock(mutex);
    auto iterator = nesPartitionToTupleBufferQueueMapping.find(nesPartitionId);
    if (iterator == nesPartitionToTupleBufferQueueMapping.end()) {
        auto queue = TupleBufferPriorityQueue();
        NES_TRACE("BufferStorage: Insert tuple with nes partition id" << nesPartitionId << " into buffer storage");
        queue.push(buffer);
        nesPartitionToTupleBufferQueueMapping[nesPartitionId] = std::move(queue);
    } else {
        NES_TRACE("BufferStorage: Insert tuple with nes partition id" << nesPartitionId << " into buffer storage");
        iterator->second.push(buffer);
    }
}

bool BufferStorageUnit::trim(uint64_t timestamp) {
    std::unique_lock<std::mutex> lock(mutex);
    for (auto& iteratorNesPartition : nesPartitionToTupleBufferQueueMapping) {
        auto& queue = iteratorNesPartition.second;
        if (!queue.empty()) {
            while (!queue.empty() && queue.top().getWatermark() < timestamp) {
                NES_TRACE("BufferStorage: Delete tuple with watermark" << queue.top().getWatermark());
                queue.pop();
            }
        }
    }
    return true;
}

size_t BufferStorageUnit::getSize() const {
    std::unique_lock<std::mutex> lock(mutex);
    size_t size = 0;
    for (auto& iteratorQueryId : nesPartitionToTupleBufferQueueMapping) {
        size += iteratorQueryId.second.size();
    }
    return size;
}

size_t BufferStorageUnit::getQueueSize(Network::PartitionId nesPartitionId) const {
    std::unique_lock<std::mutex> lock(mutex);
    auto iterator = nesPartitionToTupleBufferQueueMapping.find(nesPartitionId);
    if (iterator == nesPartitionToTupleBufferQueueMapping.end()) {
        NES_ERROR("BufferStorage: No queue with nes partition id " << nesPartitionId << "was found");
        return 0;
    } else {
        return iterator->second.size();
    }
}

}