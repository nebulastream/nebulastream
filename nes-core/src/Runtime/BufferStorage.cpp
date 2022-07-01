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
#include <Util/Logger/Logger.hpp>
#include <mutex>

namespace NES::Runtime {

BufferStorage::BufferStorage(Network::NesPartition nesPartitionId) {
    this->storage[nesPartitionId] = std::priority_queue<TupleBuffer, std::vector<TupleBuffer>, BufferSorter>();
}

void BufferStorage::insertBuffer(Network::NesPartition nesPartition, NES::Runtime::TupleBuffer buffer) {
    storage[nesPartition].push(buffer);
}

bool BufferStorage::trimBuffer(Network::NesPartition nesPartition, uint64_t timestamp) {
    auto iteratorPartitionId = this->storage.find(nesPartition);
    if (iteratorPartitionId != this->storage.end()) {
        if (!iteratorPartitionId->second.empty()) {
            while (!iteratorPartitionId->second.empty() && iteratorPartitionId->second.top().getWatermark() <= timestamp) {
                NES_TRACE("BufferStorage: Delete tuple with watermark" << iteratorPartitionId->second.top().getWatermark());
                iteratorPartitionId->second.pop();
            }
            return true;
        }
    }
    return false;
}

size_t BufferStorage::getStorageSize() const {
    size_t size = 0;
    for (auto& iteratorPartition : storage) {
        size += iteratorPartition.second.size();
    }
    return size;
}

size_t BufferStorage::getQueueSize(Network::NesPartition nesPartition) const {
    auto iteratorPartition = storage.find(nesPartition);
    if (iteratorPartition != storage.end()) {
        return iteratorPartition->second.size();
    } else {
        return 0;
    }
}

std::optional<TupleBuffer> BufferStorage::getTopElementFromQueue(Network::NesPartition nesPartition) const {
    auto iteratorQueryId = this->storage.find(nesPartition);
    if (iteratorQueryId == this->storage.end()) {
        NES_ERROR("BufferStorage: No queue with nesPartition " << nesPartition << "was found");
        return std::optional<TupleBuffer>();
    } else {
        return iteratorQueryId->second.top();
    }
}

}// namespace NES::Runtime