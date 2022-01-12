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

#include <Runtime/BufferStorage.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <queue>

namespace NES::Runtime {

void BufferStorage::insertBuffer(BufferSequenceNumber id, NES::Runtime::TupleBuffer bufferPtr) {
    auto iterator = this->buffers.find(id.getOriginId());
    if (iterator == this->buffers.end()) {
        auto queue = BufferStoragePriorityQueue();
        NES_TRACE("Insert tuple<" << id.getSequenceNumber() << "," << id.getOriginId() << "> into buffer storage");
        queue.push(std::make_shared<BufferStorageUnit>(id, bufferPtr));
        this->buffers[id.getOriginId()] = std::move(queue);
        this->sequenceNumberTracker[id.getOriginId()] = SequenceNumberTrackerUnitPtr();
    } else {
        uint64_t currentMaxSequenceNumber = this->buffers[id.getOriginId()].top()->getSequenceNumber().getSequenceNumber();
        if (currentMaxSequenceNumber != id.getSequenceNumber() - 1) {
            std::unique_lock<std::mutex> lock(mutex);
            this->sequenceNumberTracker[id.getOriginId()]->getSequenceNumberTrackerPriorityQueue().push(
                std::make_shared<BufferStorageUnit>(id, bufferPtr));
            std::unique_lock<std::mutex> unlock(mutex);
        } else {
            uint64_t currentMaxSequenceNumberAddition = 1;
            auto sequenceNumberTrackerPriorityQueue =
                this->sequenceNumberTracker[id.getOriginId()]->getSequenceNumberTrackerPriorityQueue();
            while (!sequenceNumberTrackerPriorityQueue.empty()) {
                if (sequenceNumberTrackerPriorityQueue.top()->getSequenceNumber().getSequenceNumber()
                    == currentMaxSequenceNumber + currentMaxSequenceNumberAddition + 1) {
                    sequenceNumberTrackerPriorityQueue.pop();
                    currentMaxSequenceNumberAddition++;
                } else {
                    break;
                }
            }
            this->sequenceNumberTracker[id.getOriginId()]->setCurrentHighestSequenceNumber(currentMaxSequenceNumber
                                                                                           + currentMaxSequenceNumberAddition);
        }
        NES_TRACE("Insert tuple<" << id.getSequenceNumber() << "," << id.getOriginId() << "> into buffer storage");
        iterator->second.push(std::make_shared<BufferStorageUnit>(id, bufferPtr));
    }
}

bool BufferStorage::trimBuffer(BufferSequenceNumber id) {
    NES_TRACE("Trying to delete tuple<" << id.getSequenceNumber() << "," << id.getOriginId() << "> from buffer storage");
    auto iterator = this->buffers.find(id.getOriginId());
    if (iterator != this->buffers.end()) {
        auto& queue = iterator->second;
        if (!queue.empty()) {
            auto topElement = queue.top()->getSequenceNumber();
            while (!queue.empty() && topElement < id) {
                NES_TRACE("Delete tuple<" << topElement.getSequenceNumber() << "," << topElement.getOriginId()
                                          << "> from buffer storage");
                queue.pop();
                topElement = queue.top()->getSequenceNumber();
            }
            return true;
        }
    }
    return false;
}

size_t BufferStorage::getStorageSize() const {
    std::unique_lock<std::mutex> lock(mutex);
    size_t size = 0;
    for (auto& q : buffers) {
        size += this->buffers.at(q.first).size();
    }
    return size;
}

size_t BufferStorage::getStorageSizeForQueue(uint64_t queueId) const {
    std::unique_lock<std::mutex> lock(mutex);
    auto iterator = this->buffers.find(queueId);
    if (iterator == this->buffers.end()) {
        return 0;
    } else {
        return iterator->second.size();
    }
}

BufferStorageUnitPtr BufferStorage::getTopElementFromQueue(uint64_t queueId) const {
    std::unique_lock<std::mutex> lock(mutex);
    auto iterator = this->buffers.find(queueId);
    if (iterator == this->buffers.end()) {
        return nullptr;
    } else {
        return iterator->second.top();
    }
}
std::unordered_map<uint64_t, SequenceNumberTrackerUnitPtr>& BufferStorage::getSequenceNumberTracker() {
    return sequenceNumberTracker;
}

}// namespace NES::Runtime