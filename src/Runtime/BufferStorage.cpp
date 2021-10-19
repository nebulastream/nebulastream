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
    std::unique_lock<std::mutex> lck(mutex);
    if (this->buffers.find(id.getOriginId()) == this->buffers.end()) {
        auto queue =
            std::priority_queue<BufferStorageUnitPtr, std::vector<BufferStorageUnitPtr>, std::greater<BufferStorageUnitPtr>>();
        NES_DEBUG("Insert tuple<" << id.getSequenceNumber() << "," << id.getOriginId() << "> into buffer storage");
        queue.push(std::make_shared<BufferStorageUnit>(id, bufferPtr));
        this->buffers[id.getOriginId()] = std::move(queue);
    } else {
        NES_DEBUG("Insert tuple<" << id.getSequenceNumber() << "," << id.getOriginId() << "> into buffer storage");
        this->buffers[id.getOriginId()].push(std::make_shared<BufferStorageUnit>(id, bufferPtr));
        ;
    }
}

bool BufferStorage::trimBuffer(BufferSequenceNumber id) {
    std::unique_lock<std::mutex> lck(mutex);
    NES_DEBUG("Trying to delete tuple<" << id.getSequenceNumber() << "," << id.getOriginId()
                              << "> from buffer storage");
    if (this->buffers.find(id.getOriginId()) != this->buffers.end()) {
        auto queue = &this->buffers[id.getOriginId()];
        if (!queue->empty()) {
            auto topElement = queue->top()->getSequenceNumber();
            while (!queue->empty() && topElement < id) {
                NES_DEBUG("Delete tuple<" << topElement.getSequenceNumber() << "," << topElement.getOriginId()
                                          << "> from buffer storage");
                queue->pop();
                topElement = queue->top()->getSequenceNumber();
            }
            return true;
        }
    }
    return false;
}

size_t BufferStorage::getBufferStoreSizeForAllQueues() const {
    std::unique_lock<std::mutex> lck(mutex);
    size_t size = 0;
    for (size_t i = 0; i < this->buffers.size(); i++) {
        size += this->buffers.at(i).size();
    }
    return size;
}

size_t BufferStorage::getStoreSizeForQueue(uint64_t queueId) const {
    std::unique_lock<std::mutex> lck(mutex);
    return this->buffers.at(queueId).size();
}

BufferStorageUnitPtr BufferStorage::getTopElementFromQueue(uint64_t queueId) const {
    std::unique_lock<std::mutex> lck(mutex);
    return this->buffers.at(queueId).top();
}

}// namespace NES::Runtime