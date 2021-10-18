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
        std::unique_lock<std::mutex> lck (mutex);
        if(this->buffers.find(id.getOriginId()) == this->buffers.end()) {
            auto queue = std::priority_queue<BufferStorageUnit, std::vector<BufferStorageUnit>, std::greater<BufferStorageUnit>>();
            queue.push(BufferStorageUnit{id, bufferPtr});
            this->buffers[id.getOriginId()]=std::move(queue);
        }
        else {
            this->buffers[id.getOriginId()].push(BufferStorageUnit{id, bufferPtr});
        }
    }

    bool BufferStorage::trimBuffer(BufferSequenceNumber id) {
        std::unique_lock<std::mutex> lck (mutex);
        auto queue = &this->buffers[id.getOriginId()];
        if(!queue->empty()) {
            while (!queue->empty() && queue->top().getSequenceNumber() < id) {
                queue->pop();
            }
            return true;
        }
        return false;
    }

    size_t BufferStorage::getStorageSize() const {
        std::unique_lock<std::mutex> lck (mutex);
        size_t size = 0;
        for (size_t i = 0; i < this->buffers.size(); i++) {
            size += this->buffers.at(i).size();
        }
        return size;
    }

    size_t BufferStorage::getQueueSize(uint64_t originId) const {
        std::unique_lock<std::mutex> lck (mutex);
        return this->buffers.at(originId).size();
    }

    BufferStorageUnit BufferStorage::getTopElementFromQueue(uint64_t originId) const {
        return this->buffers.at(originId).top();
    }

    }// namespace NES