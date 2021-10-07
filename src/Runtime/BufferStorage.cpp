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

namespace NES {
    void BufferStorage::insertBuffer(BufferSequenceNumber id, NES::Runtime::TupleBuffer bufferPtr) {
        mutex.lock();
        buffer.push(std::pair<BufferSequenceNumber, Runtime::TupleBuffer> {id, bufferPtr});
        mutex.unlock();
    }

    bool BufferStorage::trimBuffer(BufferSequenceNumber id) {
        mutex.lock();
        if(!buffer.empty()) {
            while (buffer.front().first < id) {
                buffer.pop();
            }
            return true;
        }
        return false;
        mutex.unlock();
    }

    size_t BufferStorage::getStorageSize() const {
        mutex.lock();
        return buffer.size();
        mutex.unlock();
    }

}// namespace NES