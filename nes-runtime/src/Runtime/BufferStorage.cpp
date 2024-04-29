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

void BufferStorage::insertBuffer(NES::Runtime::TupleBuffer buffer) { this->storage.push(buffer); }

void BufferStorage::trimBuffer(uint64_t timestamp) {
    while (!this->storage.empty() && this->storage.top().getWatermark() < timestamp) {
        NES_TRACE("BufferStorage: Delete tuple with watermark {}", this->storage.top().getWatermark());
        this->storage.pop();
    }
}

size_t BufferStorage::getStorageSize() const { return this->storage.size(); }

std::optional<TupleBuffer> BufferStorage::getTopElementFromQueue() const {
    if (storage.empty()) {
        return {};
    }
    return this->storage.top();
}

void BufferStorage::removeTopElementFromQueue() { this->storage.pop(); }

}// namespace NES::Runtime
