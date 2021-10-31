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

#include <Runtime/InMemoryLineageManager.hpp>
#include <Util/BufferSequenceNumber.hpp>

namespace NES::Runtime {

void InMemoryLineageManager::insert(BufferSequenceNumber newBufferSequenceNumber, BufferSequenceNumber oldBufferSequenceNumber) {
    std::unique_lock<std::mutex> lock(mutex);
    NES_TRACE("Insert tuple<" << newBufferSequenceNumber.getSequenceNumber() << "," << oldBufferSequenceNumber.getOriginId()
                              << "> into bufferAncestorMapping manager");
    this->bufferAncestorMapping[newBufferSequenceNumber].push_back(oldBufferSequenceNumber);
}

bool InMemoryLineageManager::trim(BufferSequenceNumber bufferSequenceNumber) {
    std::unique_lock<std::mutex> lock(mutex);
    auto iterator = this->bufferAncestorMapping.find(bufferSequenceNumber);
    if (iterator != this->bufferAncestorMapping.end()) {
        NES_TRACE("Trim tuple<" << bufferSequenceNumber.getSequenceNumber() << "," << bufferSequenceNumber.getOriginId()
                                << "> from bufferAncestorMapping manager");
        this->bufferAncestorMapping.erase(iterator);
        return true;
    }
    return false;
}

std::vector<BufferSequenceNumber> InMemoryLineageManager::findTupleBufferAncestor(BufferSequenceNumber bufferSequenceNumber) {
    std::unique_lock<std::mutex> lock(mutex);
    auto iterator = this->bufferAncestorMapping.find(bufferSequenceNumber);
    if (iterator != this->bufferAncestorMapping.end()) {
        return iterator->second;
    } else {
        //if a tuple buffer was not found return empty vector
        return std::vector<BufferSequenceNumber>(0);
    }
}

size_t InMemoryLineageManager::getLineageSize() const {
    std::unique_lock<std::mutex> lock(mutex);
    return this->bufferAncestorMapping.size();
}
}// namespace NES::Runtime