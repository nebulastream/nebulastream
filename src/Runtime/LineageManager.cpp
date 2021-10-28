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

#include <Runtime/LineageManager.hpp>
#include <Util/BufferSequenceNumber.hpp>
#include <string>

namespace NES::Runtime {

void LineageManager::insertIntoLineage(BufferSequenceNumber newId, BufferSequenceNumber oldId) {
    std::unique_lock<std::mutex> lck(mutex);
    NES_TRACE("Insert tuple<" << newId.getSequenceNumber() << "," << newId.getOriginId() << "> into lineage manager");
    this->lineage[newId] = oldId;
}

bool LineageManager::trimLineage(BufferSequenceNumber id) {
    std::unique_lock<std::mutex> lck(mutex);
    auto iterator = this->lineage.find(id);
    if (iterator != this->lineage.end() && this->lineage.size()) {
        NES_TRACE("Trim tuple<" << id.getSequenceNumber() << "," << id.getOriginId() << "> from lineage manager");
        this->lineage.erase(iterator);
        return true;
    }
    return false;
}

BufferSequenceNumber LineageManager::findTupleAncestor(BufferSequenceNumber id) {
    std::unique_lock<std::mutex> lck(mutex);
    auto iterator = this->lineage.find(id);
    if (iterator != this->lineage.end() && this->lineage.size()) {
        return iterator->second;
    } else {
        //if a tuple buffer was not found return invalid tuple buffer
        return BufferSequenceNumber(-1, -1);
    }
}

size_t LineageManager::getLineageSize() const {
    std::unique_lock<std::mutex> lck(mutex);
    return this->lineage.size();
}
}// namespace NES::Runtime