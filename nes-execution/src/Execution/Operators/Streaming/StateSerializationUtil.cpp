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

#include "Execution/Operators/Streaming/StateSerializationUtil.hpp"

namespace NES {
void StateSerializationUtil::writeToBuffer(Runtime::BufferManagerPtr bufferManager,
                                           uint64_t const bufferSize,
                                           uint64_t*& dataPtr,
                                           uint64_t& dataIdx,
                                           uint64_t& dataBuffersIdx,
                                           std::vector<Runtime::TupleBuffer>& buffers,
                                           uint64_t dataToWrite) {
    // check that current data buffer has enough space, by sending used space and space needed
    if (!hasSpaceLeft(dataIdx * sizeof(uint64_t), sizeof(uint64_t), bufferSize)) {
        // if current buffer does not contain enough space then
        // get new buffer and insert to vector of buffers
        auto newBuffer = bufferManager->getBufferBlocking();
        buffers.emplace(buffers.begin() + dataBuffersIdx++, newBuffer);
        // update pointer and index
        dataPtr = newBuffer.getBuffer<uint64_t>();
        dataIdx = 0;
    }
    dataPtr[dataIdx++] = dataToWrite;
}

uint64_t StateSerializationUtil::readFromBuffer(uint64_t*& dataPtr,
                                                uint64_t& dataIdx,
                                                uint64_t& dataBuffersIdx,
                                                std::vector<Runtime::TupleBuffer>& buffers) {
    // check left space in metadata buffer
    if (!buffers[dataBuffersIdx].hasSpaceLeft(dataIdx * sizeof(uint64_t), sizeof(uint64_t))) {
        // update metadata pointer and index
        dataPtr = buffers[++dataBuffersIdx].getBuffer<uint64_t>();
        dataIdx = 0;
    }
    return dataPtr[dataIdx++];
}

uint64_t StateSerializationUtil::readFromBuffer(const uint64_t*& dataPtr,
                                                uint64_t& dataIdx,
                                                uint64_t& dataBuffersIdx,
                                                std::span<const Runtime::TupleBuffer>& buffers) {
    // check left space in metadata buffer
    if (!buffers[dataBuffersIdx].hasSpaceLeft(dataIdx * sizeof(uint64_t), sizeof(uint64_t))) {
        // update metadata pointer and index
        dataPtr = buffers[++dataBuffersIdx].getBuffer<uint64_t>();
        dataIdx = 0;
    }
    return dataPtr[dataIdx++];
}

bool StateSerializationUtil::hasSpaceLeft(uint64_t used, uint64_t needed, uint64_t size) {
    if (used + needed <= size) {
        return true;
    }
    return false;
}
}// namespace NES