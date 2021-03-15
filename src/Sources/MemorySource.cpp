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

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/MemorySource.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>

namespace NES {

MemorySource::MemorySource(SchemaPtr schema, std::shared_ptr<uint8_t> memoryArea, size_t memoryAreaSize,
                           NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                           uint64_t numBuffersToProcess, std::chrono::milliseconds frequency, OperatorId operatorId)
    : DataSource(std::move(schema), std::move(bufferManager), std::move(queryManager), operatorId), memoryArea(memoryArea),
      memoryAreaSize(memoryAreaSize), currentPositionInBytes(0) {
    this->numBuffersToProcess = numBuffersToProcess;
    this->gatheringInterval = std::chrono::milliseconds(frequency);
    ;
    NES_DEBUG("MemorySource() numBuffersToProcess=" << numBuffersToProcess << " memoryAreaSize=" << memoryAreaSize);
    NES_ASSERT(memoryArea && memoryAreaSize > 0, "invalid memory area");
}

std::optional<NodeEngine::TupleBuffer> MemorySource::receiveData() {
    NES_DEBUG("MemorySource::receiveData called on operatorId=" << operatorId);
    auto buffer = this->bufferManager->getBufferBlocking();

    uint64_t numberOfTuples = 0;
    //if the memory area is smaller than a buffer
    if (memoryAreaSize <= buffer.getBufferSize()) {
        numberOfTuples = std::floor(double(memoryAreaSize) / double(schema->getSchemaSizeInBytes()));
    } else {
        //if the memory area spans multiple buffers
        auto restTuples = (memoryAreaSize - currentPositionInBytes) / schema->getSchemaSizeInBytes();
        auto numberOfTuplesPerBuffer = std::floor(double(buffer.getBufferSize()) / double(schema->getSchemaSizeInBytes()));
        if (restTuples > numberOfTuplesPerBuffer) {
            numberOfTuples = numberOfTuplesPerBuffer;
        } else {
            numberOfTuples = restTuples;
        }

        if (currentPositionInBytes + numberOfTuples * schema->getSchemaSizeInBytes() > memoryAreaSize) {
            if (numBuffersToProcess != 0) {
                NES_DEBUG("MemorySource::receiveData: reset buffer to 0");
                currentPositionInBytes = 0;
            } else {
                NES_DEBUG("MemorySource::receiveData: return as mem sry is empty");
                return std::nullopt;
            }
        }
    }
    uint64_t offset = numberOfTuples * schema->getSchemaSizeInBytes();

    NES_ASSERT2_FMT(numberOfTuples * schema->getSchemaSizeInBytes() <= buffer.getBufferSize(),
                    "value to write is larger than the buffer");

    memcpy(buffer.getBuffer(), memoryArea.get() + currentPositionInBytes, offset);

    if (memoryAreaSize > buffer.getBufferSize()) {
        NES_DEBUG("MemorySource::receiveData: add offset=" << offset << " to currentpos=" << currentPositionInBytes);
        currentPositionInBytes += offset;
    }

    buffer.setNumberOfTuples(numberOfTuples);

    generatedTuples += buffer.getNumberOfTuples();
    generatedBuffers++;
    NES_DEBUG("MemorySource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());
    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    } else {
        return buffer;
    }
}

const std::string MemorySource::toString() const { return "MemorySource"; }

NES::SourceType MemorySource::getType() const { return MEMORY_SOURCE; }

}// namespace NES