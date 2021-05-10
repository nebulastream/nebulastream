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

#include <NodeEngine/FixedSizeBufferPool.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/MemorySource.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <cmath>
namespace NES {

MemorySource::MemorySource(SchemaPtr schema, std::shared_ptr<uint8_t> memoryArea, size_t memoryAreaSize,
                           NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                           uint64_t numBuffersToProcess, uint64_t gatheringValue, OperatorId operatorId,
                           size_t numSourceLocalBuffers, GatheringMode gatheringMode,
                           std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(std::move(schema), bufferManager, std::move(queryManager), operatorId, numSourceLocalBuffers, gatheringMode,
                 successors),
      memoryArea(memoryArea), memoryAreaSize(memoryAreaSize), currentPositionInBytes(0), globalBufferManager(bufferManager),
      refCnt(0) {
    this->numBuffersToProcess = numBuffersToProcess;
    if (gatheringMode == GatheringMode::FREQUENCY_MODE) {
        this->gatheringInterval = std::chrono::milliseconds(gatheringValue);
    } else if (gatheringMode == GatheringMode::INGESTION_RATE_MODE) {
        this->gatheringIngestionRate = gatheringValue;
    } else {
        NES_THROW_RUNTIME_ERROR("Mode not implemented " << gatheringMode);
    }

    NES_DEBUG("MemorySource() numBuffersToProcess=" << numBuffersToProcess << " memoryAreaSize=" << memoryAreaSize);
    NES_ASSERT(memoryArea && memoryAreaSize > 0, "invalid memory area");
}

std::optional<NodeEngine::TupleBuffer> MemorySource::receiveData() {
    NES_DEBUG("MemorySource::receiveData called on operatorId=" << operatorId);

    uint64_t numberOfTuples = 0;
    //if the memory area is smaller than a buffer
    if (memoryAreaSize <= globalBufferManager->getBufferSize()) {
        numberOfTuples = std::floor(double(memoryAreaSize) / double(schema->getSchemaSizeInBytes()));
    } else {
        //if the memory area spans multiple buffers
        auto restTuples = (memoryAreaSize - currentPositionInBytes) / schema->getSchemaSizeInBytes();
        auto numberOfTuplesPerBuffer =
            std::floor(double(globalBufferManager->getBufferSize()) / double(schema->getSchemaSizeInBytes()));
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
    NES_ASSERT2_FMT(numberOfTuples * schema->getSchemaSizeInBytes() <= globalBufferManager->getBufferSize(),
                    "value to write is larger than the buffer");

    using namespace std::chrono_literals;

    auto buffer = this->bufferManager->getBufferTimeout(NES::NodeEngine::DEFAULT_BUFFER_TIMEOUT);
    if (!buffer) {
        NES_ERROR("Buffer invalid after waiting on timeout");
        return std::nullopt;
    }
    memcpy(buffer->getBuffer(), memoryArea.get() + currentPositionInBytes, globalBufferManager->getBufferSize());

    //        TODO: replace copy with inplace add like with the wraparound #1853
    //    auto buffer2 = NodeEngine::TupleBuffer::wrapMemory(memoryArea.get() + currentPositionInBytes, globalBufferManager->getBufferSize(), this);

    refCnt++;
    if (memoryAreaSize > buffer->getBufferSize()) {
        NES_DEBUG("MemorySource::receiveData: add offset=" << globalBufferManager->getBufferSize()
                                                           << " to currentpos=" << currentPositionInBytes);
        currentPositionInBytes += globalBufferManager->getBufferSize();
    }

    buffer->setNumberOfTuples(numberOfTuples);

    generatedTuples += buffer->getNumberOfTuples();
    generatedBuffers++;

    NES_DEBUG("MemorySource::receiveData filled buffer with tuples=" << buffer->getNumberOfTuples());
    if (buffer->getNumberOfTuples() == 0) {
        return std::nullopt;
    } else {
        return buffer;
    }
}

const std::string MemorySource::toString() const { return "MemorySource"; }

NES::SourceType MemorySource::getType() const { return MEMORY_SOURCE; }
void MemorySource::recyclePooledBuffer(NodeEngine::detail::MemorySegment*) { refCnt--; }
void MemorySource::recycleUnpooledBuffer(NodeEngine::detail::MemorySegment*) {}

}// namespace NES