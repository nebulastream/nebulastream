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

#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/KFSource.hpp>

namespace NES {

KFSource::KFSource(SchemaPtr schema,
                   const std::shared_ptr<uint8_t>& memoryArea,
                   size_t memoryAreaSize,
                   Runtime::BufferManagerPtr bufferManager,
                   Runtime::QueryManagerPtr queryManager,
                   uint64_t numBuffersToProcess,
                   uint64_t gatheringValue,
                   OperatorId operatorId,
                   size_t numSourceLocalBuffers,
                   GatheringMode gatheringMode,
                   uint64_t sourceAffinity,
                   std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      numBuffersToProcess,
                      operatorId,
                      numSourceLocalBuffers,
                      gatheringMode,
                      std::move(successors)),
    memoryArea(memoryArea), memoryAreaSize(memoryAreaSize) {
    this->gatheringInterval = std::chrono::milliseconds(gatheringValue);
    this->sourceAffinity = sourceAffinity;
    this->bufferSize = this->localBufferManager->getBufferSize();

    NES_ASSERT(memoryArea && memoryAreaSize > 0, "invalid memory area");
}

SourceType KFSource::getType() const {
    return SourceType::KF_SOURCE;
}

void KFSource::open() {
    // TODO: read CSV and load in memory
    DataSource::open();

    auto buffer = localBufferManager->getUnpooledBuffer(memoryAreaSize);
    numaLocalMemoryArea = *buffer;
    std::memcpy(numaLocalMemoryArea.getBuffer(), memoryArea.get(), memoryAreaSize);
    memoryArea.reset();
}

std::optional<Runtime::TupleBuffer> KFSource::receiveData() {
    // TODO: read from memory and emit buffer
    return bufferManager->getBufferBlocking();
}

}//namespace NES