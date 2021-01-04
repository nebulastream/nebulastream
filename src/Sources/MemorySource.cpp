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
                           OperatorId operatorId)
    : DataSource(std::move(schema), std::move(bufferManager), std::move(queryManager), operatorId), memoryArea(memoryArea),
      memoryAreaSize(memoryAreaSize) {
    NES_ASSERT(memoryArea && memoryAreaSize > 0, "invalid memory area");
}

std::optional<NodeEngine::TupleBuffer> MemorySource::receiveData() {
    NES_ASSERT(false, "this must not be invoked");
    return std::nullopt;
}

const std::string MemorySource::toString() const { return "MemorySource"; }

NES::SourceType MemorySource::getType() const { return MEMORY_SOURCE; }
void MemorySource::runningRoutine(NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager) {
    std::string thName = "DataSrc-" + std::to_string(operatorId);
    setThreadName(thName.c_str());

    auto recordSize = schema->getSchemaSizeInBytes();
    auto bufferSize = bufferManager->getBufferSize();
    NES_ASSERT(memoryAreaSize % bufferSize == 0, "memory area size must be a multiple of buffer size");
    NES_ASSERT(bufferSize % recordSize == 0, "buffer size must be a multiple of record size");

    auto numOfBuffers = memoryAreaSize / bufferSize;
    auto recordsPerBuffer = bufferSize / recordSize;
    auto* pointer = memoryArea.get();
    for (auto i = 0u; i < numOfBuffers; ++i) {
        auto buffer = bufferManager->getBufferBlocking();
        memcpy(buffer.getBuffer(), pointer, bufferSize);
        buffer.setOriginId(operatorId);
        buffer.setNumberOfTuples(recordsPerBuffer);
        queryManager->addWork(operatorId, buffer);
        pointer += bufferSize;
    }
}

}// namespace NES