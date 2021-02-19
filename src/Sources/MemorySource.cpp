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
    std::string thName = "MemSrc-" + std::to_string(operatorId);
    setThreadName(thName.c_str());

    auto recordSize = schema->getSchemaSizeInBytes();
    auto bufferSize = bufferManager->getBufferSize();
    auto numOfBuffers = memoryAreaSize < bufferSize ? 1 : std::ceil(double(memoryAreaSize) / double(bufferSize));
    auto* pointer = memoryArea.get();
    auto remainingSize = memoryAreaSize;
    NES_ASSERT2_FMT(bufferSize % recordSize == 0,
                "A record might span multiple buffers and this is not supported bufferSize=" << bufferSize
                                                                                             << " recordSize=" << recordSize);
    for (auto i = 0u; i < numOfBuffers; ++i) {
        auto buffer = bufferManager->getBufferBlocking();
        auto length = std::min<size_t>(bufferSize, remainingSize);
        auto recordsPerBuffer = length / recordSize;
        memcpy(buffer.getBuffer(), pointer, length);
        buffer.setOriginId(operatorId);
        buffer.setNumberOfTuples(recordsPerBuffer);
        queryManager->addWork(operatorId, buffer);
        pointer += length;
        remainingSize -= length;
    }
    NES_ASSERT2_FMT(remainingSize == 0, "something wrong with remaining size " << remainingSize);
}

}// namespace NES