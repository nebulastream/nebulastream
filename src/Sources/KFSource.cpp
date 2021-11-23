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

#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/KFSource.hpp>
#include <Sources/Parsers/CSVParser.hpp>

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

bool KFSource::loadCsvInMemory(std::string path) {
    NES_DEBUG("KFSource: Opening path " << path);
    // TODO: if c++17, change to std::filesystem::path path{ "dir/file/path" };
    char* filePath = realpath(path.c_str(), nullptr);
    fileInput.open(filePath);
    NES_DEBUG("CSVSource::CSVSource: read buffer");
    fileInput.seekg(0, std::ifstream::end);
    auto const reportedFileSize = fileInput.tellg();
    if (fileInput.bad() || reportedFileSize == -1) {
        return false;
    }
    size_t fileSize = static_cast<decltype(fileSize)>(reportedFileSize);
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    std::vector<PhysicalTypePtr> physicalTypes;
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }
    auto inputParser = std::make_shared<CSVParser>(schema->getSchemaSizeInBytes(), schema->getSize(), physicalTypes, ",");
    fileInput.seekg(0, std::ifstream::beg);
    // auto tuplesPerBuffer = bufferSize / tupleSize;
    // uint64_t tupleCount = 0;
    std::string line;
    std::getline(fileInput, line); // skip header
    fileInput.close();
    return true;
}

}//namespace NES