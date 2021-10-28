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

#include <Catalogs/MemorySourceStreamConfig.hpp>
#include <Operators/LogicalOperators/Sources/MemorySourceDescriptor.hpp>

namespace NES {

namespace detail {

struct MemoryAreaDeleter {
    void operator()(uint8_t* ptr) const { free(ptr); }
};

}// namespace detail

MemorySourceStreamConfig::MemorySourceStreamConfig(std::string sourceType,
                                                   std::string physicalStreamName,
                                                   std::vector<std::string> logicalStreamName,
                                                   uint8_t* memoryArea,
                                                   size_t memoryAreaSize,
                                                   uint64_t numBuffersToProcess,
                                                   uint64_t gatheringValue,
                                                   std::string gatheringMode)
    : PhysicalStreamConfig(SourceConfig::create()), sourceType(sourceType), memoryArea(memoryArea, detail::MemoryAreaDeleter()),
      memoryAreaSize(memoryAreaSize) {
    // nop
    this->physicalStreamName = physicalStreamName;
    this->logicalStreamName = logicalStreamName;
    this->numberOfBuffersToProduce = numBuffersToProcess;
    this->gatheringMode = DataSource::getGatheringModeFromString(gatheringMode);
    this->gatheringValue = gatheringValue;
}

const std::string MemorySourceStreamConfig::getSourceType() { return sourceType; }

const std::string MemorySourceStreamConfig::toString() { return sourceType; }

const std::string MemorySourceStreamConfig::getPhysicalStreamName() { return physicalStreamName; }

const std::vector<std::string> MemorySourceStreamConfig::getLogicalStreamNames() { return logicalStreamName; }

SourceDescriptorPtr MemorySourceStreamConfig::build(SchemaPtr ptr, std::string logicalStreamName) {
    std::string notUsed = logicalStreamName;
    return std::make_shared<MemorySourceDescriptor>(ptr,
                                                    memoryArea,
                                                    memoryAreaSize,
                                                    this->numberOfBuffersToProduce,
                                                    this->gatheringValue,
                                                    this->gatheringMode);
}

AbstractPhysicalStreamConfigPtr MemorySourceStreamConfig::create(std::string sourceType,
                                                                 std::string physicalStreamName,
                                                                 std::vector<std::string> logicalStreamName,
                                                                 uint8_t* memoryArea,
                                                                 size_t memoryAreaSize,
                                                                 uint64_t numBuffersToProcess,
                                                                 uint64_t gatheringValue,
                                                                 std::string gatheringMode) {
    NES_ASSERT(memoryArea, "invalid memory area");
    return std::make_shared<MemorySourceStreamConfig>(sourceType,
                                                      physicalStreamName,
                                                      logicalStreamName,
                                                      memoryArea,
                                                      memoryAreaSize,
                                                      numBuffersToProcess,
                                                      gatheringValue,
                                                      gatheringMode);
}

}// namespace NES