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

#include <Catalogs/KFSourceStreamConfig.hpp>
#include <Configurations/Sources/SourceConfigFactory.hpp>
#include <Operators/LogicalOperators/Sources/KFSourceDescriptor.hpp>

namespace NES {

namespace detail {
struct MemoryAreaDeleter {
    void operator()(uint8_t* ptr) const { free(ptr); }
};
}// namespace detail

KFSourceStreamConfig::KFSourceStreamConfig(std::string sourceType,
                                           std::string physicalStreamName,
                                           std::string logicalStreamName,
                                           uint8_t* memoryArea,
                                           size_t memoryAreaSize,
                                           uint64_t numBuffersToProcess,
                                           uint64_t gatheringValue,
                                           uint64_t sourceAffinity)
    : PhysicalStreamConfig(Configurations::SourceConfigFactory::createSourceConfig()),
      memoryArea(memoryArea, detail::MemoryAreaDeleter()),
      memoryAreaSize(memoryAreaSize) {
    this->physicalStreamName = std::move(physicalStreamName);
    this->logicalStreamName = std::move(logicalStreamName);
    this->numberOfBuffersToProduce = numBuffersToProcess;
    this->gatheringMode = DataSource::GatheringMode::FREQUENCY_MODE;
    this->gatheringValue = gatheringValue;
    this->sourceAffinity = sourceAffinity;
    this->sourceType = sourceType;
}

std::string KFSourceStreamConfig::getSourceType() { return sourceType; }

std::string KFSourceStreamConfig::toString() { return sourceType; }

std::string KFSourceStreamConfig::getPhysicalStreamName() { return physicalStreamName; }

std::string KFSourceStreamConfig::getLogicalStreamName() { return logicalStreamName; }

SourceDescriptorPtr KFSourceStreamConfig::build(SchemaPtr ptr) {
    return std::make_shared<KFSourceDescriptor>(ptr,
                                                memoryArea,
                                                memoryAreaSize,
                                                this->numberOfBuffersToProduce,
                                                this->gatheringValue,
                                                this->sourceAffinity);
}

AbstractPhysicalStreamConfigPtr KFSourceStreamConfig::create(const std::string& sourceType,
                                                            const std::string& physicalStreamName,
                                                            const std::string& logicalStreamName,
                                                            uint8_t* memoryArea,
                                                            size_t memoryAreaSize,
                                                            uint64_t numBuffersToProcess,
                                                            uint64_t gatheringValue,
                                                            uint64_t sourceAffinity) {
    NES_ASSERT(memoryArea, "invalid memory area");
    return std::make_shared<KFSourceStreamConfig>(sourceType,
                                                 physicalStreamName,
                                                 logicalStreamName,
                                                 memoryArea,
                                                 memoryAreaSize,
                                                 numBuffersToProcess,
                                                 gatheringValue,
                                                 sourceAffinity);
}

}// namespace NES