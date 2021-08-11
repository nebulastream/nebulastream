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
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES {

namespace detail {

struct MemoryAreaDeleter {
    void operator()(uint8_t* ptr) const { free(ptr); }
};

}// namespace detail

MemorySourceStreamConfig::MemorySourceStreamConfig(std::string sourceType,
                                                   std::string physicalStreamName,
                                                   std::string logicalStreamName,
                                                   uint8_t* memoryArea,
                                                   size_t memoryAreaSize,
                                                   uint64_t numBuffersToProcess,
                                                   uint64_t gatheringValue,
                                                   const std::string& gatheringMode,
                                                   const std::string& sourceMode)
    : PhysicalStreamConfig(SourceConfig::create()), sourceType(std::move(sourceType)),
      memoryArea(memoryArea, detail::MemoryAreaDeleter()), memoryAreaSize(memoryAreaSize) {
    // nop
    this->physicalStreamName = std::move(physicalStreamName);
    this->logicalStreamName = std::move(logicalStreamName);
    this->numberOfBuffersToProduce = numBuffersToProcess;
    this->gatheringMode = DataSource::getGatheringModeFromString(std::move(gatheringMode));
    this->sourceMode = getSourceModeFromString(std::move(sourceMode));
    this->gatheringValue = gatheringValue;
}

MemorySource::SourceMode MemorySourceStreamConfig::getSourceModeFromString(const std::string& mode) {
    UtilityFunctions::trim(mode);
    if (mode == "emptyBuffer") {
        return MemorySource::EMPTY_BUFFER;
    } else if (mode == "wrapBuffer") {
        return MemorySource::WRAP_BUFFER;
    } else if (mode == "copyBuffer") {
        return MemorySource::COPY_BUFFER;
    } else if (mode == "copyBufferSimd") {
        return MemorySource::COPY_BUFFER_SIMD;
    } else {
        NES_THROW_RUNTIME_ERROR("mode not supported " << mode);
    }
}

std::string MemorySourceStreamConfig::getSourceType() { return sourceType; }

std::string MemorySourceStreamConfig::toString() { return sourceType; }

std::string MemorySourceStreamConfig::getPhysicalStreamName() { return physicalStreamName; }

std::string MemorySourceStreamConfig::getLogicalStreamName() { return logicalStreamName; }

SourceDescriptorPtr MemorySourceStreamConfig::build(SchemaPtr ptr) {
    return std::make_shared<MemorySourceDescriptor>(ptr,
                                                    memoryArea,
                                                    memoryAreaSize,
                                                    this->numberOfBuffersToProduce,
                                                    this->gatheringValue,
                                                    this->gatheringMode,
                                                    this->sourceMode);
}

AbstractPhysicalStreamConfigPtr MemorySourceStreamConfig::create(const std::string& sourceType,
                                                                 const std::string& physicalStreamName,
                                                                 const std::string& logicalStreamName,
                                                                 uint8_t* memoryArea,
                                                                 size_t memoryAreaSize,
                                                                 uint64_t numBuffersToProcess,
                                                                 uint64_t gatheringValue,
                                                                 const std::string& gatheringMode,
                                                                 const std::string& sourceMode) {
    NES_ASSERT(memoryArea, "invalid memory area");
    return std::make_shared<MemorySourceStreamConfig>(sourceType,
                                                      physicalStreamName,
                                                      logicalStreamName,
                                                      memoryArea,
                                                      memoryAreaSize,
                                                      numBuffersToProcess,
                                                      gatheringValue,
                                                      gatheringMode,
                                                      sourceMode);
}

}// namespace NES