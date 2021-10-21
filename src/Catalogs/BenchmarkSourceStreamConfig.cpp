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

#include <Catalogs/BenchmarkSourceStreamConfig.hpp>
#include <Operators/LogicalOperators/Sources/BenchmarkSourceDescriptor.hpp>
#include <Configurations/ConfigOptions/SourceConfigurations/SourceConfigFactory.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES {

namespace detail {

struct MemoryAreaDeleter {
    void operator()(uint8_t* ptr) const { free(ptr); }
};

}// namespace detail

BenchmarkSourceStreamConfig::BenchmarkSourceStreamConfig(std::string sourceType,
                                                         std::string physicalStreamName,
                                                         std::string logicalStreamName,
                                                         uint8_t* memoryArea,
                                                         size_t memoryAreaSize,
                                                         uint64_t numBuffersToProcess,
                                                         uint64_t gatheringValue,
                                                         const std::string& gatheringMode,
                                                         const std::string& sourceMode,
                                                         uint64_t sourceAffinity)
    : PhysicalStreamConfig(SourceConfigFactory::createSourceConfig()), sourceType(std::move(sourceType)),
      memoryArea(memoryArea, detail::MemoryAreaDeleter()), memoryAreaSize(memoryAreaSize) {
    // nop
    this->physicalStreamName = std::move(physicalStreamName);
    this->logicalStreamName = std::move(logicalStreamName);
    this->numberOfBuffersToProduce = numBuffersToProcess;
    this->gatheringMode = DataSource::getGatheringModeFromString(std::move(gatheringMode));
    this->sourceMode = getSourceModeFromString(std::move(sourceMode));
    this->gatheringValue = gatheringValue;
    this->sourceAffinity = sourceAffinity;
}

BenchmarkSource::SourceMode BenchmarkSourceStreamConfig::getSourceModeFromString(const std::string& mode) {
    Util::trim(mode);
    if (mode == "emptyBuffer") {
        return BenchmarkSource::EMPTY_BUFFER;
    } else if (mode == "wrapBuffer") {
        return BenchmarkSource::WRAP_BUFFER;
    } else if (mode == "copyBuffer") {
        return BenchmarkSource::COPY_BUFFER;
    } else if (mode == "copyBufferSimdRte") {
        return BenchmarkSource::COPY_BUFFER_SIMD_RTE;
    } else if (mode == "cacheCopy") {
        return BenchmarkSource::CACHE_COPY;
    } else if (mode == "copyBufferSimdApex") {
        return BenchmarkSource::COPY_BUFFER_SIMD_APEX;
    } else {
        NES_THROW_RUNTIME_ERROR("mode not supported " << mode);
    }
}

std::string BenchmarkSourceStreamConfig::getSourceType() { return sourceType; }

std::string BenchmarkSourceStreamConfig::toString() { return sourceType; }

std::string BenchmarkSourceStreamConfig::getPhysicalStreamName() { return physicalStreamName; }

std::string BenchmarkSourceStreamConfig::getLogicalStreamName() { return logicalStreamName; }

SourceDescriptorPtr BenchmarkSourceStreamConfig::build(SchemaPtr ptr) {
    return std::make_shared<BenchmarkSourceDescriptor>(ptr,
                                                       memoryArea,
                                                       memoryAreaSize,
                                                       this->numberOfBuffersToProduce,
                                                       this->gatheringValue,
                                                       this->gatheringMode,
                                                       this->sourceMode,
                                                       this->sourceAffinity);
}

AbstractPhysicalStreamConfigPtr BenchmarkSourceStreamConfig::create(const std::string& sourceType,
                                                                    const std::string& physicalStreamName,
                                                                    const std::string& logicalStreamName,
                                                                    uint8_t* memoryArea,
                                                                    size_t memoryAreaSize,
                                                                    uint64_t numBuffersToProcess,
                                                                    uint64_t gatheringValue,
                                                                    const std::string& gatheringMode,
                                                                    const std::string& sourceMode,
                                                                    uint64_t sourceAffinity) {
    NES_ASSERT(memoryArea, "invalid memory area");
    return std::make_shared<BenchmarkSourceStreamConfig>(sourceType,
                                                         physicalStreamName,
                                                         logicalStreamName,
                                                         memoryArea,
                                                         memoryAreaSize,
                                                         numBuffersToProcess,
                                                         gatheringValue,
                                                         gatheringMode,
                                                         sourceMode,
                                                         sourceAffinity);
}

}// namespace NES
