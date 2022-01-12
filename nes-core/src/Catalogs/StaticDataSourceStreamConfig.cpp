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

#include <Catalogs/StaticDataSourceStreamConfig.hpp>
#include <Configurations/Sources/DefaultSourceConfig.hpp>
#include <Operators/LogicalOperators/Sources/StaticDataSourceDescriptor.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES::Experimental {

using namespace Configurations;

namespace detail {

//struct MemoryAreaDeleter {
//    void operator()(uint8_t* ptr) const { free(ptr); }
//};

}// namespace detail

StaticDataSourceStreamConfig::StaticDataSourceStreamConfig(std::string sourceType,
                                                   std::string physicalStreamName,
                                                   std::string logicalStreamName,
                                                   std::string pathTableFile,
                                                   uint64_t numBuffersToProcess)
    : PhysicalStreamConfig(DefaultSourceConfig::create()), sourceType(std::move(sourceType)),
    pathTableFile(std::move(pathTableFile)) {
    this->physicalStreamName = std::move(physicalStreamName);
    this->logicalStreamName = std::move(logicalStreamName);
    this->numberOfBuffersToProduce = numBuffersToProcess;
}

std::string StaticDataSourceStreamConfig::getSourceType() { return sourceType; }

std::string StaticDataSourceStreamConfig::toString() { return sourceType; }

std::string StaticDataSourceStreamConfig::getPhysicalStreamName() { return physicalStreamName; }

std::string StaticDataSourceStreamConfig::getLogicalStreamName() { return logicalStreamName; }

SourceDescriptorPtr StaticDataSourceStreamConfig::build(SchemaPtr ptr) {
    return std::make_shared<StaticDataSourceDescriptor>(ptr, this->pathTableFile);
}

AbstractPhysicalStreamConfigPtr StaticDataSourceStreamConfig::create(const std::string& sourceType,
                                                                 const std::string& physicalStreamName,
                                                                 const std::string& logicalStreamName,
                                                                 const std::string& pathTableFile,
                                                                 uint64_t numBuffersToProcess) {
    return std::make_shared<StaticDataSourceStreamConfig>(sourceType,
                                                      physicalStreamName,
                                                      logicalStreamName,
                                                      pathTableFile,
                                                      numBuffersToProcess);
}

}// namespace NES
