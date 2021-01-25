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

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/YSBSourceDescriptor.hpp>
#include <Util/Logger.hpp>
#include <sstream>
namespace NES {

PhysicalStreamConfigPtr PhysicalStreamConfig::create(SourceConfig* sourceConfig) {
    return std::make_shared<PhysicalStreamConfig>(PhysicalStreamConfig(sourceConfig));
}

PhysicalStreamConfig::PhysicalStreamConfig(SourceConfig* sourceConfig)
    : sourceType(sourceConfig->getSourceType().getValue()), sourceConfig(sourceConfig->getSourceConfig().getValue()),
      sourceFrequency(sourceConfig->getSourceFrequency().getValue()),
      numberOfTuplesToProducePerBuffer(sourceConfig->getNumberOfTuplesToProducePerBuffer().getValue()),
      numberOfBuffersToProduce(sourceConfig->getNumberOfBuffersToProduce().getValue()),
      physicalStreamName(sourceConfig->getPhysicalStreamName().getValue()),
      logicalStreamName(sourceConfig->getLogicalStreamName().getValue()), skipHeader(sourceConfig->getSkipHeader().getValue()){
    NES_INFO("PhysicalStreamConfig: Created source with config: " << this->toString());
                                                                          };

const std::string PhysicalStreamConfig::toString() {
    std::stringstream ss;
    ss << "sourceType=" << sourceType << " sourceConfig=" << sourceConfig << " sourceFrequency=" << sourceFrequency
       << " numberOfTuplesToProducePerBuffer=" << numberOfTuplesToProducePerBuffer
       << " numberOfBuffersToProduce=" << numberOfBuffersToProduce << " physicalStreamName=" << physicalStreamName
       << " logicalStreamName=" << logicalStreamName;
    return ss.str();
}

const std::string PhysicalStreamConfig::getSourceType() { return sourceType; }

const std::string PhysicalStreamConfig::getSourceConfig() const { return sourceConfig; }

uint32_t PhysicalStreamConfig::getSourceFrequency() const { return sourceFrequency; }

uint32_t PhysicalStreamConfig::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

uint32_t PhysicalStreamConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

const std::string PhysicalStreamConfig::getPhysicalStreamName() { return physicalStreamName; }

const std::string PhysicalStreamConfig::getLogicalStreamName() { return logicalStreamName; }

bool PhysicalStreamConfig::getSkipHeader() const { return skipHeader; }

SourceDescriptorPtr PhysicalStreamConfig::build(SchemaPtr schema) {
    auto* config = this;
    auto streamName = config->getLogicalStreamName();

    // Pick the first element from the catalog entry and identify the type to create appropriate source type
    // todo add handling for support of multiple physical streams.
    std::string type = config->getSourceType();
    std::string conf = config->getSourceConfig();
    uint64_t frequency = config->getSourceFrequency();
    uint64_t numBuffers = config->getNumberOfBuffersToProduce();
    bool skipHeader = config->getSkipHeader();

    uint64_t numberOfTuplesToProducePerBuffer = config->getNumberOfTuplesToProducePerBuffer();

    if (type == "DefaultSource") {
        NES_DEBUG("TypeInferencePhase: create default source for one buffer");
        return DefaultSourceDescriptor::create(schema, streamName, numBuffers, frequency);
    } else if (type == "CSVSource") {
        NES_DEBUG("TypeInferencePhase: create CSV source for " << conf << " buffers");
        return CsvSourceDescriptor::create(schema, streamName, conf, /**delimiter*/ ",", numberOfTuplesToProducePerBuffer,
                                           numBuffers, frequency, skipHeader);
    } else if (type == "SenseSource") {
        NES_DEBUG("TypeInferencePhase: create Sense source for udfs " << conf);
        return SenseSourceDescriptor::create(schema, streamName, /**udfs*/ conf);
    } else if (type == "YSBSource") {
        NES_DEBUG("TypeInferencePhase: create YSB source for " << conf);
        return YSBSourceDescriptor::create(streamName, numberOfTuplesToProducePerBuffer, numBuffers, frequency);
    } else {
        NES_THROW_RUNTIME_ERROR("TypeInferencePhase:: source type " + type + " not supported");
    }
    return nullptr;
}
}// namespace NES
