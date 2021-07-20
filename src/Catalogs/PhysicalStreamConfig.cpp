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
#include <Configurations/ConfigOption.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <sstream>
namespace NES {

PhysicalStreamConfigPtr PhysicalStreamConfig::create(SourceConfigPtr sourceConfig) {
    return std::make_shared<PhysicalStreamConfig>(PhysicalStreamConfig(sourceConfig));
}

PhysicalStreamConfigPtr PhysicalStreamConfig::createEmpty() {
    return std::make_shared<PhysicalStreamConfig>(PhysicalStreamConfig(SourceConfig::create()));
}

PhysicalStreamConfig::PhysicalStreamConfig(SourceConfigPtr sourceConfig)
    : sourceType(sourceConfig->getSourceType()->getValue()), sourceConfig(sourceConfig->getSourceConfig()->getValue()),
      sourceFrequency(sourceConfig->getSourceFrequency()->getValue()),
      numberOfTuplesToProducePerBuffer(sourceConfig->getNumberOfTuplesToProducePerBuffer()->getValue()),
      numberOfBuffersToProduce(sourceConfig->getNumberOfBuffersToProduce()->getValue()),
      physicalStreamName(sourceConfig->getPhysicalStreamName()->getValue()),
      logicalStreamName(sourceConfig->getLogicalStreamName()->getValue()), skipHeader(sourceConfig->getSkipHeader()->getValue()) {
    NES_INFO("PhysicalStreamConfig: Created source with config: " << this->toString());
};

const std::string PhysicalStreamConfig::toString() {
    std::stringstream ss;
    ss << "sourceType=" << sourceType << " sourceConfig=" << sourceConfig << " sourceFrequency=" << sourceFrequency.count()
       << "ms"
       << " numberOfTuplesToProducePerBuffer=" << numberOfTuplesToProducePerBuffer
       << " numberOfBuffersToProduce=" << numberOfBuffersToProduce << " physicalStreamName=" << physicalStreamName
       << " logicalStreamName="
       << "(" + UtilityFunctions::combineStringsWithDelimiter(logicalStreamName, ", ") + ")";
    return ss.str();
}

const std::string PhysicalStreamConfig::getSourceType() { return sourceType; }

const std::string PhysicalStreamConfig::getSourceConfig() const { return sourceConfig; }

std::chrono::milliseconds PhysicalStreamConfig::getSourceFrequency() const { return sourceFrequency; }

uint32_t PhysicalStreamConfig::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

uint32_t PhysicalStreamConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

const std::string PhysicalStreamConfig::getPhysicalStreamName() { return physicalStreamName; }

const std::vector<std::string> PhysicalStreamConfig::getLogicalStreamName() { return logicalStreamName; }

void PhysicalStreamConfig::addLogicalStreamName(std::string logicalStreamName) {
    this->logicalStreamName.push_back(logicalStreamName);
}

bool PhysicalStreamConfig::getSkipHeader() const { return skipHeader; }

SourceDescriptorPtr PhysicalStreamConfig::build(SchemaPtr schema, std::string streamName) {
    auto* config = this;

    std::string type = config->getSourceType();
    std::string conf = config->getSourceConfig();
    std::chrono::milliseconds frequency = config->getSourceFrequency();
    uint64_t numBuffers = config->getNumberOfBuffersToProduce();
    bool skipHeader = config->getSkipHeader();

    uint64_t numberOfTuplesToProducePerBuffer = config->getNumberOfTuplesToProducePerBuffer();

    if (type == "DefaultSource") {
        NES_DEBUG("PhysicalStreamConfig: create default source for one buffer");
        return DefaultSourceDescriptor::create(schema, streamName, numBuffers, frequency.count());
    } else if (type == "CSVSource") {
        NES_DEBUG("PhysicalStreamConfig: create CSV source for " << conf << " buffers");
        return CsvSourceDescriptor::create(schema,
                                           streamName,
                                           conf,
                                           /**delimiter*/ ",",
                                           numberOfTuplesToProducePerBuffer,
                                           numBuffers,
                                           frequency.count(),
                                           skipHeader);
    } else if (type == "SenseSource") {
        NES_DEBUG("PhysicalStreamConfig: create Sense source for udfs " << conf);
        return SenseSourceDescriptor::create(schema, streamName, /**udfs*/ conf);
    } else {
        NES_THROW_RUNTIME_ERROR("PhysicalStreamConfig:: source type " + type + " not supported");
    }
    return nullptr;
}
void PhysicalStreamConfig::setSourceFrequency(uint32_t sourceFrequency) {
    PhysicalStreamConfig::sourceFrequency = std::chrono::milliseconds(sourceFrequency);
}
void PhysicalStreamConfig::setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBuffer) {
    PhysicalStreamConfig::numberOfTuplesToProducePerBuffer = numberOfTuplesToProducePerBuffer;
}
void PhysicalStreamConfig::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce) {
    PhysicalStreamConfig::numberOfBuffersToProduce = numberOfBuffersToProduce;
}
SourceConfigPtr PhysicalStreamConfig::toSourceConfig() {
    auto config = SourceConfig::create();
    config->setSourceType(sourceType);
    config->setSourceConfig(sourceConfig);
    config->setSourceFrequency(sourceFrequency.count());
    config->setNumberOfBuffersToProduce(numberOfTuplesToProducePerBuffer);
    config->setNumberOfTuplesToProducePerBuffer(numberOfBuffersToProduce);
    config->setPhysicalStreamName(physicalStreamName);
    config->setLogicalStreamName(UtilityFunctions::combineStringsWithDelimiter(logicalStreamName, ","));
    config->setSkipHeader(skipHeader);
    return config;
}
void PhysicalStreamConfig::setPhysicalStreamName(const std::string& newName) {
    PhysicalStreamConfig::physicalStreamName = newName;
}
}// namespace NES
