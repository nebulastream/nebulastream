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
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <sstream>
#include <string.h>
#include <utility>
namespace NES {

PhysicalStreamConfigPtr PhysicalStreamConfig::create(const SourceConfigPtr& sourceConfig) {
    return std::make_shared<PhysicalStreamConfig>(PhysicalStreamConfig(sourceConfig));
}

PhysicalStreamConfigPtr PhysicalStreamConfig::createEmpty() {
    return std::make_shared<PhysicalStreamConfig>(PhysicalStreamConfig(SourceConfig::create()));
}

PhysicalStreamConfig::PhysicalStreamConfig(const SourceConfigPtr& sourceConfig)
    : sourceType(sourceConfig->getSourceType()->getValue()), sourceConfig(sourceConfig->getSourceConfig()->getValue()),
      sourceFrequency(sourceConfig->getSourceFrequency()->getValue()),
      numberOfTuplesToProducePerBuffer(sourceConfig->getNumberOfTuplesToProducePerBuffer()->getValue()),
      numberOfBuffersToProduce(sourceConfig->getNumberOfBuffersToProduce()->getValue()),
      physicalStreamName(sourceConfig->getPhysicalStreamName()->getValue()),
      logicalStreamName(sourceConfig->getLogicalStreamName()->getValue()), skipHeader(sourceConfig->getSkipHeader()->getValue()) {
    NES_INFO("PhysicalStreamConfig: Created source with config: " << this->toString());
};

std::string PhysicalStreamConfig::toString() {
    std::stringstream ss;
    ss << "sourceType=" << sourceType << " sourceConfig=" << sourceConfig << " sourceFrequency=" << sourceFrequency.count()
       << "ms"
       << " numberOfTuplesToProducePerBuffer=" << numberOfTuplesToProducePerBuffer
       << " numberOfBuffersToProduce=" << numberOfBuffersToProduce << " physicalStreamName=" << physicalStreamName
       << " logicalStreamName=" << logicalStreamName;
    return ss.str();
}

std::string PhysicalStreamConfig::getSourceType() { return sourceType; }

std::string PhysicalStreamConfig::getSourceConfig() const { return sourceConfig; }

std::chrono::milliseconds PhysicalStreamConfig::getSourceFrequency() const { return sourceFrequency; }

uint32_t PhysicalStreamConfig::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

uint32_t PhysicalStreamConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

std::string PhysicalStreamConfig::getPhysicalStreamName() { return physicalStreamName; }

std::string PhysicalStreamConfig::getLogicalStreamName() { return logicalStreamName; }

bool PhysicalStreamConfig::getSkipHeader() const { return skipHeader; }

SourceDescriptorPtr PhysicalStreamConfig::build(SchemaPtr schema) {
    auto* config = this;
    auto streamName = config->getLogicalStreamName();

    // Pick the first element from the catalog entry and identify the type to create appropriate source type
    // todo add handling for support of multiple physical streams.
    std::string type = config->getSourceType();
    std::string conf = config->getSourceConfig();
    std::chrono::milliseconds frequency = config->getSourceFrequency();
    uint64_t numBuffers = config->getNumberOfBuffersToProduce();

    bool const newSkipHeader = config->getSkipHeader();
    uint64_t newNumberOfTuplesToProducePerBuffer = config->getNumberOfTuplesToProducePerBuffer();

    if (type == "DefaultSource") {
        NES_DEBUG("PhysicalStreamConfig: create default source for one buffer");
        return DefaultSourceDescriptor::create(schema, streamName, numBuffers, frequency.count());
    }
    if (type == "CSVSource") {
        NES_DEBUG("PhysicalStreamConfig: create CSV source for " << conf << " buffers");
        return CsvSourceDescriptor::create(schema,
                                           streamName,
                                           conf,
                                           /**delimiter*/ ",",
                                           newNumberOfTuplesToProducePerBuffer,
                                           numBuffers,
                                           frequency.count(),
                                           newSkipHeader);
    } else if (type == "SenseSource") {
        NES_DEBUG("PhysicalStreamConfig: create Sense source for udfs " << conf);
        return SenseSourceDescriptor::create(schema, streamName, /**udfs*/ conf);
#ifdef ENABLE_MQTT_BUILD
        NES_DEBUG("PhysicalStreamConfig: create MQTT source with configurations: " << conf << ".");
    } else if (type == "MQTTSource") {
        std::vector<std::string> mqttConfig = UtilityFunctions::splitWithStringDelimiter(conf, ";");

        //init inputFormat to default value (JSON). Only flat JSON and CSV format implemented currently
        SourceDescriptor::InputFormat inputFormat = MQTTSourceDescriptor::JSON;
        if (strcasecmp(mqttConfig[4].c_str(), "JSON") == 0) {
            inputFormat = SourceDescriptor::InputFormat::JSON;
        } else if (strcasecmp(mqttConfig[4].c_str(), "CSV") == 0) {
            inputFormat = SourceDescriptor::InputFormat::CSV;
        }

        //Places in mqttConfig provide:
        //0 = serverAddress; 1 = clientId; 2 = user; 3 = topic; 4 = inputFormat (conversion to enum above)
        //5 = qos (conversion to enum above); 6 = cleanSession; 7 = bufferFlushIntervalMs
        long bufferFlushIntervalMs = -1;
        if (mqttConfig.size() > 7) {
            bufferFlushIntervalMs = std::stol(mqttConfig[7]);
        }
        return MQTTSourceDescriptor::create(schema,
                                            mqttConfig[0],
                                            mqttConfig[1],
                                            mqttConfig[2],
                                            mqttConfig[3],
                                            inputFormat,
                                            stoi(mqttConfig[5]),
                                            (strcasecmp("true", mqttConfig[6].c_str()) == 0),
                                            bufferFlushIntervalMs);
#endif
    } else {
        NES_THROW_RUNTIME_ERROR("PhysicalStreamConfig:: source type " + type + " not supported");
        return nullptr;
    }
}
void PhysicalStreamConfig::setSourceFrequency(uint32_t sf) {
    PhysicalStreamConfig::sourceFrequency = std::chrono::milliseconds(sf);
}
void PhysicalStreamConfig::setNumberOfTuplesToProducePerBuffer(uint32_t n) {
    PhysicalStreamConfig::numberOfTuplesToProducePerBuffer = n;
}
void PhysicalStreamConfig::setNumberOfBuffersToProduce(uint32_t n) { PhysicalStreamConfig::numberOfBuffersToProduce = n; }
}// namespace NES
