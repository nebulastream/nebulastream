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
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Configurations/Sources/DefaultSourceConfig.hpp>
#include <Configurations/Sources/MQTTSourceConfig.hpp>
#include <Configurations/Sources/SenseSourceConfig.hpp>
#include <Configurations/Sources/SourceConfigFactory.hpp>
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

PhysicalStreamConfigPtr PhysicalStreamConfig::create(const Configurations::SourceConfigPtr& sourceConfig) {
    return std::make_shared<PhysicalStreamConfig>(PhysicalStreamConfig(sourceConfig));
}

PhysicalStreamConfigPtr PhysicalStreamConfig::createEmpty() {
    return std::make_shared<PhysicalStreamConfig>(PhysicalStreamConfig(Configurations::DefaultSourceConfig::create()));
}

PhysicalStreamConfig::PhysicalStreamConfig(const Configurations::SourceConfigPtr& sourceConfigPtr)
    : sourceConfigPtr(sourceConfigPtr),
      numberOfTuplesToProducePerBuffer(sourceConfigPtr->getNumberOfTuplesToProducePerBuffer()->getValue()),
      numberOfBuffersToProduce(sourceConfigPtr->getNumberOfBuffersToProduce()->getValue()),
      physicalStreamName(sourceConfigPtr->getPhysicalStreamName()->getValue()),
      logicalStreamName(sourceConfigPtr->getLogicalStreamName()->getValue()), sourceType(sourceConfigPtr->getSourceType()->getValue()) {
    NES_INFO("PhysicalStreamConfig: Created source with config: " << sourceConfigPtr->toString());
};

std::string PhysicalStreamConfig::toString() {
    std::stringstream ss;
    ss << sourceConfigPtr->toString();
    return ss.str();
}

Configurations::SourceConfigPtr PhysicalStreamConfig::getSourceConfigPtr() const { return sourceConfigPtr; }
std::string PhysicalStreamConfig::getLogicalStreamName() { return logicalStreamName; }
std::string PhysicalStreamConfig::getPhysicalStreamName() { return physicalStreamName; }
uint32_t PhysicalStreamConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }
uint32_t PhysicalStreamConfig::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }
std::string PhysicalStreamConfig::getSourceType() { return sourceType; }

SourceDescriptorPtr PhysicalStreamConfig::build(SchemaPtr schema) {
    // todo add handling for support of multiple physical streams.
    if (sourceConfigPtr->getSourceType()->getValue() == "DefaultSource") {
        NES_DEBUG("PhysicalStreamConfig: create default source for one buffer");
        return DefaultSourceDescriptor::create(schema,
            sourceConfigPtr->getLogicalStreamName()->getValue(),
            sourceConfigPtr->getNumberOfBuffersToProduce()->getValue(),
                                               std::chrono::milliseconds(sourceConfigPtr->getSourceFrequency()->getValue()).count());
    }
    if (sourceConfigPtr->getSourceType()->getValue() == "CSVSource") {
        NES_DEBUG("PhysicalStreamConfig: create CSV source for " << sourceConfigPtr->as<Configurations::CSVSourceConfig>()->getFilePath()->getValue()
                                                                 << " buffers");
        return CsvSourceDescriptor::create(schema,
                                           sourceConfigPtr->as<Configurations::CSVSourceConfig>(),
                                           /**delimiter*/ ",");
    } else if (sourceConfigPtr->getSourceType()->getValue() == "SenseSource") {
        NES_DEBUG("PhysicalStreamConfig: create Sense source for udfs "
                  << sourceConfigPtr->as<Configurations::SenseSourceConfig>()->getUdfs()->getValue());
        return SenseSourceDescriptor::create(schema,
            sourceConfigPtr->getLogicalStreamName()->getValue(),
                                             /**udfs*/ sourceConfigPtr->as<Configurations::SenseSourceConfig>()->getUdfs()->getValue());
#ifdef ENABLE_MQTT_BUILD
    } else if (sourceConfigPtr->getSourceType()->getValue() == "MQTTSource") {
        NES_DEBUG("PhysicalStreamConfig: create MQTT source with configurations: " << sourceConfigPtr->toString());

        //init inputFormat to default value (JSON). Only flat JSON and CSV format implemented currently
        SourceDescriptor::InputFormat inputFormatEnum = MQTTSourceDescriptor::JSON;
        if (strcasecmp(sourceConfigPtr->getInputFormat()->getValue().c_str(), "JSON") == 0) {
            inputFormatEnum = SourceDescriptor::InputFormat::JSON;
        } else if (strcasecmp(sourceConfigPtr->getInputFormat()->getValue().c_str(), "CSV") == 0) {
            inputFormatEnum = SourceDescriptor::InputFormat::CSV;
        }

        return MQTTSourceDescriptor::create(schema, sourceConfigPtr->as<Configurations::MQTTSourceConfig>(), inputFormatEnum);
#endif
    } else {
        NES_THROW_RUNTIME_ERROR("PhysicalStreamConfig:: source type " + sourceConfigPtr->getSourceType()->getValue()
                                + " not supported");
        return nullptr;
    }
}

}// namespace NES
