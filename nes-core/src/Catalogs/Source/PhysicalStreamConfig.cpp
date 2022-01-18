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
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/CSVSourceTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/DefaultSourceTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/MQTTSourceTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/PhysicalStreamTypeConfiguration.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/SenseSourceTypeConfig.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/SourceTypeConfig.hpp>
#include <Configurations/Sources/MaterializedViewSourceConfig.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MaterializedViewSourceDescriptor.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <sstream>
#include <string.h>
#include <utility>
namespace NES {

PhysicalSourcePtr PhysicalStreamConfig::create(const Configurations::PhysicalStreamTypePtr& physicalStreamTypeConfig) {
    return std::make_shared<PhysicalStreamConfig>(PhysicalStreamConfig(physicalStreamTypeConfig));
}

PhysicalSourcePtr PhysicalStreamConfig::createEmpty() {
    return std::make_shared<PhysicalStreamConfig>(PhysicalStreamConfig(Configurations::StreamTypeConfiguration::create()));
}

PhysicalStreamConfig::PhysicalStreamConfig(const Configurations::PhysicalStreamTypePtr& physicalStreamTypeConfig)
    : physicalStreamTypeConfig(physicalStreamTypeConfig) {
    NES_INFO("PhysicalStreamConfig: Created source with config: " << physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->toString());
};

std::string PhysicalStreamConfig::toString() {
    std::stringstream ss;
    ss << physicalStreamTypeConfig->toString();
    return ss.str();
}

SourceDescriptorPtr PhysicalStreamConfig::build(SchemaPtr schema) {
    // todo add handling for support of multiple physical streams.
    if (physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->getSourceType()->getValue() == "DefaultSource") {
        NES_DEBUG("PhysicalStreamConfig: create default source for one buffer");
        return DefaultSourceDescriptor::create(schema,
                                               physicalStreamTypeConfig->getLogicalStreamName()->getValue(),
            physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()
                                                   ->as<Configurations::DefaultSourceTypeConfig>()
                                                   ->getNumberOfBuffersToProduce()
                                                   ->getValue(),
                                               std::chrono::milliseconds(physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()
                                                                             ->as<Configurations::DefaultSourceTypeConfig>()
                                                                             ->getSourceFrequency()
                                                                             ->getValue())
                                                   .count());
    }
    if (physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->getSourceType()->getValue() == "CSVSource") {
        NES_DEBUG("PhysicalStreamConfig: create CSV source for " << physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()
                                                                        ->as<Configurations::CSVSourceTypeConfig>()
                                                                        ->getFilePath()
                                                                        ->getValue()
                                                                 << " buffers");
        return CsvSourceDescriptor::create(
            schema,
            physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->as<Configurations::CSVSourceTypeConfig>());
    } else if (physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->getSourceType()->getValue() == "SenseSource") {
        NES_DEBUG("PhysicalStreamConfig: create Sense source for udfs " << physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()
                                                                               ->as<Configurations::SenseSourceTypeConfig>()
                                                                               ->getUdfs()
                                                                               ->getValue());
        return SenseSourceDescriptor::create(
            schema,
            physicalStreamTypeConfig->getLogicalStreamName()->getValue(),
            /**udfs*/
                                             physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->as<Configurations::SenseSourceTypeConfig>()->getUdfs()->getValue());
#ifdef ENABLE_MQTT_BUILD
    } else if (physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->getSourceType()->getValue() == "MQTTSource") {
        NES_DEBUG("PhysicalStreamConfig: create MQTT source with configurations: "
                  << physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->toString());

        //init inputFormat to default value (JSON). Only flat JSON and CSV format implemented currently
        SourceDescriptor::InputFormat inputFormatEnum = MQTTSourceDescriptor::JSON;
        if (strcasecmp(physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()
                           ->as<Configurations::MQTTSourceTypeConfig>()
                           ->getInputFormat()
                           ->getValue()
                           .c_str(),
                       "JSON")
            == 0) {
            inputFormatEnum = SourceDescriptor::InputFormat::JSON;
        } else if (strcasecmp(physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()
                                  ->as<Configurations::MQTTSourceTypeConfig>()
                                  ->getInputFormat()
                                  ->getValue()
                                  .c_str(),
                              "CSV")
                   == 0) {
             inputFormatEnum = SourceDescriptor::InputFormat::CSV;
        }

        return MQTTSourceDescriptor::create(
            schema,
            physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->as<Configurations::MQTTSourceTypeConfig>(),
            inputFormatEnum);
#endif
    } else if (sourceConfig->getSourceType()->getValue() == "MaterializedViewSource") {
        NES_DEBUG("PhysicalStreamConfig: create materialized view source with configuration: " << sourceConfig->toString());
        return Experimental::MaterializedView::MaterializedViewSourceDescriptor::create(
                schema,
                sourceConfig->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceConfig>()->getId()->getValue());
    } else {
        NES_THROW_RUNTIME_ERROR("PhysicalStreamConfig:: source type "
                                + physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->getSourceType()->getValue()
                                + " not supported");
        return nullptr;
    }
}
Configurations::PhysicalStreamTypePtr PhysicalStreamConfig::getPhysicalStreamTypeConfig() {
    return physicalStreamTypeConfig;
}

}// namespace NES
