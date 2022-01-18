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

#include "Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp"
#include <Util/Logger.hpp>

namespace NES {

SourceDescriptorPtr PhysicalStreamConfig::build(SchemaPtr schema) {
    // todo add handling for support of multiple physical streams.
    if (physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->getSourceType()->getValue() == "DefaultSource") {
        NES_DEBUG("PhysicalSourceType: create default source for one buffer");
        return DefaultSourceDescriptor::create(
            schema,
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
        NES_DEBUG("PhysicalSourceType: create CSV source for " << physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()
                                                                      ->as<Configurations::CSVSourceTypeConfig>()
                                                                      ->getFilePath()
                                                                      ->getValue()
                                                               << " buffers");
        return CsvSourceDescriptor::create(
            schema,
            physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->as<Configurations::CSVSourceTypeConfig>());
    } else if (physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->getSourceType()->getValue() == "SenseSource") {
        NES_DEBUG("PhysicalSourceType: create Sense source for udfs "
                  << physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()
                         ->as<Configurations::SenseSourceTypeConfig>()
                         ->getUdfs()
                         ->getValue());
        return SenseSourceDescriptor::create(schema,
                                             physicalStreamTypeConfig->getLogicalStreamName()->getValue(),
                                             /**udfs*/
                                             physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()
                                                 ->as<Configurations::SenseSourceTypeConfig>()
                                                 ->getUdfs()
                                                 ->getValue());
#ifdef ENABLE_MQTT_BUILD
    } else if (physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->getSourceType()->getValue() == "MQTTSource") {
        NES_DEBUG("PhysicalSourceType: create MQTT source with configurations: "
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
    } else {
        NES_THROW_RUNTIME_ERROR("PhysicalSourceType:: source type "
                                + physicalStreamTypeConfig->getPhysicalStreamTypeConfiguration()->getSourceType()->getValue()
                                + " not supported");
        return nullptr;
    }
}

PhysicalSourceType::PhysicalSourceType(SourceType sourceType) : sourceType(sourceType) {}

SourceType PhysicalSourceType::getSourceType() { return sourceType; }

std::string PhysicalSourceType::getSourceTypeAsString() { return sourceTypeToString[sourceType]; }
}// namespace NES
