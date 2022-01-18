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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/BinarySourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MQTTSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/OPCSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/SenseSourceType.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/PhysicalSourceFactory.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/rapidyaml.hpp>

namespace NES {

namespace Configurations {

PhysicalSourcePtr PhysicalSourceFactory::createSourceConfig(const std::map<std::string, std::string>& commandLineParams) {
    if (commandLineParams.contains("--" + SOURCE_TYPE_CONFIG)) {
        if (commandLineParams.contains("--" + PHYSICAL_SOURCE_NAME_CONFIG)) {
            if (commandLineParams.contains("--" + LOGICAL_SOURCE_NAME_CONFIG)) {
                std::string logicalStreamName = commandLineParams.at(LOGICAL_SOURCE_NAME_CONFIG);
                std::string physicalStreamName = commandLineParams.at(PHYSICAL_SOURCE_NAME_CONFIG);
                std::string sourceType = commandLineParams.at(SOURCE_TYPE_CONFIG);
                auto physicalSourceType = createPhysicalSourceType(sourceType, commandLineParams);
                return PhysicalSource::create(logicalStreamName, physicalStreamName, physicalSourceType);
            } else {
                NES_THROW_RUNTIME_ERROR(
                    "No logical source name supplied for creating the physical source. Please supply logical source name using --"
                    << LOGICAL_SOURCE_NAME_CONFIG);
            }
        } else {
            NES_THROW_RUNTIME_ERROR(
                "No physical source name supplied for creating the physical source. Please supply physical source name using --"
                << PHYSICAL_SOURCE_NAME_CONFIG);
        }
    }
    return nullptr;
}

std::vector<PhysicalSourcePtr> PhysicalSourceFactory::createPhysicalSources(ryml::NodeRef yamlConfig) {

    std::vector<PhysicalSourcePtr> physicalSources;

    for (ryml::NodeRef const& child : yamlConfig) {

        std::string logicalSourceName, physicalSourceName, sourceType;

        if (child.find_child(ryml::to_csubstr(LOGICAL_SOURCE_NAME_CONFIG)).has_val()) {
            logicalSourceName = child.find_child(ryml::to_csubstr(LOGICAL_SOURCE_NAME_CONFIG)).val().str;
        } else {
            NES_THROW_RUNTIME_ERROR("Found Invalid Logical Source Configuration. Please define Logical Source Name.");
        }

        if (child.find_child(ryml::to_csubstr(PHYSICAL_SOURCE_NAME_CONFIG)).has_val()) {
            physicalSourceName = child.find_child(ryml::to_csubstr(PHYSICAL_SOURCE_NAME_CONFIG)).val().str;
        } else {
            NES_THROW_RUNTIME_ERROR("Found Invalid Physical Source Configuration. Please define Physical Source Name.");
        }

        if (child.find_child(ryml::to_csubstr(SOURCE_TYPE_CONFIG)).has_val()) {
            sourceType = child.find_child(ryml::to_csubstr(SOURCE_TYPE_CONFIG)).val().str;
        } else {
            NES_THROW_RUNTIME_ERROR("Found Invalid Physical Source Configuration. Please define Source type.");
        }

        auto physicalSourceType = createPhysicalSourceType(sourceType, child);

        physicalSources.emplace_back(PhysicalSource::create(logicalSourceName, physicalSourceName, physicalSourceType));
    }

    return physicalSources;
}

PhysicalSourceTypePtr
PhysicalSourceFactory::createPhysicalSourceType(std::string sourceType,
                                                const std::map<std::string, std::string>& commandLineParams) {
    switch (stringToSourceType[sourceType]) {
        case CSV_SOURCE: return CSVSourceType::create(commandLineParams);
        case MQTT_SOURCE: return MQTTSourceType::create(commandLineParams);
        case KAFKA_SOURCE: return KafkaSourceType::create(commandLineParams);
        case OPC_SOURCE: return OPCSourceType::create(commandLineParams);
        case BINARY_SOURCE: return BinarySourceType::create(commandLineParams);
        case SENSE_SOURCE: return SenseSourceType::create(commandLineParams);
        case DEFAULT_SOURCE: return DefaultSourceType::create(commandLineParams);
        default: NES_THROW_RUNTIME_ERROR("SourceConfigFactory:: source type " << sourceType << " not supported");
    }
}

PhysicalSourceTypePtr PhysicalSourceFactory::createPhysicalSourceType(std::string sourceType, ryml::NodeRef yamlConfig) {
    switch (stringToSourceType[sourceType]) {
        case CSV_SOURCE: return CSVSourceType::create(yamlConfig);
        case MQTT_SOURCE: return MQTTSourceType::create(yamlConfig);
        case KAFKA_SOURCE: return KafkaSourceType::create(yamlConfig);
        case OPC_SOURCE: return OPCSourceType::create(yamlConfig);
        case BINARY_SOURCE: return BinarySourceType::create(yamlConfig);
        case SENSE_SOURCE: return SenseSourceType::create(yamlConfig);
        case DEFAULT_SOURCE: return DefaultSourceType::create(yamlConfig);
        default: NES_THROW_RUNTIME_ERROR("SourceConfigFactory:: source type " << sourceType << " not supported");
    }
}

}// namespace Configurations
}// namespace NES