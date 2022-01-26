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

namespace NES {

namespace Configurations {

PhysicalSourcePtr PhysicalSourceFactory::createPhysicalSource(const std::map<std::string, std::string>& commandLineParams) {

    std::string sourceType, logicalSourceName, physicalSourceName;
    for (auto it = commandLineParams.begin(); it != commandLineParams.end(); ++it) {
        if (it->first == "--" + SOURCE_TYPE_CONFIG && !it->second.empty()) {
            sourceType = it->second;
        } else if (it->first == "--" + PHYSICAL_SOURCE_NAME_CONFIG && !it->second.empty()) {
            physicalSourceName = it->second;
        } else if (it->first == "--" + LOGICAL_SOURCE_NAME_CONFIG && !it->second.empty()) {
            logicalSourceName = it->second;
        }
    }

    if (logicalSourceName.empty()) {
        NES_WARNING("No logical source name is not supplied for creating the physical source. Please supply "
                                "logical source name using --"
                                << LOGICAL_SOURCE_NAME_CONFIG);
        return nullptr;
    } else if (physicalSourceName.empty()) {
        NES_WARNING(
            "No physical source name supplied for creating the physical source. Please supply physical source name using --"
            << PHYSICAL_SOURCE_NAME_CONFIG);
        return nullptr;
    } else if (sourceType.empty()) {
        NES_WARNING("No source type supplied for creating the physical source. Please supply source type using --"
                                << SOURCE_TYPE_CONFIG);
        return nullptr;
    }

    auto physicalSourceType = createPhysicalSourceType(sourceType, commandLineParams);
    return PhysicalSource::create(logicalSourceName, physicalSourceName, physicalSourceType);
}

std::vector<PhysicalSourcePtr> PhysicalSourceFactory::createPhysicalSources(Yaml::Node& yamlConfig) {
    std::vector<PhysicalSourcePtr> physicalSources;
    //Iterate over all physical sources defined in the yaml file
    for (uint16_t i = 0; i < yamlConfig.Size(); i++) {

        std::string logicalSourceName, physicalSourceName, sourceType;
        if (!yamlConfig[i][LOGICAL_SOURCE_NAME_CONFIG].As<std::string>().empty()
            && yamlConfig[i][LOGICAL_SOURCE_NAME_CONFIG].As<std::string>() != "\n") {
            logicalSourceName = yamlConfig[i][LOGICAL_SOURCE_NAME_CONFIG].As<std::string>();
        } else {
            NES_THROW_RUNTIME_ERROR("Found Invalid Logical Source Configuration. Please define Logical Source Name.");
        }

        if (!yamlConfig[i][PHYSICAL_SOURCE_NAME_CONFIG].As<std::string>().empty()
            && yamlConfig[i][PHYSICAL_SOURCE_NAME_CONFIG].As<std::string>() != "\n") {
            physicalSourceName = yamlConfig[i][PHYSICAL_SOURCE_NAME_CONFIG].As<std::string>();
        } else {
            NES_THROW_RUNTIME_ERROR("Found Invalid Physical Source Configuration. Please define Physical Source Name.");
        }

        if (!yamlConfig[i][SOURCE_TYPE_CONFIG].As<std::string>().empty()
            && yamlConfig[i][SOURCE_TYPE_CONFIG].As<std::string>() != "\n") {
            sourceType = yamlConfig[i][SOURCE_TYPE_CONFIG].As<std::string>();
        } else {
            NES_THROW_RUNTIME_ERROR("Found Invalid Physical Source Configuration. Please define Source type.");
        }

        if (yamlConfig[i][PHYSICAL_SOURCE_TYPE_CONFIGURATION].IsMap()) {
            auto physicalSourceTypeConfiguration = yamlConfig[i][PHYSICAL_SOURCE_TYPE_CONFIGURATION];
            auto physicalSourceType = createPhysicalSourceType(sourceType, physicalSourceTypeConfiguration);
            physicalSources.emplace_back(PhysicalSource::create(logicalSourceName, physicalSourceName, physicalSourceType));
        } else {
            NES_THROW_RUNTIME_ERROR(
                "Found Invalid Physical Source Configuration. Please define Source Type Configuration properties.");
        }
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
        case MATERIALIZEDVIEW_SOURCE: return DefaultSourceType::create(commandLineParams);
        default: NES_THROW_RUNTIME_ERROR("SourceConfigFactory:: source type " << sourceType << " not supported");
    }
}

PhysicalSourceTypePtr PhysicalSourceFactory::createPhysicalSourceType(std::string sourceType, Yaml::Node& yamlConfig) {

    if (stringToSourceType.find(sourceType) == stringToSourceType.end()) {
        NES_THROW_RUNTIME_ERROR("SourceConfigFactory:: source type " << sourceType << " not supported");
    }

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