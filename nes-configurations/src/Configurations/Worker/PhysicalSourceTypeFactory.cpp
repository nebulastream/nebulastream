/*
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

#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/PhysicalSourceFactoryPlugin.hpp>
#include <Configurations/Worker/PhysicalSourceTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/ArrowSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/BinarySourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/MQTTSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/OPCSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/SenseSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PluginRegistry.hpp>

namespace NES::Configurations {

PhysicalSourceTypePtr PhysicalSourceTypeFactory::createFromString(std::string,
                                                                  std::map<std::string, std::string>& commandLineParams) {

    std::string sourceType, logicalSourceName, physicalSourceName;
    for (auto it = commandLineParams.begin(); it != commandLineParams.end(); ++it) {
        if (it->first == SOURCE_TYPE_CONFIG && !it->second.empty()) {
            sourceType = it->second;
        } else if (it->first == PHYSICAL_SOURCE_NAME_CONFIG && !it->second.empty()) {
            physicalSourceName = it->second;
        } else if (it->first == LOGICAL_SOURCE_NAME_CONFIG && !it->second.empty()) {
            logicalSourceName = it->second;
        }
    }

    if (logicalSourceName.empty()) {
        NES_WARNING("No logical source name is not supplied for creating the physical source. Please supply "
                    "logical source name using --{}",
                    LOGICAL_SOURCE_NAME_CONFIG);
        return nullptr;
    } else if (physicalSourceName.empty()) {
        NES_WARNING(
            "No physical source name supplied for creating the physical source. Please supply physical source name using --{}",
            PHYSICAL_SOURCE_NAME_CONFIG);
        return nullptr;
    } else if (sourceType.empty()) {
        NES_WARNING("No source type supplied for creating the physical source. Please supply source type using --{}",
                    SOURCE_TYPE_CONFIG);
        return nullptr;
    }

    return createPhysicalSourceType(logicalSourceName, physicalSourceName, sourceType, commandLineParams);
}

PhysicalSourceTypePtr PhysicalSourceTypeFactory::createFromYaml(Yaml::Node& yamlConfig) {
    NES_ERROR("Creating physical source type from yaml config");
    std::vector<PhysicalSourceTypePtr> physicalSources;
    //Iterate over all physical sources defined in the yaml file
    std::string logicalSourceName, physicalSourceName, sourceType;
    if (!yamlConfig[LOGICAL_SOURCE_NAME_CONFIG].As<std::string>().empty()
        && yamlConfig[LOGICAL_SOURCE_NAME_CONFIG].As<std::string>() != "\n") {
        logicalSourceName = yamlConfig[LOGICAL_SOURCE_NAME_CONFIG].As<std::string>();
    } else {
        NES_THROW_RUNTIME_ERROR("Found Invalid Logical Source Configuration. Please define Logical Source Name.");
    }

    if (!yamlConfig[PHYSICAL_SOURCE_NAME_CONFIG].As<std::string>().empty()
        && yamlConfig[PHYSICAL_SOURCE_NAME_CONFIG].As<std::string>() != "\n") {
        physicalSourceName = yamlConfig[PHYSICAL_SOURCE_NAME_CONFIG].As<std::string>();
    } else {
        NES_THROW_RUNTIME_ERROR("Found Invalid Physical Source Configuration. Please define Physical Source Name.");
    }

    if (!yamlConfig[SOURCE_TYPE_CONFIG].As<std::string>().empty() && yamlConfig[SOURCE_TYPE_CONFIG].As<std::string>() != "\n") {
        sourceType = yamlConfig[SOURCE_TYPE_CONFIG].As<std::string>();
    } else {
        NES_THROW_RUNTIME_ERROR("Found Invalid Physical Source Configuration. Please define Source type.");
    }

    if (yamlConfig[PHYSICAL_SOURCE_TYPE_CONFIGURATION].IsMap()) {
        auto physicalSourceTypeConfiguration = yamlConfig[PHYSICAL_SOURCE_TYPE_CONFIGURATION];
        return createPhysicalSourceType(logicalSourceName, physicalSourceName, sourceType, physicalSourceTypeConfiguration);
    } else {
        NES_THROW_RUNTIME_ERROR(
            "Found Invalid Physical Source Configuration. Please define Source Type Configuration properties.");
    }
    NES_ERROR("Finished physical source type from yaml config");
}

PhysicalSourceTypePtr
PhysicalSourceTypeFactory::createPhysicalSourceType(std::string logicalSourceName,
                                                    std::string physicalSourceName,
                                                    std::string sourceType,
                                                    const std::map<std::string, std::string>& commandLineParams) {

    // check if a plugin is registered to handle this source type
    for (const auto& plugin : PhysicalSourceFactoryPluginRegistry ::getPlugins()) {
        if (auto type = plugin->createPhysicalSourceType(sourceType, commandLineParams)) {
            return type;
        }
    }

    switch (magic_enum::enum_cast<SourceType>(sourceType).value()) {
        case SourceType::CSV_SOURCE: return CSVSourceType::create(logicalSourceName, physicalSourceName, commandLineParams);
        case SourceType::MQTT_SOURCE: return MQTTSourceType::create(logicalSourceName, physicalSourceName, commandLineParams);
        case SourceType::KAFKA_SOURCE: return KafkaSourceType::create(logicalSourceName, physicalSourceName, commandLineParams);
        case SourceType::OPC_SOURCE: return OPCSourceType::create(logicalSourceName, physicalSourceName, commandLineParams);
        case SourceType::BINARY_SOURCE: return BinarySourceType::create(logicalSourceName, physicalSourceName, commandLineParams);
        case SourceType::SENSE_SOURCE: return SenseSourceType::create(logicalSourceName, physicalSourceName, commandLineParams);
        case SourceType::ARROW_SOURCE: return ArrowSourceType::create(logicalSourceName, physicalSourceName, commandLineParams);
        case SourceType::TCP_SOURCE: return TCPSourceType::create(logicalSourceName, physicalSourceName, commandLineParams);
        case SourceType::DEFAULT_SOURCE:
            return DefaultSourceType::create(logicalSourceName, physicalSourceName, commandLineParams);
        default: NES_THROW_RUNTIME_ERROR("SourceConfigFactory:: source type " << sourceType << " not supported");
    }
}

PhysicalSourceTypePtr PhysicalSourceTypeFactory::createPhysicalSourceType(std::string logicalSourceName,
                                                                          std::string physicalSourceName,
                                                                          std::string sourceType,
                                                                          Yaml::Node& yamlConfig) {

    // check a plugin is registered to handle this source type
    for (const auto& plugin : PhysicalSourceFactoryPluginRegistry ::getPlugins()) {
        if (auto type = plugin->createPhysicalSourceType(sourceType, yamlConfig)) {
            return type;
        }
    }

    if (!magic_enum::enum_cast<SourceType>(sourceType).has_value()) {
        NES_THROW_RUNTIME_ERROR("SourceConfigFactory:: source type " << sourceType << " not supported");
    }

    NES_ERROR("Creating physical source type from yaml config");
    switch (magic_enum::enum_cast<SourceType>(sourceType).value()) {
        case SourceType::CSV_SOURCE: return CSVSourceType::create(logicalSourceName, physicalSourceName, yamlConfig);
        case SourceType::MQTT_SOURCE: return MQTTSourceType::create(logicalSourceName, physicalSourceName, yamlConfig);
        case SourceType::KAFKA_SOURCE: return KafkaSourceType::create(logicalSourceName, physicalSourceName, yamlConfig);
        case SourceType::OPC_SOURCE: return OPCSourceType::create(logicalSourceName, physicalSourceName, yamlConfig);
        case SourceType::BINARY_SOURCE: return BinarySourceType::create(logicalSourceName, physicalSourceName, yamlConfig);
        case SourceType::SENSE_SOURCE: return SenseSourceType::create(logicalSourceName, physicalSourceName, yamlConfig);
        case SourceType::DEFAULT_SOURCE: return DefaultSourceType::create(logicalSourceName, physicalSourceName, yamlConfig);
        case SourceType::TCP_SOURCE: return TCPSourceType::create(logicalSourceName, physicalSourceName, yamlConfig);
        case SourceType::ARROW_SOURCE: return ArrowSourceType::create(logicalSourceName, physicalSourceName, yamlConfig);
        default: NES_THROW_RUNTIME_ERROR("SourceConfigFactory:: source type " << sourceType << " not supported");
    }
    NES_ERROR("Finished creating physical source type from yaml config");
}

}// namespace NES::Configurations
