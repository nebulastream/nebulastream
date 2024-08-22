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
#include <Configurations/Worker/PhysicalSourceTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PluginRegistry.hpp>

namespace NES::Configurations
{

PhysicalSourceTypePtr PhysicalSourceTypeFactory::createFromString(std::string, std::map<std::string, std::string>& commandLineParams)
{
    std::string sourceType, logicalSourceName;
    for (auto it = commandLineParams.begin(); it != commandLineParams.end(); ++it)
    {
        if (it->first == SOURCE_TYPE_CONFIG && !it->second.empty())
        {
            sourceType = it->second;
        }
        else if (it->first == LOGICAL_SOURCE_NAME_CONFIG && !it->second.empty())
        {
            logicalSourceName = it->second;
        }
    }

    if (logicalSourceName.empty())
    {
        NES_WARNING(
            "No logical source name is not supplied for creating the physical source. Please supply "
            "logical source name using --{}",
            LOGICAL_SOURCE_NAME_CONFIG);
        return nullptr;
    }
    else if (sourceType.empty())
    {
        NES_WARNING("No source type supplied for creating the physical source. Please supply source type using --{}", SOURCE_TYPE_CONFIG);
        return nullptr;
    }

    return createPhysicalSourceType(logicalSourceName, sourceType, commandLineParams);
}

PhysicalSourceTypePtr PhysicalSourceTypeFactory::createFromYaml(Yaml::Node& yamlConfig)
{
    std::vector<PhysicalSourceTypePtr> physicalSources;
    ///Iterate over all physical sources defined in the yaml file
    std::string logicalSourceName, sourceType;
    if (!yamlConfig[LOGICAL_SOURCE_NAME_CONFIG].As<std::string>().empty()
        && yamlConfig[LOGICAL_SOURCE_NAME_CONFIG].As<std::string>() != "\n")
    {
        logicalSourceName = yamlConfig[LOGICAL_SOURCE_NAME_CONFIG].As<std::string>();
    }
    else
    {
        NES_THROW_RUNTIME_ERROR("Found Invalid Logical Source Configuration. Please define Logical Source Name.");
    }

    if (!yamlConfig[SOURCE_TYPE_CONFIG].As<std::string>().empty() && yamlConfig[SOURCE_TYPE_CONFIG].As<std::string>() != "\n")
    {
        sourceType = yamlConfig[SOURCE_TYPE_CONFIG].As<std::string>();
    }
    else
    {
        NES_THROW_RUNTIME_ERROR("Found Invalid Physical Source Configuration. Please define Source type.");
    }

    if (yamlConfig[PHYSICAL_SOURCE_TYPE_CONFIGURATION].IsMap())
    {
        auto physicalSourceTypeConfiguration = yamlConfig[PHYSICAL_SOURCE_TYPE_CONFIGURATION];
        return createPhysicalSourceType(logicalSourceName, sourceType, physicalSourceTypeConfiguration);
    }
    else
    {
        NES_THROW_RUNTIME_ERROR("Found Invalid Physical Source Configuration. Please define Source Type Configuration properties.");
    }
}

PhysicalSourceTypePtr PhysicalSourceTypeFactory::createPhysicalSourceType(
    std::string logicalSourceName, std::string sourceType, const std::map<std::string, std::string>& commandLineParams)
{
    switch (magic_enum::enum_cast<SourceType>(sourceType).value())
    {
        case SourceType::CSV_SOURCE:
            return CSVSourceType::create(logicalSourceName, commandLineParams);
        case SourceType::TCP_SOURCE:
            return TCPSourceType::create(logicalSourceName, commandLineParams);
        default:
            NES_THROW_RUNTIME_ERROR("SourceConfigFactory:: source type " << sourceType << " not supported");
    }
}

PhysicalSourceTypePtr
PhysicalSourceTypeFactory::createPhysicalSourceType(std::string logicalSourceName, std::string sourceType, Yaml::Node& yamlConfig)
{
    if (!magic_enum::enum_cast<SourceType>(sourceType).has_value())
    {
        NES_THROW_RUNTIME_ERROR("SourceConfigFactory:: source type " << sourceType << " not supported");
    }

    switch (magic_enum::enum_cast<SourceType>(sourceType).value())
    {
        case SourceType::CSV_SOURCE:
            return CSVSourceType::create(logicalSourceName, yamlConfig);
        case SourceType::TCP_SOURCE:
            return TCPSourceType::create(logicalSourceName, yamlConfig);
        default:
            NES_THROW_RUNTIME_ERROR("SourceConfigFactory:: source type " << sourceType << " not supported");
    }
}

} /// namespace NES::Configurations
