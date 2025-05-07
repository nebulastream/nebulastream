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

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/OptionVisitor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <yaml-cpp/node/parse.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>

namespace NES::Configurations
{

BaseConfiguration::BaseConfiguration(const std::string& name, const std::string& description) : BaseOption(name, description) { };

void BaseConfiguration::parseFromYAMLNode(const YAML::Node config)
{
    auto optionMap = getOptionMap();
    if (!config.IsMap())
    {
        throw InvalidConfigParameter("Malformed YAML configuration: {}", name);
    }
    for (auto entry : config)
    {
        auto identifier = entry.first.as<std::string>();
        auto node = entry.second;
        if (!optionMap.contains(identifier))
        {
            throw InvalidConfigParameter("Identifier: {} is not known. Check if it exposed in the getOptions function.", identifier);
        }
        /// check if config is empty
        if (node.IsScalar())
        {
            auto value = node.as<std::string>();
            if (value.empty() || std::ranges::all_of(value, ::isspace))
            {
                throw InvalidConfigParameter("Value for: {} is empty.", identifier);
            }
        }
        else if ((node.IsSequence() || node.IsMap()) && node.size() == 0)
        {
            /// if the node is a sequence or map and has no elements
            throw InvalidConfigParameter("Value for: {} is empty.", identifier);
        }
        try
        {
            optionMap[identifier]->parseFromYAMLNode(node);
        }
        catch (const Exception& e)
        {
            NES_ERROR("Configuration error: ", e.what());
            throw;
        }
    }
}

void BaseConfiguration::parseFromString(std::string identifier, std::unordered_map<std::string, std::string>& inputParams)
{
    auto optionMap = getOptionMap();

    if (!optionMap.contains(identifier))
    {
        throw InvalidConfigParameter("Identifier for: {} is not known.", identifier);
    }
    if (auto* option = dynamic_cast<BaseConfiguration*>(optionMap[identifier]))
    {
        option->overwriteConfigWithCommandLineInput(inputParams);
    }
    else
    {
        try
        {
            optionMap[identifier]->parseFromString(identifier, inputParams);
        }
        catch (const Exception& e)
        {
            NES_ERROR("Configuration error: ", e.what());
            throw;
        }
    }
}

void BaseConfiguration::overwriteConfigWithYAMLFileInput(const std::string& filePath)
{
    try
    {
        YAML::Node config = YAML::LoadFile(filePath);
        if (config.IsNull())
        {
            return;
        }
        parseFromYAMLNode(config);
    }
    catch (const std::exception& ex)
    {
        throw CannotLoadConfig("Exception while loading configurations from: {}. Exception: {}", filePath, ex.what());
    }
}

void BaseConfiguration::overwriteConfigWithCommandLineInput(const std::unordered_map<std::string, std::string>& inputParams)
{
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> groupedIdentifiers;
    for (const auto& [id, value] : inputParams)
    {
        std::string identifier{id};
        const std::string identifierStart = "--";
        if (identifier.starts_with(identifierStart))
        {
            /// remove the -- in the beginning
            identifier = identifier.substr(identifierStart.size());
        }

        if (identifier.find('.') != std::string::npos)
        {
            auto index = std::string(identifier).find('.');
            auto parentIdentifier = std::string(identifier).substr(0, index);
            auto childrenIdentifier = std::string(identifier).substr(index + 1, identifier.length());
            groupedIdentifiers[parentIdentifier].insert({childrenIdentifier, value});
        }
        else
        {
            groupedIdentifiers[identifier].insert({identifier, value});
        }
    }

    for (auto [identifier, values] : groupedIdentifiers)
    {
        parseFromString(identifier, values);
    }
}

std::string BaseConfiguration::toString()
{
    std::stringstream ss;
    for (auto option : getOptions())
    {
        ss << option->toString() << "\n";
    }
    return ss.str();
}

void BaseConfiguration::clear()
{
    for (auto* option : getOptions())
    {
        option->clear();
    }
};

void BaseConfiguration::accept(OptionVisitor& visitor)
{
    if (!name.empty())
    {
        visitor.visitConcrete(getName(), getDescription(), "");
    }
    for (auto& option : getOptions())
    {
        visitor.push();
        option->accept(visitor);
        visitor.pop();
    }
};

std::unordered_map<std::string, Configurations::BaseOption*> BaseConfiguration::getOptionMap()
{
    std::unordered_map<std::string, Configurations::BaseOption*> optionMap;
    for (auto* option : getOptions())
    {
        auto identifier = option->getName();
        optionMap[identifier] = option;
    }
    return optionMap;
}

bool BaseConfiguration::persistWorkerIdInYamlConfigFile(std::string yamlFilePath, WorkerId workerId, bool withOverwrite)
{
    std::ifstream configFile(yamlFilePath);
    std::stringstream ss;
    std::string searchKey = "workerId: ";

    if (!withOverwrite)
    {
        std::string yamlValueAsString = workerId.toString();
        std::string yamlConfigValue = "\n" + searchKey + yamlValueAsString;

        if (!yamlFilePath.empty())
        {
            if (!std::filesystem::exists(yamlFilePath))
            {
                NES_WARNING("Worker.yaml was not found. Creating a new file.");
            }
            configFile >> ss.rdbuf();
            try
            {
                std::ofstream output;
                output.open(yamlFilePath, std::ios::app); /// append mode
                output << yamlConfigValue;
            }
            catch (const std::exception& e)
            {
                throw InvalidConfigParameter("Exception while persisting in yaml file", e.what());
            }
        }
        else
        {
            NES_ERROR("BaseConfiguration: yamlFilePath is empty.");
            return false;
        }
    }
    else
    {
        ss << configFile.rdbuf();
        std::string yamlContent = ss.str();

        size_t startPos = yamlContent.find(searchKey);
        if (startPos != std::string::npos)
        {
            /// move the position to the start of the value
            startPos += searchKey.size();
            /// find the end of the line
            size_t endPos = yamlContent.find('\n', startPos);
            /// replace the old value with the new value for workerId
            yamlContent.replace(startPos, endPos - startPos, workerId.toString());
        }
        else
        {
            return false;
        }

        std::ofstream output(yamlFilePath);
        output << yamlContent;
    }
    configFile.close();
    return true;
}

}
