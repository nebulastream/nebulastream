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

#include <filesystem>
#include <fstream>
#include <ranges>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ReadingVisitor.hpp>
#include <Configurations/WritingVisitor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
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
            if (value.empty() || std::all_of(value.begin(), value.end(), ::isspace))
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

void BaseConfiguration::accept(ReadingVisitor& visitor) const
{
    visitor.push(*this);
    for (auto& option : getOptions())
    {
        option->accept(visitor);
    }
    visitor.pop(*this);
};

void BaseConfiguration::accept(WritingVisitor& visitor)
{
    visitor.push(*this);
    for (auto& option : getOptions())
    {
        option->accept(visitor);
    }
    visitor.pop(*this);
}

bool BaseConfiguration::operator==(const BaseOption& other) const
{
    if (const auto* otherBaseConfiguration = dynamic_cast<const BaseConfiguration*>(&other))
    {
        return std::ranges::equal(
            getOptions(), otherBaseConfiguration->getOptions(), [](const auto& lhs, const auto& rhs) { return *lhs == *rhs; });
    }
    return false;
};

std::vector<const BaseOption*> BaseConfiguration::getOptions() const
{
    return const_cast<BaseConfiguration*>(this)->getOptions()
        | std::views::transform([](const auto& ptr) -> const BaseOption* { return ptr; }) | ranges::to<std::vector>();
}


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

}
