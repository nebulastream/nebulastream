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

#include "YamlBinding.hpp"

#include <algorithm>
#include <cctype>
#include <initializer_list>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace
{
/// Convert a string type name to a NES DataType.
/// Ported from CLIStarter.cpp::stringToFieldType.
NES::DataType stringToFieldType(const std::string& fieldNodeType)
{
    try
    {
        return NES::DataTypeProvider::provideDataType(fieldNodeType);
    }
    catch (std::runtime_error&)
    {
        throw NES::SLTWrongSchema("Found invalid logical source configuration. {} is not a proper datatype.", fieldNodeType);
    }
}

/// Bind and validate an identifier name.
/// Ported from CLIStarter.cpp::bindIdentifierName.
std::string bindIdentifierName(std::string_view identifier)
{
    auto verifyAllowedCharacters = [](std::string_view potentiallyInvalid)
    {
        if (!std::ranges::all_of(
                potentiallyInvalid, [](char character) { return std::isalnum(character) || character == '_' || character == '$'; }))
        {
            throw NES::InvalidIdentifier("{}", potentiallyInvalid);
        }
    };

    if (identifier.size() > 2 && identifier.starts_with('`') && identifier.ends_with('`'))
    {
        verifyAllowedCharacters(identifier.substr(1, identifier.size() - 2));
        return std::string(identifier.substr(1, identifier.size() - 2));
    }

    verifyAllowedCharacters(identifier);
    return NES::toUpperCase(identifier);
}

/// Validates that a YAML map node contains only the expected keys.
/// Throws InvalidConfigParameter if an unknown key is found.
/// Ported from CLIStarter.cpp::acceptKeys.
void acceptKeys(std::initializer_list<std::string_view> allowed, const YAML::Node& node)
{
    if (!node.IsMap())
    {
        return;
    }
    for (const auto& entry : node)
    {
        const auto key = entry.first.as<std::string>();
        if (std::find(allowed.begin(), allowed.end(), key) == allowed.end())
        {
            throw NES::InvalidConfigParameter("Unknown key '{}'. Expected one of: {}", key, fmt::join(allowed, ", "));
        }
    }
}
} // namespace

namespace YAML
{

bool convert<NES::Validator::SchemaField>::decode(const Node& node, NES::Validator::SchemaField& rhs)
{
    acceptKeys({"name", "type"}, node);
    rhs.name = bindIdentifierName(node["name"].as<std::string>());
    rhs.type = stringToFieldType(node["type"].as<std::string>());
    return true;
}

bool convert<NES::Validator::SinkConfig>::decode(const Node& node, NES::Validator::SinkConfig& rhs)
{
    acceptKeys({"name", "type", "schema", "host", "config", "parser_config"}, node);
    rhs.name = bindIdentifierName(node["name"].as<std::string>());
    rhs.type = node["type"].as<std::string>();
    rhs.schema = node["schema"].as<std::vector<NES::Validator::SchemaField>>();
    rhs.host = node["host"].as<std::string>();
    rhs.config = node["config"].as<std::unordered_map<std::string, std::string>>();
    rhs.parserConfig = node["parser_config"].as<std::unordered_map<std::string, std::string>>();
    return true;
}

bool convert<NES::Validator::LogicalSourceConfig>::decode(const Node& node, NES::Validator::LogicalSourceConfig& rhs)
{
    acceptKeys({"name", "schema"}, node);
    rhs.name = bindIdentifierName(node["name"].as<std::string>());
    rhs.schema = node["schema"].as<std::vector<NES::Validator::SchemaField>>();
    return true;
}

bool convert<NES::Validator::PhysicalSourceConfig>::decode(const Node& node, NES::Validator::PhysicalSourceConfig& rhs)
{
    acceptKeys({"logical", "type", "host", "parser_config", "source_config"}, node);
    rhs.logical = bindIdentifierName(node["logical"].as<std::string>());
    rhs.type = node["type"].as<std::string>();
    rhs.host = node["host"].as<std::string>();
    rhs.parserConfig = node["parser_config"].as<std::unordered_map<std::string, std::string>>();
    rhs.sourceConfig = node["source_config"].as<std::unordered_map<std::string, std::string>>();
    return true;
}

bool convert<NES::Validator::WorkerConfig>::decode(const Node& node, NES::Validator::WorkerConfig& rhs)
{
    acceptKeys({"host", "data", "max_operators", "downstream", "config"}, node);
    rhs.host = node["host"].as<std::string>();
    rhs.data = node["data"].IsDefined() ? node["data"].as<std::string>() : "";
    if (node["max_operators"].IsDefined())
    {
        rhs.maxOperators = node["max_operators"].as<size_t>();
    }
    if (node["downstream"].IsDefined())
    {
        rhs.downstream = node["downstream"].as<std::vector<std::string>>();
    }
    return true;
}

bool convert<NES::Validator::TopologyConfig>::decode(const Node& node, NES::Validator::TopologyConfig& rhs)
{
    acceptKeys({"query", "sinks", "logical", "physical", "optimizer", "workers"}, node);
    rhs.sinks = node["sinks"].as<std::vector<NES::Validator::SinkConfig>>();
    rhs.logical = node["logical"].as<std::vector<NES::Validator::LogicalSourceConfig>>();
    rhs.physical = node["physical"].as<std::vector<NES::Validator::PhysicalSourceConfig>>();
    rhs.workers = node["workers"].as<std::vector<NES::Validator::WorkerConfig>>();
    rhs.query = {};
    if (node["query"].IsDefined())
    {
        if (node["query"].IsSequence())
        {
            rhs.query = node["query"].as<std::vector<std::string>>();
        }
        else
        {
            rhs.query.emplace_back(node["query"].as<std::string>());
        }
    }
    return true;
}

} // namespace YAML
