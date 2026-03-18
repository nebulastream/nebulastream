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
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

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
} // namespace

namespace YAML
{

bool convert<NES::Validator::SchemaField>::decode(const Node& node, NES::Validator::SchemaField& rhs)
{
    rhs.name = bindIdentifierName(node["name"].as<std::string>());
    rhs.type = stringToFieldType(node["type"].as<std::string>());
    return true;
}

bool convert<NES::Validator::SinkConfig>::decode(const Node& node, NES::Validator::SinkConfig& rhs)
{
    rhs.name = bindIdentifierName(node["name"].as<std::string>());
    rhs.type = node["type"].as<std::string>();
    rhs.schema = node["schema"] ? node["schema"].as<std::vector<NES::Validator::SchemaField>>() : std::vector<NES::Validator::SchemaField>{};
    rhs.config = node["config"] ? node["config"].as<std::unordered_map<std::string, std::string>>() : std::unordered_map<std::string, std::string>{};
    rhs.parserConfig = node["parser_config"] ? node["parser_config"].as<std::unordered_map<std::string, std::string>>() : std::unordered_map<std::string, std::string>{};
    return true;
}

bool convert<NES::Validator::LogicalSourceConfig>::decode(const Node& node, NES::Validator::LogicalSourceConfig& rhs)
{
    rhs.name = bindIdentifierName(node["name"].as<std::string>());
    rhs.schema = node["schema"].as<std::vector<NES::Validator::SchemaField>>();
    return true;
}

bool convert<NES::Validator::PhysicalSourceConfig>::decode(const Node& node, NES::Validator::PhysicalSourceConfig& rhs)
{
    rhs.logical = bindIdentifierName(node["logical"].as<std::string>());
    rhs.host = node["host"] ? node["host"].as<std::string>() : "";
    rhs.type = node["type"].as<std::string>();
    rhs.parserConfig = node["parser_config"] ? node["parser_config"].as<std::unordered_map<std::string, std::string>>() : std::unordered_map<std::string, std::string>{};
    rhs.sourceConfig = node["source_config"] ? node["source_config"].as<std::unordered_map<std::string, std::string>>() : std::unordered_map<std::string, std::string>{};
    return true;
}

bool convert<NES::Validator::TopologyConfig>::decode(const Node& node, NES::Validator::TopologyConfig& rhs)
{
    rhs.sinks = node["sinks"] ? node["sinks"].as<std::vector<NES::Validator::SinkConfig>>() : std::vector<NES::Validator::SinkConfig>{};
    rhs.logical = node["logical"] ? node["logical"].as<std::vector<NES::Validator::LogicalSourceConfig>>() : std::vector<NES::Validator::LogicalSourceConfig>{};
    rhs.physical = node["physical"] ? node["physical"].as<std::vector<NES::Validator::PhysicalSourceConfig>>() : std::vector<NES::Validator::PhysicalSourceConfig>{};
    rhs.query = {};
    // Support new format: queries: [{query: "...", name: "..."}]
    if (node["queries"].IsDefined() && node["queries"].IsSequence())
    {
        for (const auto& q : node["queries"])
        {
            if (q["query"].IsDefined())
            {
                rhs.query.emplace_back(q["query"].as<std::string>());
            }
        }
    }
    // Support old format: query: "..." or query: ["..."]
    else if (node["query"].IsDefined())
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
