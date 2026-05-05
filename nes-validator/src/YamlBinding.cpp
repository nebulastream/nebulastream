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
#include <cstddef>
#include <initializer_list>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

/// All helpers and decode logic in this file mirror nes-frontend/apps/cli/CLIStarter.cpp
/// exactly. The validator MUST accept and reject the same YAML the CLI does so that
/// editor-side validation matches CLI behavior. When CLI's YAML schema changes, this
/// file must change in lockstep.
namespace
{
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

thread_local std::vector<std::string> yamlPath;

struct YamlPathGuard
{
    explicit YamlPathGuard(std::string segment) { yamlPath.push_back(std::move(segment)); }

    ~YamlPathGuard() { yamlPath.pop_back(); }

    YamlPathGuard(const YamlPathGuard&) = delete;
    YamlPathGuard& operator=(const YamlPathGuard&) = delete;
    YamlPathGuard(YamlPathGuard&&) = delete;
    YamlPathGuard& operator=(YamlPathGuard&&) = delete;
};

std::string currentYamlPath()
{
    std::string result;
    for (const auto& segment : yamlPath)
    {
        if (!result.empty() && segment[0] != '[')
        {
            result += '.';
        }
        result += segment;
    }
    return result.empty() ? "<root>" : result;
}

std::string formatMark(const YAML::Mark& mark)
{
    if (mark.is_null())
    {
        return "";
    }
    return fmt::format(" (line {})", mark.line + 1);
}

template <typename T>
T getValue(const YAML::Node& node, const std::string& key)
{
    const YamlPathGuard guard(key);
    if (!node[key])
    {
        throw NES::InvalidConfigParameter("Missing required key '{}' at {}{}", key, currentYamlPath(), formatMark(node.Mark()));
    }
    try
    {
        return node[key].as<T>();
    }
    catch (const YAML::Exception&)
    {
        throw NES::InvalidConfigParameter("Invalid value for '{}' at {}{}", key, currentYamlPath(), formatMark(node[key].Mark()));
    }
}

template <typename T>
std::vector<T> getList(const YAML::Node& node, const std::string& key)
{
    const YamlPathGuard guard(key);
    if (!node[key])
    {
        throw NES::InvalidConfigParameter("Missing required key '{}' at {}{}", key, currentYamlPath(), formatMark(node.Mark()));
    }
    if (!node[key].IsSequence())
    {
        throw NES::InvalidConfigParameter("Expected a list at {}{}", currentYamlPath(), formatMark(node[key].Mark()));
    }
    std::vector<T> result;
    for (std::size_t i = 0; i < node[key].size(); ++i)
    {
        const YamlPathGuard indexGuard("[" + std::to_string(i) + "]");
        try
        {
            result.push_back(node[key][i].as<T>());
        }
        catch (const YAML::Exception&)
        {
            throw NES::InvalidConfigParameter("Invalid value at {}{}", currentYamlPath(), formatMark(node[key][i].Mark()));
        }
    }
    return result;
}

template <typename T>
T getOrDefault(const YAML::Node& node, const std::string& key, T defaultValue = T{})
{
    if (!node[key])
    {
        return defaultValue;
    }
    const YamlPathGuard guard(key);
    try
    {
        return node[key].as<T>();
    }
    catch (const YAML::Exception&)
    {
        throw NES::InvalidConfigParameter("Invalid value for '{}' at {}{}", key, currentYamlPath(), formatMark(node[key].Mark()));
    }
}

template <typename T>
std::optional<T> getOptional(const YAML::Node& node, const std::string& key)
{
    if (!node[key])
    {
        return std::nullopt;
    }
    const YamlPathGuard guard(key);
    try
    {
        return node[key].as<T>();
    }
    catch (const YAML::Exception&)
    {
        throw NES::InvalidConfigParameter("Invalid value for '{}' at {}{}", key, currentYamlPath(), formatMark(node[key].Mark()));
    }
}

template <typename T>
std::vector<T> getListOrDefault(const YAML::Node& node, const std::string& key)
{
    if (!node[key])
    {
        return {};
    }
    return getList<T>(node, key);
}

void acceptKeys(std::initializer_list<std::string_view> allowed, const YAML::Node& node)
{
    if (!node.IsMap())
    {
        return;
    }
    for (const auto& entry : node)
    {
        const auto key = entry.first.as<std::string>();
        if (std::ranges::find(allowed, key) == allowed.end())
        {
            throw NES::InvalidConfigParameter(
                "Unknown key '{}' at {}{}. Expected one of: {}",
                key,
                currentYamlPath(),
                formatMark(entry.first.Mark()),
                fmt::join(allowed, ", "));
        }
    }
}
} // namespace

namespace YAML
{

bool convert<NES::Validator::SchemaField>::decode(const Node& node, NES::Validator::SchemaField& rhs)
{
    acceptKeys({"name", "type"}, node);
    rhs.name = bindIdentifierName(getValue<std::string>(node, "name"));
    rhs.type = stringToFieldType(getValue<std::string>(node, "type"));
    return true;
}

bool convert<NES::Validator::SinkConfig>::decode(const Node& node, NES::Validator::SinkConfig& rhs)
{
    acceptKeys({"name", "type", "schema", "host", "config", "parser_config"}, node);
    rhs.name = bindIdentifierName(getValue<std::string>(node, "name"));
    rhs.type = getValue<std::string>(node, "type");
    rhs.schema = getList<NES::Validator::SchemaField>(node, "schema");
    rhs.host = getValue<std::string>(node, "host");
    rhs.config = getOrDefault<std::unordered_map<std::string, std::string>>(node, "config");
    rhs.parserConfig = getOrDefault<std::unordered_map<std::string, std::string>>(node, "parser_config");
    return true;
}

bool convert<NES::Validator::LogicalSourceConfig>::decode(const Node& node, NES::Validator::LogicalSourceConfig& rhs)
{
    acceptKeys({"name", "schema"}, node);
    rhs.name = bindIdentifierName(getValue<std::string>(node, "name"));
    rhs.schema = getList<NES::Validator::SchemaField>(node, "schema");
    return true;
}

bool convert<NES::Validator::PhysicalSourceConfig>::decode(const Node& node, NES::Validator::PhysicalSourceConfig& rhs)
{
    acceptKeys({"logical", "type", "host", "parser_config", "source_config"}, node);
    rhs.logical = bindIdentifierName(getValue<std::string>(node, "logical"));
    rhs.type = getValue<std::string>(node, "type");
    rhs.host = getValue<std::string>(node, "host");
    rhs.parserConfig = getValue<std::unordered_map<std::string, std::string>>(node, "parser_config");
    rhs.sourceConfig = getOrDefault<std::unordered_map<std::string, std::string>>(node, "source_config");
    return true;
}

bool convert<NES::Validator::WorkerConfig>::decode(const Node& node, NES::Validator::WorkerConfig& rhs)
{
    acceptKeys({"host", "data_address", "max_operators", "downstream", "config"}, node);
    rhs.maxOperators = getOptional<std::size_t>(node, "max_operators");
    rhs.downstream = getOrDefault<std::vector<std::string>>(node, "downstream");
    rhs.host = getValue<std::string>(node, "host");
    rhs.dataAddress = getOrDefault<std::string>(node, "data_address");
    return true;
}

bool convert<NES::Validator::TopologyConfig>::decode(const Node& node, NES::Validator::TopologyConfig& rhs)
{
    acceptKeys({"query", "sinks", "logical", "physical", "optimizer", "workers", "models"}, node);
    rhs.sinks = getListOrDefault<NES::Validator::SinkConfig>(node, "sinks");
    rhs.logical = getList<NES::Validator::LogicalSourceConfig>(node, "logical");
    rhs.physical = getListOrDefault<NES::Validator::PhysicalSourceConfig>(node, "physical");
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
    rhs.workers = getList<NES::Validator::WorkerConfig>(node, "workers");
    return true;
}

} // namespace YAML
