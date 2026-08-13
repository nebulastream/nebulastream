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

#include <Configurations/ConfigParsing.hpp>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sstream>
#include <variant>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <yaml-cpp/yaml.h> /// NOLINT(misc-include-cleaner) - full definitions required for YAML node iteration
#include <ErrorHandling.hpp>

#include <Configurations/ConfigField.hpp>

namespace
{

void flattenYAMLNode(const YAML::Node& node, const std::string& prefix, std::vector<NES::LiteralConfigValue>& values)
{
    if (!node.IsMap() || node.size() == 0)
    {
        throw NES::InvalidConfigParameter("Malformed YAML configuration: expected a non-empty map at '{}'", prefix);
    }
    for (const auto& entry : node)
    {
        const auto key = entry.first.as<std::string>();
        const auto qualifiedName = prefix.empty() ? key : fmt::format("{}.{}", prefix, key);
        if (const auto& value = entry.second; value.IsMap())
        {
            flattenYAMLNode(value, qualifiedName, values);
        }
        else if (value.IsScalar())
        {
            const auto raw = value.as<std::string>();
            if (raw.empty() || std::ranges::all_of(raw, ::isspace))
            {
                throw NES::InvalidConfigParameter("Value for: {} is empty.", qualifiedName);
            }
            values.emplace_back(NES::QualifiedIdentifier::parse(qualifiedName), NES::parseConfigLiteral(raw));
        }
        else
        {
            throw NES::InvalidConfigParameter("Unsupported YAML value for: {} (only scalars and nested maps are allowed)", qualifiedName);
        }
    }
}

}

namespace NES
{

ConfigLiteral parseConfigLiteral(const std::string& raw)
{
    const auto lowered = toLowerCase(raw);
    if (lowered == "true")
    {
        return true;
    }
    if (lowered == "false")
    {
        return false;
    }
    /// from_chars<int64_t> accepts partial matches ("1.5" would parse as 1), so take the integer
    /// path only when the input is entirely an integer; everything else falls through to the double parse.
    const auto trimmed = trimWhiteSpaces(raw);
    if (const auto digits = trimmed.starts_with('-') ? trimmed.substr(1) : trimmed;
        !digits.empty() && std::ranges::all_of(digits, [](const unsigned char character) { return std::isdigit(character) != 0; }))
    {
        if (const auto asSigned = from_chars<int64_t>(raw))
        {
            return *asSigned;
        }
    }
    if (const auto asDouble = from_chars<double>(raw))
    {
        return *asDouble;
    }
    return raw;
}

std::string formatConfigLiteral(const ConfigLiteral& literal)
{
    return std::visit(
        Overloaded{
            [](const std::monostate&) { return std::string{"null"}; },
            [](const Schema<UnqualifiedUnboundField, Ordered>& schema)
            {
                std::ostringstream oss;
                oss << schema;
                return std::move(oss).str();
            },
            [](const auto& value) { return fmt::format("{}", value); }},
        literal);
}

Schema<LiteralConfigValue, Ordered> flattenYAMLConfig(const YAML::Node& config)
{
    if (config.IsNull())
    {
        return {};
    }
    std::vector<LiteralConfigValue> values;
    flattenYAMLNode(config, "", values);
    return createConfigLiteralSchema(std::move(values));
}

Schema<LiteralConfigValue, Ordered> createConfigLiteralSchema(std::vector<LiteralConfigValue> values)
{
    std::unordered_set<QualifiedIdentifier> names;
    std::vector<std::string> duplicates;
    for (const auto& value : values)
    {
        if (!names.insert(value.getFullyQualifiedName()).second)
        {
            duplicates.push_back(toLowerCase(fmt::format("{}", value.getFullyQualifiedName())));
        }
    }
    if (!duplicates.empty())
    {
        throw InvalidConfigParameter(
            "Configuration options must not be set more than once, but these were: {}", fmt::join(duplicates, ", "));
    }
    return Schema<LiteralConfigValue, Ordered>{std::move(values)};
}

ConfigMergeResult mergeConfigLayers(const std::vector<ConfigLayer>& layersLowestPriorityFirst)
{
    struct Entry
    {
        LiteralConfigValue value;
        std::string_view layer;
    };

    /// Names must match exactly here — Schema's own name lookup is suffix-addressable, which would
    /// let a short name in a later layer replace a longer one it is a suffix of.
    /// TODO #1893: Add the ability to distinguish between absolute and relative paths to the Qua
    std::vector<Entry> merged;
    std::unordered_map<QualifiedIdentifier, size_t> indexByName;
    std::vector<ConfigOverwrite> overwrites;
    for (const auto& layer : layersLowestPriorityFirst)
    {
        for (const auto& value : layer.literals)
        {
            const auto [existing, inserted] = indexByName.try_emplace(value.getFullyQualifiedName(), merged.size());
            if (inserted)
            {
                merged.push_back(Entry{.value = value, .layer = layer.name});
                continue;
            }
            auto& entry = merged[existing->second];
            overwrites.push_back(ConfigOverwrite{
                .name = value.getFullyQualifiedName(),
                .overwrittenLayer = std::string{entry.layer},
                .overwrittenValue = entry.value.getValue(),
                .appliedLayer = layer.name,
                .appliedValue = value.getValue()});
            entry = Entry{.value = value, .layer = layer.name};
        }
    }
    return ConfigMergeResult{
        .literals = Schema<
            LiteralConfigValue,
            Ordered>{merged | std::views::transform([](Entry& entry) { return std::move(entry.value); }) | std::ranges::to<std::vector>()},
        .overwrites = std::move(overwrites)};
}

Schema<LiteralConfigValue, Ordered> parseCommandLineConfig(const std::vector<std::string>& arguments)
{
    std::vector<LiteralConfigValue> values;
    values.reserve(arguments.size());
    for (const auto& arg : arguments)
    {
        const auto separator = arg.find('=');
        if (separator == std::string::npos)
        {
            throw InvalidConfigParameter("Expected --key=value, but got: {}", arg);
        }
        auto key = arg.substr(0, separator);
        auto value = arg.substr(separator + 1);
        if (key.starts_with("--"))
        {
            key = key.substr(2);
        }
        auto name = QualifiedIdentifier::tryParse(key);
        if (!name.has_value())
        {
            throw InvalidConfigParameter("Invalid configuration key '{}': {}", key, name.error().what());
        }
        values.emplace_back(*std::move(name), parseConfigLiteral(value));
    }
    return createConfigLiteralSchema(std::move(values));
}

Schema<LiteralConfigValue, Ordered> flattenYAMLConfig(const std::filesystem::path& configFile)
{
    try
    {
        return flattenYAMLConfig(YAML::LoadFile(configFile.string()));
    }
    catch (const std::exception& ex)
    {
        throw CannotLoadConfig("Exception while loading configurations from: {}. Exception: {}", configFile.string(), ex.what());
    }
}

}
