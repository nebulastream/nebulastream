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

#pragma once

#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <Identifiers/Identifier.hpp>
#include <rfl/Generic.hpp>

/// The wire format of the catalog's source and sink config maps.
/// The write path (statements that create a source or sink) and the read path (lookups that rebuild descriptors)
/// must agree on how keys are spelled and where a sink's formatter options are nested, so both are defined here once.
namespace NES::CatalogConfig
{

/// A sink's output-formatter options are stored nested under this key inside the sink config.
inline constexpr std::string_view OUTPUT_FORMATTER_KEY{"OUTPUT_FORMATTER"};

/// JSON has only string keys, so identifier keys are serialized in their canonical (case-folded) spelling.
/// The two conversions below are inverses.
inline std::unordered_map<std::string, std::string> toStringKeys(const std::unordered_map<Identifier, std::string>& config)
{
    return config | std::views::transform([](const auto& kv) { return std::pair{kv.first.asCanonicalString(), kv.second}; })
        | std::ranges::to<std::unordered_map<std::string, std::string>>();
}

inline std::unordered_map<Identifier, std::string> toIdentifierKeys(const std::unordered_map<std::string, std::string>& config)
{
    return config | std::views::transform([](const auto& kv) { return std::pair{Identifier::fromCanonical(kv.first), kv.second}; })
        | std::ranges::to<std::unordered_map<Identifier, std::string>>();
}

inline rfl::Generic toGeneric(const std::unordered_map<std::string, std::string>& config)
{
    rfl::Generic::Object object;
    for (const auto& [key, value] : config)
    {
        object.insert(key, rfl::Generic(value));
    }
    return rfl::Generic(object);
}

/// SQL nests the output-formatter options inside the sink's config options, and the catalog stores that same shape.
inline rfl::Generic mergeFormatConfig(
    const std::unordered_map<std::string, std::string>& sinkConfig, const std::unordered_map<std::string, std::string>& formatConfig)
{
    rfl::Generic::Object object;
    for (const auto& [key, value] : sinkConfig)
    {
        object.insert(key, rfl::Generic(value));
    }
    if (!formatConfig.empty())
    {
        object.insert(std::string{OUTPUT_FORMATTER_KEY}, toGeneric(formatConfig));
    }
    return rfl::Generic(object);
}

}
