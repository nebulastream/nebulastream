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

#include <string>
#include <unordered_map>

#include <Identifiers/Identifier.hpp>
#include <Util/Reflection.hpp>
#include <rfl/Generic.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>

/// How the options of a source or sink are written as JSON for the catalog.
/// The planner writes them when a source or sink is created, and the catalog bridge reads them back.
namespace NES::CatalogConfig
{

/// The options of a source, stored as one JSON object with the canonical option names as keys:
/// `{"<option>": "<value>"}`.
struct Options
{
    std::unordered_map<Identifier, std::string> values;
};

/// The options of a sink and of its output formatter.
/// The catalog stores both in one JSON object, with the formatter options nested under `OUTPUT_FORMATTER`:
/// `{"<option>": "<value>", "OUTPUT_FORMATTER": {"<option>": "<value>"}}`.
/// The SQL statement nests them the same way.
struct SinkOptions
{
    std::unordered_map<Identifier, std::string> sinkConfig;
    std::unordered_map<Identifier, std::string> formatConfig;
};

/// Writes any reflectable value as JSON.
template <typename T>
std::string toJson(const T& value)
{
    return rfl::json::write(*ReflectionContext{}.reflect(value));
}

/// Reads any reflectable value from JSON.
template <typename T>
T fromJson(const std::string& json)
{
    auto parsed = rfl::json::read<rfl::Generic>(json);
    if (!parsed)
    {
        throw CannotDeserialize("Failed to parse JSON from the catalog: {}", parsed.error().what());
    }
    return ReflectionContext{}.unreflect<T>(Reflected{*parsed});
}

}

namespace NES
{

template <>
struct Reflector<CatalogConfig::Options>
{
    Reflected operator()(const CatalogConfig::Options& options, const ReflectionContext& context) const;
};

template <>
struct Unreflector<CatalogConfig::Options>
{
    CatalogConfig::Options operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<CatalogConfig::SinkOptions>
{
    Reflected operator()(const CatalogConfig::SinkOptions& options, const ReflectionContext& context) const;
};

template <>
struct Unreflector<CatalogConfig::SinkOptions>
{
    CatalogConfig::SinkOptions operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

}
