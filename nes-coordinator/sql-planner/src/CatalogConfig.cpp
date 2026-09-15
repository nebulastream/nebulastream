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

#include <CatalogConfig.hpp>

#include <string>
#include <unordered_map>
#include <utility>

#include <Identifiers/Identifier.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Util/Reflection.hpp>
#include <rfl/Generic.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

rfl::Generic::Object toObject(const std::unordered_map<Identifier, std::string>& config)
{
    rfl::Generic::Object object;
    for (const auto& [name, value] : config)
    {
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) inserts the key
        object[name.asCanonicalString()] = rfl::Generic{value};
    }
    return object;
}

std::unordered_map<Identifier, std::string> fromObject(const rfl::Generic::Object& object)
{
    std::unordered_map<Identifier, std::string> config;
    for (const auto& [name, value] : object)
    {
        auto text = value.to_string();
        if (!text)
        {
            throw CannotDeserialize("config option '{}' is not a string", name);
        }
        config.emplace(Identifier::fromCanonical(name), *text);
    }
    return config;
}

rfl::Generic::Object asObject(const Reflected& reflected)
{
    auto object = reflected->to_object();
    if (!object)
    {
        throw CannotDeserialize("config is not a JSON object");
    }
    return *object;
}

}

Reflected Reflector<CatalogConfig::Options>::operator()(const CatalogConfig::Options& options, const ReflectionContext&) const
{
    return Reflected{toObject(options.values)};
}

CatalogConfig::Options Unreflector<CatalogConfig::Options>::operator()(const Reflected& reflected, const ReflectionContext&) const
{
    return CatalogConfig::Options{.values = fromObject(asObject(reflected))};
}

Reflected Reflector<CatalogConfig::SinkOptions>::operator()(const CatalogConfig::SinkOptions& options, const ReflectionContext&) const
{
    auto object = toObject(options.sinkConfig);
    if (!options.formatConfig.empty())
    {
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) inserts the key
        object[std::string{OUTPUT_FORMATTER_OPTION_GROUP}] = rfl::Generic{toObject(options.formatConfig)};
    }
    return Reflected{std::move(object)};
}

CatalogConfig::SinkOptions Unreflector<CatalogConfig::SinkOptions>::operator()(const Reflected& reflected, const ReflectionContext&) const
{
    CatalogConfig::SinkOptions options;
    rfl::Generic::Object sinkObject;
    for (const auto& [name, value] : asObject(reflected))
    {
        if (name != OUTPUT_FORMATTER_OPTION_GROUP)
        {
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) inserts the key
            sinkObject[name] = value;
            continue;
        }
        auto formatObject = value.to_object();
        if (!formatObject)
        {
            throw CannotDeserialize("the formatter options of a sink are not a JSON object");
        }
        options.formatConfig = fromObject(*formatObject);
    }
    options.sinkConfig = fromObject(sinkObject);
    return options;
}

}
