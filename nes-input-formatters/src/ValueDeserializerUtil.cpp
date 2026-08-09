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

#include <ValueDeserializerUtil.hpp>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <magic_enum/magic_enum.hpp>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/Record.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ErrorHandling.hpp>
#include <ValueDeserializer.hpp>
#include <ValueDeserializerRegistry.hpp>

namespace NES
{

std::unordered_map<Record::RecordFieldIdentifier, std::string>
parseValueDeserializerOverrides(const std::string& overrides, const std::vector<Record::RecordFieldIdentifier>& fieldNames)
{
    std::unordered_map<Record::RecordFieldIdentifier, std::string> deserializerTypes;
    /// Ignore the commas within quoted field names.
    for (const auto& entry : splitOnMultipleDelimiters(overrides, {','}, {'"'}))
    {
        const auto separator = entry.find(':');
        if (separator == std::string_view::npos)
        {
            throw InvalidConfigParameter(
                "VALUE_DESERIALIZERS entry '{}' is not of the form [FIELD-NAME]:[DESERIALIZER-KEY]", escapeSpecialCharacters(entry));
        }
        const auto configuredName = QualifiedIdentifier::tryParse(trimWhiteSpaces(entry.substr(0, separator)));
        if (not configuredName.has_value())
        {
            throw InvalidConfigParameter(
                "VALUE_DESERIALIZERS entry '{}' does not start with a valid field name: {}",
                escapeSpecialCharacters(entry),
                configuredName.error().what());
        }
        /// Ignoring an entry that names no field of the schema would hide a typo until someone wonders why the configured
        /// deserializer never ran.
        if (std::ranges::find(fieldNames, configuredName.value()) == fieldNames.end())
        {
            throw InvalidConfigParameter(
                "VALUE_DESERIALIZERS configures a deserializer for the field '{}', which is not part of the schema. Known fields: {}",
                configuredName.value(),
                fmt::join(fieldNames, ", "));
        }
        deserializerTypes[configuredName.value()] = std::string{trimWhiteSpaces(entry.substr(separator + 1))};
    }
    return deserializerTypes;
}

[[nodiscard]] std::optional<std::string> getPluginTypeDefaultDeserializer(const std::string& pluginName)
{
    const std::string defaultDeserializerName = "Default" + pluginName;
    if (const auto deserializerFactory = ValueDeserializerRegistry::instance().find(defaultDeserializerName))
    {
        return std::optional{defaultDeserializerName};
    }
    return std::nullopt;
}

[[nodiscard]] std::string
getDeserializerType(const DataType& dataType, const std::unordered_map<DataType::Type, std::string>& deserializerTypes)
{
    if (dataType.type == DataType::Type::STRUCT)
    {
        /// Use the default deserializer for this type, if it exists.
        if (auto pluginDefault = getPluginTypeDefaultDeserializer(dataType.structName))
        {
            return std::move(*pluginDefault);
        }
    }
    if (const auto it = deserializerTypes.find(dataType.type); it != deserializerTypes.end())
    {
        return it->second;
    }
    throw UnknownValueDeserializerType("No ValueDeserializer configured for DataType {}", magic_enum::enum_name(dataType.type));
}

std::unique_ptr<ValueDeserializer> provideValueDeserializer(const std::string& deserializerType, const ValueDeserializerConfig& config)
{
    /// Resolve the "Nullable" member
    const std::string completeDeserializerName = config.nullable ? "Nullable" + deserializerType : deserializerType;
    const ValueDeserializerRegistryArguments arguments{.quoted = config.quoted, .hasTrailingSpaces = config.hasTrailingSpaces};
    if (const auto deserializerFactory = ValueDeserializerRegistry::instance().find(completeDeserializerName))
    {
        return (*deserializerFactory)(arguments);
    }
    throw UnknownValueDeserializerType("Unknown Value Deserializer: {}", deserializerType);
}
}
