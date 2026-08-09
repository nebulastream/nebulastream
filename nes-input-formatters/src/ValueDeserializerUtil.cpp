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
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <magic_enum/magic_enum.hpp>

#include <ErrorHandling.hpp>
#include <ValueDeserializer.hpp>
#include <ValueDeserializerRegistry.hpp>

namespace NES
{

void parseValueDeserializerOverrides(const std::string& overrides, std::unordered_map<DeserializerKey, std::string>& deserializersMap)
{
    size_t typeNameStart = 0;
    size_t typeNameEnd = overrides.find(':', typeNameStart);
    while (typeNameEnd != std::string::npos)
    {
        const std::string typeName = overrides.substr(typeNameStart, typeNameEnd - typeNameStart);
        const size_t deserializerTypeStart = typeNameEnd + 1;
        const size_t deserializerTypeEnd = std::min(overrides.size(), overrides.find(',', deserializerTypeStart));
        const std::string deserializerType = overrides.substr(deserializerTypeStart, deserializerTypeEnd - deserializerTypeStart);

        if (std::optional<DataType::Type> dataType = magic_enum::enum_cast<DataType::Type>(typeName))
        {
            deserializersMap[dataType.value()] = deserializerType;
        }
        else
        {
            /// typeName did not resolve as a DataType, we treat it as struct-name instead
            deserializersMap[typeName] = deserializerType;
        }
        typeNameStart = deserializerTypeEnd + 1;
        typeNameEnd = overrides.find(':', typeNameStart);
    }
}

std::unique_ptr<ValueDeserializer> provideValueDeserializer(const std::string& deserializerType, const ValueDeserializerConfig& config)
{
    /// Resolve the "Nullable" member
    const std::string completeDeserializerName = config.nullable ? "Nullable" + deserializerType : deserializerType;
    const ValueDeserializerRegistryArguments arguments{.quoted = config.quoted, .hasTrailingSpaces = config.hasTrailingSpaces};
    if (auto deserializer = ValueDeserializerRegistry::instance().create(completeDeserializerName, arguments))
    {
        return std::move(deserializer.value());
    }
    throw UnknownValueDeserializerType("Unknown Value Deserializer: {}", deserializerType);
}

std::unique_ptr<ValueDeserializer> provideDeserializerForType(
    const DataType& dataType,
    const ValueDeserializerConfig& config,
    const std::unordered_map<DeserializerKey, std::string>& deserializersMap)
{
    /// If the type is a struct, we try to either get the specifically configured deserializer for this struct-name, or the default, if it exists.
    /// Otherwise, we fall back to the standard struct deserializers.
    if (dataType.type == DataType::Type::STRUCT)
    {
        if (const auto it = deserializersMap.find(dataType.structName); it != deserializersMap.end())
        {
            return provideValueDeserializer(it->second, config);
        }
        /// Try the default deserializer for this type, if it exists
        const std::string defaultDeserializerName = "Default" + dataType.structName;
        try
        {
            return provideValueDeserializer(defaultDeserializerName, config);
        }
        catch (Exception& e)
        {
            /// Fall back to DataType::Type specific deserializers.
            if (const auto it = deserializersMap.find(dataType.type); it != deserializersMap.end())
            {
                return provideValueDeserializer(it->second, config);
            }
            throw UnknownValueDeserializerType("No deserializer configured for datatype {}", magic_enum::enum_name(dataType.type));
        }
    }
    if (const auto it = deserializersMap.find(dataType.type); it != deserializersMap.end())
    {
        return provideValueDeserializer(it->second, config);
    }
    throw UnknownValueDeserializerType("No deserializer configured for datatype {}", magic_enum::enum_name(dataType.type));
}
}
