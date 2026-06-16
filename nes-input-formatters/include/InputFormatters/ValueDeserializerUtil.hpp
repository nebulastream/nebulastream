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

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>

#include <DataTypes/DataType.hpp>
#include <ValueDeserializer.hpp>

namespace NES
{

/// Current placeholder for a fully functional ValueDeserializerDescriptor.
/// Attributes are either passed into the deserializer implementation or used in the ValueDeserializerProvider to determine the deserializer type.
struct ValueDeserializerConfig
{
    bool nullable;
    bool quoted;
    bool hasTrailingSpaces;
};

/// Overrides the deserializer types for the datatypes based on the overrides string. The function expects the string to be formatted like this:
/// [TYPENAME]:[DESERIALIZER-KEY],...
void parseValueDeserializerOverrides(const std::string& overrides, std::unordered_map<DataType::Type, std::string>& deserializersMap);

/// Fetches ValueDeserializer from Registry
/// The concrete type of deserializer is [Nullable]<deserializerType>ValueDeserializer
std::unique_ptr<ValueDeserializer> provideValueDeserializer(const std::string& deserializerType, const ValueDeserializerConfig& config);
}
