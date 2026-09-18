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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <Interface/Record.hpp>
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

/// Resolves the deserializer that the user configured for individual fields against the fields of the schema.
/// The function expects the overrides string to be formatted like this: [FIELD-NAME]:[DESERIALIZER-KEY],...
/// Throws an InvalidConfigParameter for a malformed entry or an entry that names no field of the schema.
[[nodiscard]] std::unordered_map<Record::RecordFieldIdentifier, std::string>
parseValueDeserializerOverrides(const std::string& overrides, const std::vector<Record::RecordFieldIdentifier>& fieldNames);

/// Fetches ValueDeserializer from Registry
/// The concrete type of deserializer is [Nullable]<deserializerType>ValueDeserializer
std::unique_ptr<ValueDeserializer> provideValueDeserializer(const std::string& deserializerType, const ValueDeserializerConfig& config);
}
