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
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/Record.hpp>
#include <OutputFormatters/ValueSerializer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/ranges.h>

namespace NES
{
/// Write the serialized value represented by valuePtr and valueSize completely into the buffer.
/// Child buffers may be allocated if it does not fit completely into the main memory of the tuple buffer.
/// String may span between children or between the main buffer and the first child.
/// RemainingSpace tells the function the amount of space that is left in the main buffer.
/// Will return the amount of bytes written in the main memory of the buffer
uint64_t writeValueToBuffer(
    const char* valuePtr,
    size_t valueSize,
    uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress);

/// Config parameters for value serializers
struct ValueSerializerConfig
{
    bool quoted;
};

/// Check if the datatype plugin of the name pluginName has registered a default serializer.
/// Returns its name if it did.
/// Otherwise, return nullopt.
[[nodiscard]] std::optional<std::string> getPluginTypeDefaultSerializer(const std::string& pluginName);

/// Get the serializer type for a datatype.
/// Before the format-specific STRUCT default is used for datatype plugins, we check if the plugin has registered a default serializer under Default<DataType Key>.
[[nodiscard]] std::string
getSerializerType(const DataType& dataType, const std::unordered_map<DataType::Type, std::string>& serializerTypes);

/// Resolves the serializer that the user configured for individual fields against the fields of the output schema.
/// The function expects the overrides string to be formatted like this: [FIELD-NAME]:[SERIALIZER-KEY],...
/// Throws an InvalidConfigParameter for a malformed entry or an entry that names no field of the output schema.
[[nodiscard]] std::unordered_map<Record::RecordFieldIdentifier, std::string>
parseValueSerializerOverrides(const std::string& overrides, const std::vector<Record::RecordFieldIdentifier>& fieldNames);

/// Fetches ValueSerializer from Registry
std::unique_ptr<ValueSerializer> provideValueSerializer(const std::string& serializerType, const ValueSerializerConfig& config);
}
