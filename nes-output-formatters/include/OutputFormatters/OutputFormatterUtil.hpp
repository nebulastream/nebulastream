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
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>

#include <DataTypes/DataType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{
class ValueSerializer;

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

using SerializerKey = std::variant<DataType::Type, std::string>;

/// Overrides the serializer types for the datatypes based on the overrides string. The function expects the string to be formatted like this:
/// [TYPENAME]:[SERIALIZER-KEY],...
void parseValueSerializerOverrides(const std::string& overrides, std::unordered_map<SerializerKey, std::string>& serializersMap);

/// Fetches ValueSerializer from Registry
std::unique_ptr<ValueSerializer> provideValueSerializer(const std::string& serializerType, const ValueSerializerConfig& config);

std::unique_ptr<ValueSerializer> provideSerializerForType(
    const DataType& dataType, const ValueSerializerConfig& config, const std::unordered_map<SerializerKey, std::string>& serializersMap);

}
