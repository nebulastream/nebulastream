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

#include <JSONValueSerializers.hpp>

#include <cmath>
#include <concepts>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <simdjson.h>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Strings.hpp>
#include <ValueSerializerRegistry.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES::JSONValueSerializer
{

/// Escapes a raw byte string into a quoted JSON string literal. simdjson's string builder performs
/// RFC 8259-conformant escaping (\b \f \n \r \t \" \\ short forms, \uXXXX for the remaining control
/// characters) with a SIMD fast path that scans for characters needing escaping.
std::string escapeAsJsonString(const std::string_view input)
{
    simdjson::builder::string_builder builder;
    builder.escape_and_append_with_quotes(input);
    return std::string{builder.view().value()};
}

uint64_t serializeChar(
    const char value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string serializedValue = escapeAsJsonString(std::string(1, value));
    return writeValueToBuffer(
        serializedValue.data(), serializedValue.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

uint64_t serializeVarsized(
    const int8_t* valueAddress,
    const uint64_t valueSize,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string serializedValue = escapeAsJsonString(std::string(reinterpret_cast<const char*>(valueAddress), valueSize));
    return writeValueToBuffer(
        serializedValue.data(), serializedValue.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// JSON has no literals for NaN or infinity, so non-finite values are written as null.
template <std::floating_point T>
uint64_t serializeFloat(
    const T value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string serializedValue = std::isfinite(value) ? formatFloat(value) : "null";
    return writeValueToBuffer(
        serializedValue.data(), serializedValue.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}
}

namespace NES
{
nautilus::val<uint64_t> JSONCHARValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<char>>();
    return nautilus::invoke(
        JSONValueSerializer::serializeChar, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> JSONVARSIZEDValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<VariableSizedData>();
    return nautilus::invoke(
        JSONValueSerializer::serializeVarsized,
        castedVal.getContent(),
        castedVal.getSize(),
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> JSONF32ValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<float>>();
    return nautilus::invoke(
        JSONValueSerializer::serializeFloat<float>, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> JSONF64ValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<double>>();
    return nautilus::invoke(
        JSONValueSerializer::serializeFloat<double>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

ValueSerializerRegistryReturnType JSONCHARValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONCHARValueSerializer>();
}

ValueSerializerRegistryReturnType JSONVARSIZEDValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONVARSIZEDValueSerializer>();
}

ValueSerializerRegistryReturnType JSONF32ValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONF32ValueSerializer>();
}

ValueSerializerRegistryReturnType JSONF64ValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONF64ValueSerializer>();
}
}
