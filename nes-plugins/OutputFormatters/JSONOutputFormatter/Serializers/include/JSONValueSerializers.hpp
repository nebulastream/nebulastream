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

#include <cstdint>
#include <string>
#include <string_view>

#include <simdjson.h>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <OutputFormatters/ValueSerializer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <CompilationContext.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES::JSONValueSerializer
{

/// Escapes a raw byte string into a quoted JSON string literal. simdjson's string builder performs
/// RFC 8259-conformant escaping (\b \f \n \r \t \" \\ short forms, \uXXXX for the remaining control
/// characters) with a SIMD fast path that scans for characters needing escaping.
/// The builder is held per thread because constructing one allocates its 1 KiB buffer, and this runs per field
/// per record. The returned view points into that buffer and is valid until this thread escapes its next value.
inline std::string_view escapeAsJsonString(const std::string_view input)
{
    thread_local simdjson::builder::string_builder builder;
    builder.clear();
    builder.escape_and_append_with_quotes(input);
    return builder.view().value();
}

/// A null is the bare literal the format prescribes, not a quoted string, so it bypasses escaping.
inline void appendJsonValue(std::string& out, const std::string_view rawValue, const bool isNull, const char* nullLiteral)
{
    if (isNull)
    {
        out.append(nullLiteral);
        return;
    }
    out.append(escapeAsJsonString(rawValue));
}

inline uint64_t serializeChar(
    const char value,
    const bool isNull,
    const char* prefix,
    const char* nullLiteral,
    const char* delimiter,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    std::string& serializedValue = serializationBuffer();
    serializedValue.append(prefix);
    appendJsonValue(serializedValue, std::string_view{&value, 1}, isNull, nullLiteral);
    serializedValue.append(delimiter);
    return writeValueToBuffer(
        serializedValue.data(), serializedValue.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

inline uint64_t serializeVarSized(
    int8_t* valueAddress,
    const uint64_t valueSize,
    const bool isNull,
    const char* prefix,
    const char* nullLiteral,
    const char* delimiter,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    std::string& serializedValue = serializationBuffer();
    serializedValue.append(prefix);
    appendJsonValue(serializedValue, std::string_view{reinterpret_cast<const char*>(valueAddress), valueSize}, isNull, nullLiteral);
    serializedValue.append(delimiter);
    return writeValueToBuffer(
        serializedValue.data(), serializedValue.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}
}

namespace NES
{

class JSONCHARValueSerializer final : public ValueSerializer
{
public:
    explicit JSONCHARValueSerializer() : ValueSerializer("JSONCHAR") { }

    void resolve(CompilationContext& compilationContext) override
    {
        serializeFunction.resolve(compilationContext, tracedFunctionName, &JSONValueSerializer::serializeChar);
    }

    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        CompilationContext& compilationContext,
        const VarVal& value,
        const nautilus::val<bool>& isNull,
        const nautilus::val<const char*>& prefix,
        const nautilus::val<const char*>& nullLiteral,
        const nautilus::val<const char*>& delimiter,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override
    {
        auto& serialize = serializeFunction.get(compilationContext, tracedFunctionName, &JSONValueSerializer::serializeChar);
        return serialize(
            value.getRawValueAs<nautilus::val<char>>(),
            isNull,
            prefix,
            nullLiteral,
            delimiter,
            startingAddress,
            remainingSize,
            recordBuffer.getReference(),
            bufferProvider);
    }

private:
    TracedInvokeMemo<decltype(&JSONValueSerializer::serializeChar)> serializeFunction;
};

/// The raw JSON text of a string is not its value, so a var-sized value has to be escaped on the way out.
class JSONVARSIZEDValueSerializer final : public ValueSerializer
{
public:
    explicit JSONVARSIZEDValueSerializer() : ValueSerializer("JSONVARSIZED") { }

    void resolve(CompilationContext& compilationContext) override
    {
        serializeFunction.resolve(compilationContext, tracedFunctionName, &JSONValueSerializer::serializeVarSized);
    }

    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        CompilationContext& compilationContext,
        const VarVal& value,
        const nautilus::val<bool>& isNull,
        const nautilus::val<const char*>& prefix,
        const nautilus::val<const char*>& nullLiteral,
        const nautilus::val<const char*>& delimiter,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override
    {
        auto& serialize = serializeFunction.get(compilationContext, tracedFunctionName, &JSONValueSerializer::serializeVarSized);
        const auto varSized = value.getRawValueAs<VariableSizedData>();
        return serialize(
            varSized.getContent(),
            varSized.getSize(),
            isNull,
            prefix,
            nullLiteral,
            delimiter,
            startingAddress,
            remainingSize,
            recordBuffer.getReference(),
            bufferProvider);
    }

private:
    TracedInvokeMemo<decltype(&JSONValueSerializer::serializeVarSized)> serializeFunction;
};
}
