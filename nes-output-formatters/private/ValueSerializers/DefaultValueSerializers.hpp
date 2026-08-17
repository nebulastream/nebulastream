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

#include <array>
#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <OutputFormatters/ValueSerializer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <CompilationContext.hpp>
#include <val_arith.hpp>
#include <val_base.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES::DefaultValueSerializer
{

/// Everything below is plain C++ behind a single nautilus call. Producing the null literal and the delimiter here
/// is what lets a field cost one call instead of a traced branch plus two buffer writes.

/// In the future, we could introduce customizable formatting functions for every data type via a registry.
template <typename T>
void appendValueAsString(std::string& out, const T value)
{
    using RemovedCVRefT = std::remove_cvref_t<T>;
    if constexpr (std::is_same_v<RemovedCVRefT, float> || std::is_same_v<RemovedCVRefT, double>)
    {
        out.append(formatFloat(value));
    }
    else if constexpr (std::is_same_v<RemovedCVRefT, bool>)
    {
        out.append(value ? "true" : "false");
    }
    else if constexpr (std::is_same_v<RemovedCVRefT, char>)
    {
        out.push_back(value);
    }
    else
    {
        /// to_chars writes into the stack buffer without allocating. The value is widened first because the
        /// narrow types here are character types, for which to_chars would not render a number.
        std::array<char, 32> digits{};
        char* const end = std::is_signed_v<RemovedCVRefT>
            ? std::to_chars(digits.data(), digits.data() + digits.size(), static_cast<int64_t>(value)).ptr
            : std::to_chars(digits.data(), digits.data() + digits.size(), static_cast<uint64_t>(value)).ptr;
        out.append(digits.data(), static_cast<size_t>(end - digits.data()));
    }
}

template <typename T>
uint64_t serializeFixedSized(
    const T value,
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
    if (isNull)
    {
        serializedValue.append(nullLiteral);
    }
    else
    {
        appendValueAsString(serializedValue, value);
    }
    serializedValue.append(delimiter);
    return writeValueToBuffer(
        serializedValue.data(), serializedValue.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// Quoting wraps the value in double quotes and doubles every embedded one, per RFC 4180.
template <bool Quoted>
uint64_t serializeVarSized(
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
    if (isNull)
    {
        serializedValue.append(nullLiteral);
    }
    else
    {
        const std::string_view rawValue{reinterpret_cast<const char*>(valueAddress), valueSize};
        if constexpr (Quoted)
        {
            /// Every embedded quote is doubled, so the worst case is twice the payload plus the enclosing pair.
            serializedValue.reserve(serializedValue.size() + (2 * rawValue.size()) + 2);
            serializedValue.push_back('"');
            for (const char character : rawValue)
            {
                serializedValue.push_back(character);
                if (character == '"')
                {
                    serializedValue.push_back('"');
                }
            }
            serializedValue.push_back('"');
        }
        else
        {
            serializedValue.append(rawValue);
        }
    }
    serializedValue.append(delimiter);
    return writeValueToBuffer(
        serializedValue.data(), serializedValue.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}
}

namespace NES
{

/// Covers every fixed-size default serializer. T is both the type extracted from the VarVal and the proxy's
/// parameter type, so nothing converts at the boundary.
template <typename T>
class DefaultFixedSizeValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultFixedSizeValueSerializer(const std::string_view typeKey) : ValueSerializer(typeKey) { }

    void resolve(CompilationContext& compilationContext) override
    {
        serializeFunction.resolve(compilationContext, tracedFunctionName, &DefaultValueSerializer::serializeFixedSized<T>);
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
        auto& serialize = serializeFunction.get(compilationContext, tracedFunctionName, &DefaultValueSerializer::serializeFixedSized<T>);
        return serialize(
            value.getRawValueAs<nautilus::val<T>>(),
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
    TracedInvokeMemo<decltype(&DefaultValueSerializer::serializeFixedSized<T>)> serializeFunction;
};

/// Var-sized values are written straight from the raw buffer, so they carry an address and a size. Quoting
/// selects the instantiation and therefore has to appear in the traced name.
class DefaultVarSizedValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultVarSizedValueSerializer(const bool quoted)
        : ValueSerializer(quoted ? "DefaultVARSIZED_Quoted" : "DefaultVARSIZED"), quoted(quoted)
    {
    }

    void resolve(CompilationContext& compilationContext) override
    {
        serializeFunction.resolve(compilationContext, tracedFunctionName, selectProxy());
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
        auto& serialize = serializeFunction.get(compilationContext, tracedFunctionName, selectProxy());
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
    using VarSizedProxy = decltype(&DefaultValueSerializer::serializeVarSized<true>);

    TracedInvokeMemo<VarSizedProxy> serializeFunction;

    /// One place decides which instantiation this serializer uses, so the traced name and the body cannot drift apart.
    [[nodiscard]] VarSizedProxy selectProxy() const
    {
        return quoted ? &DefaultValueSerializer::serializeVarSized<true> : &DefaultValueSerializer::serializeVarSized<false>;
    }

    bool quoted;
};
}
