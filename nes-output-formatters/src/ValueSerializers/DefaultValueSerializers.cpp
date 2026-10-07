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

#include <ValueSerializers/DefaultValueSerializers.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <rapidcheck/shrink/Shrink.h>
#include <ValueSerializerRegistry.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES::DefaultValueSerializer
{

template <typename T>
uint64_t serializeNumeric(
    const T value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string serializedValue = std::to_string(value);
    return writeValueToBuffer(
        serializedValue.data(), serializedValue.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

uint64_t serializeChar(
    const char value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string serializedValue{value};
    return writeValueToBuffer(
        serializedValue.data(), serializedValue.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

template <typename T>
uint64_t serializeFloat(
    const T value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string serializedValue = formatFloat(value);
    return writeValueToBuffer(
        serializedValue.data(), serializedValue.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

uint64_t serializeBool(
    const bool value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string serializedValue = value ? "true" : "false";
    return writeValueToBuffer(
        serializedValue.data(), serializedValue.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

template <bool Quoted>
uint64_t serializeVarsized(
    const int8_t* valueAddress,
    const uint64_t valueSize,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string_view stringFormattedValue{reinterpret_cast<const char*>(valueAddress), valueSize};
    if constexpr (Quoted)
    {
        /// Quote varsized and replace all " instances in the string with ""
        uint64_t writtenBytes = 0;
        writtenBytes += writeValueToBuffer(
            "\"", 1, remainingSpace - writtenBytes, tupleBuffer, bufferProvider, bufferStartingAddress + writtenBytes);

        size_t substringStart = 0;
        size_t quoteOffset = stringFormattedValue.find('\"');
        while (quoteOffset != std::string::npos)
        {
            /// Write substring until the quote and then an additional quote
            writtenBytes += writeValueToBuffer(
                stringFormattedValue.substr(substringStart, quoteOffset + 1).data(),
                stringFormattedValue.substr(substringStart, quoteOffset + 1).size(),
                remainingSpace - writtenBytes,
                tupleBuffer,
                bufferProvider,
                bufferStartingAddress + writtenBytes);
            writtenBytes += writeValueToBuffer(
                "\"", 1, remainingSpace - writtenBytes, tupleBuffer, bufferProvider, bufferStartingAddress + writtenBytes);
            substringStart += quoteOffset + 1;
            quoteOffset = stringFormattedValue.substr(substringStart).find('\"');
        }

        writtenBytes += writeValueToBuffer(
            stringFormattedValue.substr(substringStart).data(),
            stringFormattedValue.substr(substringStart).size(),
            remainingSpace - writtenBytes,
            tupleBuffer,
            bufferProvider,
            bufferStartingAddress + writtenBytes);
        writtenBytes += writeValueToBuffer(
            "\"", 1, remainingSpace - writtenBytes, tupleBuffer, bufferProvider, bufferStartingAddress + writtenBytes);
        return writtenBytes;
    }
    else
    {
        return writeValueToBuffer(
            stringFormattedValue.data(), stringFormattedValue.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
    }
}
}

namespace NES
{
nautilus::val<uint64_t> DefaultCHARValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<char>>();
    return nautilus::invoke(
        DefaultValueSerializer::serializeChar, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultF32ValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<float>>();
    return nautilus::invoke(
        DefaultValueSerializer::serializeFloat<float>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultF64ValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<double>>();
    return nautilus::invoke(
        DefaultValueSerializer::serializeFloat<double>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultINT8ValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return nautilus::invoke(
        DefaultValueSerializer::serializeNumeric<int8_t>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultINT16ValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return nautilus::invoke(
        DefaultValueSerializer::serializeNumeric<int16_t>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultINT32ValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return nautilus::invoke(
        DefaultValueSerializer::serializeNumeric<int32_t>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultINT64ValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int64_t>>();
    return nautilus::invoke(
        DefaultValueSerializer::serializeNumeric<int64_t>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultBOOLValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<bool>>();
    return nautilus::invoke(
        DefaultValueSerializer::serializeBool, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT8ValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint8_t>>();
    return nautilus::invoke(
        DefaultValueSerializer::serializeNumeric<uint8_t>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT16ValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint16_t>>();
    return nautilus::invoke(
        DefaultValueSerializer::serializeNumeric<uint16_t>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT32ValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint32_t>>();
    return nautilus::invoke(
        DefaultValueSerializer::serializeNumeric<uint32_t>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT64ValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint64_t>>();
    return nautilus::invoke(
        DefaultValueSerializer::serializeNumeric<uint64_t>,
        castedVal,
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> DefaultVARSIZEDValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<VariableSizedData>();
    if (quoted)
    {
        return nautilus::invoke(
            DefaultValueSerializer::serializeVarsized<true>,
            castedVal.getContent(),
            castedVal.getSize(),
            startingAddress,
            remainingSize,
            recordBuffer.getReference(),
            bufferProvider);
    }
    return nautilus::invoke(
        DefaultValueSerializer::serializeVarsized<false>,
        castedVal.getContent(),
        castedVal.getSize(),
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

std::unique_ptr<ValueSerializer> DefaultCHARValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultCHARValueSerializer>();
}

std::unique_ptr<ValueSerializer> DefaultF32ValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultF32ValueSerializer>();
}

std::unique_ptr<ValueSerializer> DefaultF64ValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultF64ValueSerializer>();
}

std::unique_ptr<ValueSerializer> DefaultINT8ValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultINT8ValueSerializer>();
}

std::unique_ptr<ValueSerializer> DefaultINT16ValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultINT16ValueSerializer>();
}

std::unique_ptr<ValueSerializer> DefaultINT32ValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultINT32ValueSerializer>();
}

std::unique_ptr<ValueSerializer> DefaultINT64ValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultINT64ValueSerializer>();
}

std::unique_ptr<ValueSerializer> DefaultBOOLValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultBOOLValueSerializer>();
}

std::unique_ptr<ValueSerializer> DefaultUINT8ValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultUINT8ValueSerializer>();
}

std::unique_ptr<ValueSerializer> DefaultUINT16ValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultUINT16ValueSerializer>();
}

std::unique_ptr<ValueSerializer> DefaultUINT32ValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultUINT32ValueSerializer>();
}

std::unique_ptr<ValueSerializer> DefaultUINT64ValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultUINT64ValueSerializer>();
}

std::unique_ptr<ValueSerializer> DefaultVARSIZEDValueSerializer::provideSerializer(ValueSerializerRegistryArguments args)
{
    return std::make_unique<DefaultVARSIZEDValueSerializer>(args.quoted);
}
}
