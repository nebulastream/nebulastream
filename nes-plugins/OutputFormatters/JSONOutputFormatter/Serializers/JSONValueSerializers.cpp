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

/// Write the delimiting bracket / comma + the field-name of a struct's field
uint64_t writeStructFieldPrefix(
    const bool isFirstField,
    const char* fieldIdentifier,
    const uint64_t remainingSpace,
    TupleBuffer* buffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferAddress)
{
    std::string out = isFirstField ? std::string("{\"") : std::string(",\"");
    out += fieldIdentifier;
    out += "\":";
    return writeValueToBuffer(out.data(), out.size(), remainingSpace, buffer, bufferProvider, bufferAddress);
}
}

namespace NES
{
nautilus::val<uint64_t> JSONCHARValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
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
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
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

nautilus::val<uint64_t> JSONSTRUCTValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>& serializerTypes,
    const DataType& valueType) const
{
    const auto castedVal = value.getRawValueAs<StructData>();
    nautilus::val<uint64_t> bytesWritten{0};

    for (nautilus::static_val<size_t> i = 0; i < castedVal.getNumFields(); ++i)
    {
        /// Get subfield name from valuetype, because the field name out of the structdata value does not survive the trace.
        const auto& [fieldName, fieldType] = valueType.fields.at(i);
        /// Try to create the default serializer for this field type
        const ValueSerializerConfig config{.quoted = true};
        const std::unique_ptr<ValueSerializer> subFieldSerializer
            = provideValueSerializer(getSerializerType(fieldType, serializerTypes), config);

        /// Write either the delimiting curly bracket or a comma + the subfield name
        bytesWritten += nautilus::invoke(
            JSONValueSerializer::writeStructFieldPrefix,
            nautilus::val<size_t>{i} == nautilus::val<size_t>{0},
            nautilus::val<const char*>{fieldName.c_str()},
            remainingSize - bytesWritten,
            recordBuffer.getReference(),
            bufferProvider,
            startingAddress + bytesWritten);

        /// Serializer the subfield value and write
        bytesWritten += subFieldSerializer->serializeAndWrite(
            castedVal.at(i),
            remainingSize - bytesWritten,
            recordBuffer,
            bufferProvider,
            startingAddress + bytesWritten,
            serializerTypes,
            fieldType);
    }
    /// Write closing bracket and return the number of bytes that were written into main buffer memory
    bytesWritten += nautilus::invoke(
        writeValueToBuffer,
        nautilus::val<const char*>("}"),
        nautilus::val<size_t>{1},
        remainingSize - bytesWritten,
        recordBuffer.getReference(),
        bufferProvider,
        startingAddress + bytesWritten);
    return bytesWritten;
}

nautilus::val<uint64_t> JSONFIXEDSIZEDValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>& serializerTypes,
    const DataType& valueType) const
{
    const auto castedVal = value.getRawValueAs<FixedSizedData>();

    /// Construct the serializer for the elements of the array
    const ValueSerializerConfig config{.quoted = true};
    const std::unique_ptr<ValueSerializer> elementSerializer
        = provideValueSerializer(getSerializerType(*valueType.elementType, serializerTypes), config);

    nautilus::val<uint64_t> bytesWritten{0};
    for (nautilus::static_val<size_t> i = 0; i < castedVal.getNumElements(); ++i)
    {
        /// Write either beginning bracket or the element-delimiting comma
        /// const nautilus::val<const char*> arrayPrefix
        const nautilus::val<const char*> elementPrefix = i == nautilus::static_val<size_t>{0} ? "[" : ",";
        bytesWritten += nautilus::invoke(
            writeValueToBuffer,
            elementPrefix,
            nautilus::val<size_t>{1},
            remainingSize - bytesWritten,
            recordBuffer.getReference(),
            bufferProvider,
            startingAddress + bytesWritten);

        /// Write the serialized element at i
        bytesWritten += elementSerializer->serializeAndWrite(
            castedVal.at(nautilus::val<uint64_t>{i}),
            remainingSize - bytesWritten,
            recordBuffer,
            bufferProvider,
            startingAddress + bytesWritten,
            serializerTypes,
            *valueType.elementType);
    }
    /// Write closing bracket
    bytesWritten += nautilus::invoke(
        writeValueToBuffer,
        nautilus::val<const char*>("]"),
        nautilus::val<size_t>{1},
        remainingSize - bytesWritten,
        recordBuffer.getReference(),
        bufferProvider,
        startingAddress + bytesWritten);
    return bytesWritten;
}

nautilus::val<uint64_t> JSONVECTORValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>& serializerTypes,
    const DataType& valueType) const
{
    /// Works basically identical to the fixedsized counterpart.
    /// The only difference is that the loop variable is a nautilus::val instead of nautilus::static_val, as the number of elements varies
    /// for each vector value of a field.
    const auto castedVal = value.getRawValueAs<VectorData>();

    /// Construct the serializer for the elements of the array
    const ValueSerializerConfig config{.quoted = true};
    const std::unique_ptr<ValueSerializer> elementSerializer
        = provideValueSerializer(getSerializerType(*valueType.elementType, serializerTypes), config);

    nautilus::val<uint64_t> bytesWritten{0};
    for (nautilus::val<size_t> i = 0; i < castedVal.getNumElements(); ++i)
    {
        /// Write either beginning bracket or the element-delimiting comma
        /// const nautilus::val<const char*> arrayPrefix
        const nautilus::val<const char*> elementPrefix = i == nautilus::val<size_t>{0} ? "[" : ",";
        bytesWritten += nautilus::invoke(
            writeValueToBuffer,
            elementPrefix,
            nautilus::val<size_t>{1},
            remainingSize - bytesWritten,
            recordBuffer.getReference(),
            bufferProvider,
            startingAddress + bytesWritten);

        /// Write the serialized element at i
        bytesWritten += elementSerializer->serializeAndWrite(
            castedVal.at(i),
            remainingSize - bytesWritten,
            recordBuffer,
            bufferProvider,
            startingAddress + bytesWritten,
            serializerTypes,
            *valueType.elementType);
    }
    /// Write closing bracket
    bytesWritten += nautilus::invoke(
        writeValueToBuffer,
        nautilus::val<const char*>("]"),
        nautilus::val<size_t>{1},
        remainingSize - bytesWritten,
        recordBuffer.getReference(),
        bufferProvider,
        startingAddress + bytesWritten);
    return bytesWritten;
}

ValueSerializerRegistryReturnType JSONCHARValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONCHARValueSerializer>();
}

ValueSerializerRegistryReturnType JSONVARSIZEDValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONVARSIZEDValueSerializer>();
}

ValueSerializerRegistryReturnType JSONSTRUCTValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONSTRUCTValueSerializer>();
}

ValueSerializerRegistryReturnType JSONFIXEDSIZEDValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONFIXEDSIZEDValueSerializer>();
}

std::unique_ptr<ValueSerializer> JSONVECTORValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONVECTORValueSerializer>();
}
}
