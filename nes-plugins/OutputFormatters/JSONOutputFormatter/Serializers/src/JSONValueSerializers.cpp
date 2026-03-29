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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <simdjson.h>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <OutputFormatters/ValueSerializer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ValueSerializerRegistry.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/StructData.hpp>
#include <ErrorHandling.hpp>
#include <select.hpp>

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
    nautilus::val<uint64_t> bytesWritten{0};

    /// Create the serializer for the elements of the array
    if (const auto it = serializerTypes.find(castedVal.getElementType().type); it != serializerTypes.end())
    {
        const ValueSerializerConfig config{.quoted = true};
        const std::unique_ptr<ValueSerializer> elementSerializer = provideValueSerializer(it->second, config);
        for (nautilus::static_val<size_t> i = 0; i < castedVal.getNumElements(); ++i)
        {
            /// Write either opening bracket or comma
            const nautilus::val<const char*> arrayPrefix
                = nautilus::select(i == nautilus::val<size_t>{0}, nautilus::val<const char*>{"["}, nautilus::val<const char*>{","});
            bytesWritten += nautilus::invoke(
                writeValueToBuffer,
                arrayPrefix,
                nautilus::val<size_t>{1},
                remainingSize - bytesWritten,
                recordBuffer.getReference(),
                bufferProvider,
                startingAddress + bytesWritten);

            /// Serialize and write value at position i
            bytesWritten += elementSerializer->serializeAndWrite(
                castedVal.at(nautilus::val<uint64_t>{i}),
                remainingSize - bytesWritten,
                recordBuffer,
                bufferProvider,
                startingAddress + bytesWritten,
                serializerTypes,
                valueType.elementType[0]);
        }
        /// Write closing bracket and return number of bytes written to main memory
        bytesWritten += nautilus::invoke(
            writeValueToBuffer,
            nautilus::val<const char*>{"]"},
            nautilus::val<size_t>{1},
            remainingSize - bytesWritten,
            recordBuffer.getReference(),
            bufferProvider,
            startingAddress + bytesWritten);
        return bytesWritten;
    }
    throw UnknownValueSerializerType(
        "No serializer configured for FIXEDSIZED element-type {}.", magic_enum::enum_name(castedVal.getElementType().type));
}

/// Basically identical to the JSONFIXEDSIZED serializer. Only difference is that the loop variable is a nautilus::val instead of nautilus::static_val.
nautilus::val<uint64_t> JSONVARARRAYValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>& serializerTypes,
    const DataType& valueType) const
{
    const auto castedVal = value.getRawValueAs<VarArrayData>();
    nautilus::val<uint64_t> bytesWritten{0};

    /// Create the serializer for the elements of the vector
    if (const auto it = serializerTypes.find(castedVal.getElementType().type); it != serializerTypes.end())
    {
        const ValueSerializerConfig config{.quoted = true};
        const std::unique_ptr<ValueSerializer> elementSerializer = provideValueSerializer(it->second, config);
        for (nautilus::val<size_t> i = 0; i < castedVal.getNumElements(); ++i)
        {
            /// Write either opening bracket or comma
            const nautilus::val<const char*> arrayPrefix
                = nautilus::select(i == nautilus::val<size_t>{0}, nautilus::val<const char*>{"["}, nautilus::val<const char*>{","});
            bytesWritten += nautilus::invoke(
                writeValueToBuffer,
                arrayPrefix,
                nautilus::val<size_t>{1},
                remainingSize - bytesWritten,
                recordBuffer.getReference(),
                bufferProvider,
                startingAddress + bytesWritten);

            /// Serialize and write value at position i
            bytesWritten += elementSerializer->serializeAndWrite(
                castedVal.at(i),
                remainingSize - bytesWritten,
                recordBuffer,
                bufferProvider,
                startingAddress + bytesWritten,
                serializerTypes,
                valueType.elementType[0]);
        }
        /// Write closing bracket and return number of bytes written to main memory
        bytesWritten += nautilus::invoke(
            writeValueToBuffer,
            nautilus::val<const char*>{"]"},
            nautilus::val<size_t>{1},
            remainingSize - bytesWritten,
            recordBuffer.getReference(),
            bufferProvider,
            startingAddress + bytesWritten);
        return bytesWritten;
    }
    throw UnknownValueSerializerType(
        "No serializer configured for VARARRAY element-type {}.", magic_enum::enum_name(castedVal.getElementType().type));
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
        /// We need to obtain the subFieldName from the passed datatype because the field name from the StructData object does not survive the trace.
        const auto& [subFieldName, subFieldType] = valueType.fields.at(i);
        /// Try to create the serializer for the type of this field and serialize the value of this field
        if (const auto it = serializerTypes.find(subFieldType.type); it != serializerTypes.end())
        {
            const ValueSerializerConfig config{.quoted = true};
            const std::unique_ptr<ValueSerializer> subFieldSerializer = provideValueSerializer(it->second, config);

            /// Write either delimiting curly bracket or comma + the subFieldName
            bytesWritten += nautilus::invoke(
                JSONValueSerializer::writeStructFieldPrefix,
                nautilus::val<uint64_t>{i} == nautilus::val<uint64_t>{0},
                nautilus::val<const char*>{subFieldName.c_str()},
                remainingSize - bytesWritten,
                recordBuffer.getReference(),
                bufferProvider,
                startingAddress + bytesWritten);

            bytesWritten += subFieldSerializer->serializeAndWrite(
                castedVal.at(i),
                remainingSize - bytesWritten,
                recordBuffer,
                bufferProvider,
                startingAddress + bytesWritten,
                serializerTypes,
                subFieldType);
        }
        else
        {
            throw UnknownValueSerializerType(
                "No serializer configured for STRUCT element-type {}.", magic_enum::enum_name(subFieldType.type));
        }
    }
    /// Write closing bracket and return number of bytes written to main memory
    bytesWritten += nautilus::invoke(
        writeValueToBuffer,
        nautilus::val<const char*>{"}"},
        nautilus::val<size_t>{1},
        remainingSize - bytesWritten,
        recordBuffer.getReference(),
        bufferProvider,
        startingAddress + bytesWritten);
    return bytesWritten;
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterJSONCHARValueSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONCHARValueSerializer>();
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterJSONVARSIZEDValueSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONVARSIZEDValueSerializer>();
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterJSONFIXEDSIZEDValueSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONFIXEDSIZEDValueSerializer>();
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterJSONVARARRAYValueSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONVARARRAYValueSerializer>();
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterJSONSTRUCTValueSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<JSONSTRUCTValueSerializer>();
}
}
