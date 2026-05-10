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

#include <JSONOutputFormatter.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Util/Strings.hpp>
#include <magic_enum/magic_enum.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <fmt/format.h>
#include <std/cstring.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nlohmann/json.hpp>
#include <OutputFormatterRegistry.hpp>
#include <OutputFormatterValidationRegistry.hpp>
#include <function.hpp>
#include <select.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
uint64_t writePreValueContents(
    const bool isFirstField,
    const char* fieldIdentifier,
    const uint64_t remainingSpace,
    TupleBuffer* buffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferAddress)
{
    std::string preValueContentString = "\"" + std::string(fieldIdentifier) + "\":";
    if (isFirstField)
    {
        preValueContentString = "{" + preValueContentString;
    }
    return writeValueToBuffer(preValueContentString.c_str(), remainingSpace, buffer, bufferProvider, bufferAddress);
}

uint64_t writeBool(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const bool value,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    /// JSON booleans need to be represented by true/false instead of 1/0
    const std::string valueAsString = value ? "true" : "false";
    return writeValueToBuffer(valueAsString.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

uint64_t writeChar(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const char content,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    /// Chars are treated as strings in JSON
    const std::string charAsJsonString = nlohmann::json(std::string(1, content)).dump();
    return writeValueToBuffer(charAsJsonString.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

uint64_t writeVarsized(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const int8_t* varSizedContent,
    const uint64_t contentSize,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    /// Use nlohmann json library to delimit all special characters in the string
    const std::string jsonFormattedString = nlohmann::json(std::string(reinterpret_cast<const char*>(varSizedContent), contentSize)).dump();
    return writeValueToBuffer(jsonFormattedString.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// Renders a FIXEDSIZED array as a JSON array literal `[v0,v1,...]`. One specialization
/// per primitive element type; the dispatch from the traced `writeValue` keys on
/// `dataType.elementType` (host-side) to pick the right instantiation.
template <typename ElementT>
uint64_t writeFixedSizedAsJsonArray(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const int8_t* arrayBytes,
    const uint64_t numElements,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    std::string out = "[";
    const auto* elements = reinterpret_cast<const ElementT*>(arrayBytes);
    for (uint64_t i = 0; i < numElements; ++i)
    {
        if (i > 0)
        {
            out += ",";
        }
        if constexpr (std::is_same_v<ElementT, bool>)
        {
            out += elements[i] ? "true" : "false";
        }
        else if constexpr (std::is_same_v<ElementT, char>)
        {
            out += nlohmann::json(std::string(1, elements[i])).dump();
        }
        else if constexpr (std::is_floating_point_v<ElementT>)
        {
            out += formatFloat(elements[i]);
        }
        else
        {
            /// uint8/int8 promoted to int so std::to_string doesn't render them as chars.
            if constexpr (sizeof(ElementT) == 1)
            {
                out += std::to_string(static_cast<int32_t>(elements[i]));
            }
            else
            {
                out += std::to_string(elements[i]);
            }
        }
    }
    out += "]";
    return writeValueToBuffer(out.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// Writes either `{"fieldName":` (first sub-field) or `,"fieldName":` (subsequent
/// sub-fields) into the buffer; used by the STRUCT case of writeValue to glue
/// together the recursive child renderings.
uint64_t writeStructFieldNamePrefix(
    const bool isFirstField,
    const char* fieldName,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferAddress)
{
    std::string out = isFirstField ? std::string("{\"") : std::string(",\"");
    out += fieldName;
    out += "\":";
    return writeValueToBuffer(out.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferAddress);
}

uint64_t writeStructEnd(
    int8_t* bufferAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeValueToBuffer("}", remainingSpace, tupleBuffer, bufferProvider, bufferAddress);
}

void writeValue(
    const DataType& fieldType,
    const VarVal& value,
    const nautilus::val<int8_t*>& fieldPointer,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    nautilus::val<uint64_t>& written,
    nautilus::val<uint64_t>& currentRemainingSize)
{
    switch (fieldType.type)
    {
        case DataType::Type::BOOLEAN: {
            const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                writeBool,
                fieldPointer + written,
                currentRemainingSize,
                value.getRawValueAs<nautilus::val<bool>>(),
                recordBuffer.getReference(),
                bufferProvider);
            written += amountWritten;
            currentRemainingSize -= amountWritten;
            break;
        }
        case DataType::Type::CHAR: {
            const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                writeChar,
                fieldPointer + written,
                currentRemainingSize,
                value.getRawValueAs<nautilus::val<char>>(),
                recordBuffer.getReference(),
                bufferProvider);
            written += amountWritten;
            currentRemainingSize -= amountWritten;
            break;
        }
        case DataType::Type::VARSIZED: {
            const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
            const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                writeVarsized,
                fieldPointer + written,
                currentRemainingSize,
                varSizedValue.getContent(),
                varSizedValue.getSize(),
                recordBuffer.getReference(),
                bufferProvider);

            written += amountWritten;
            currentRemainingSize -= amountWritten;
            break;
        }
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64: {
            const nautilus::val<uint64_t> amountWritten
                = formatAndWriteVal(value, fieldType, fieldPointer + written, currentRemainingSize, recordBuffer, bufferProvider);
            written += amountWritten;
            currentRemainingSize -= amountWritten;
            break;
        }
        case DataType::Type::FIXEDSIZED: {
            const auto fixedValue = value.getRawValueAs<FixedSizedData>();
            const auto numElements = nautilus::val<uint64_t>(static_cast<uint64_t>(fixedValue.getNumElements()));
            nautilus::val<uint64_t> amountWritten{0};
            switch (fieldType.elementType)
            {
                case DataType::Type::INT8:
                    amountWritten = nautilus::invoke(
                        writeFixedSizedAsJsonArray<int8_t>,
                        fieldPointer + written,
                        currentRemainingSize,
                        fixedValue.getRawPtr(),
                        numElements,
                        recordBuffer.getReference(),
                        bufferProvider);
                    break;
                case DataType::Type::INT16:
                    amountWritten = nautilus::invoke(
                        writeFixedSizedAsJsonArray<int16_t>,
                        fieldPointer + written,
                        currentRemainingSize,
                        fixedValue.getRawPtr(),
                        numElements,
                        recordBuffer.getReference(),
                        bufferProvider);
                    break;
                case DataType::Type::INT32:
                    amountWritten = nautilus::invoke(
                        writeFixedSizedAsJsonArray<int32_t>,
                        fieldPointer + written,
                        currentRemainingSize,
                        fixedValue.getRawPtr(),
                        numElements,
                        recordBuffer.getReference(),
                        bufferProvider);
                    break;
                case DataType::Type::INT64:
                    amountWritten = nautilus::invoke(
                        writeFixedSizedAsJsonArray<int64_t>,
                        fieldPointer + written,
                        currentRemainingSize,
                        fixedValue.getRawPtr(),
                        numElements,
                        recordBuffer.getReference(),
                        bufferProvider);
                    break;
                case DataType::Type::UINT8:
                    amountWritten = nautilus::invoke(
                        writeFixedSizedAsJsonArray<uint8_t>,
                        fieldPointer + written,
                        currentRemainingSize,
                        fixedValue.getRawPtr(),
                        numElements,
                        recordBuffer.getReference(),
                        bufferProvider);
                    break;
                case DataType::Type::UINT16:
                    amountWritten = nautilus::invoke(
                        writeFixedSizedAsJsonArray<uint16_t>,
                        fieldPointer + written,
                        currentRemainingSize,
                        fixedValue.getRawPtr(),
                        numElements,
                        recordBuffer.getReference(),
                        bufferProvider);
                    break;
                case DataType::Type::UINT32:
                    amountWritten = nautilus::invoke(
                        writeFixedSizedAsJsonArray<uint32_t>,
                        fieldPointer + written,
                        currentRemainingSize,
                        fixedValue.getRawPtr(),
                        numElements,
                        recordBuffer.getReference(),
                        bufferProvider);
                    break;
                case DataType::Type::UINT64:
                    amountWritten = nautilus::invoke(
                        writeFixedSizedAsJsonArray<uint64_t>,
                        fieldPointer + written,
                        currentRemainingSize,
                        fixedValue.getRawPtr(),
                        numElements,
                        recordBuffer.getReference(),
                        bufferProvider);
                    break;
                case DataType::Type::FLOAT32:
                    amountWritten = nautilus::invoke(
                        writeFixedSizedAsJsonArray<float>,
                        fieldPointer + written,
                        currentRemainingSize,
                        fixedValue.getRawPtr(),
                        numElements,
                        recordBuffer.getReference(),
                        bufferProvider);
                    break;
                case DataType::Type::FLOAT64:
                    amountWritten = nautilus::invoke(
                        writeFixedSizedAsJsonArray<double>,
                        fieldPointer + written,
                        currentRemainingSize,
                        fixedValue.getRawPtr(),
                        numElements,
                        recordBuffer.getReference(),
                        bufferProvider);
                    break;
                case DataType::Type::BOOLEAN:
                    amountWritten = nautilus::invoke(
                        writeFixedSizedAsJsonArray<bool>,
                        fieldPointer + written,
                        currentRemainingSize,
                        fixedValue.getRawPtr(),
                        numElements,
                        recordBuffer.getReference(),
                        bufferProvider);
                    break;
                case DataType::Type::CHAR:
                    amountWritten = nautilus::invoke(
                        writeFixedSizedAsJsonArray<char>,
                        fieldPointer + written,
                        currentRemainingSize,
                        fixedValue.getRawPtr(),
                        numElements,
                        recordBuffer.getReference(),
                        bufferProvider);
                    break;
                case DataType::Type::VARSIZED:
                case DataType::Type::FIXEDSIZED:
                case DataType::Type::STRUCT:
                case DataType::Type::UNDEFINED:
                    throw UnknownDataType(
                        "JSON-OutputFormatting for FIXEDSIZED arrays of {} is not supported", magic_enum::enum_name(fieldType.elementType));
            }
            written += amountWritten;
            currentRemainingSize -= amountWritten;
            break;
        }
        case DataType::Type::STRUCT: {
            /// Render as JSON object: `{"f1":v1,"f2":v2,...}`. Field names and
            /// the sub-field DataType are host-side schema, so we unroll over
            /// `fieldType.fields` with `static_val<uint64_t>` and recurse into
            /// `writeValue` for each sub-VarVal pulled out of `StructData::at`.
            const auto structValue = value.getRawValueAs<StructData>();
            for (nautilus::static_val<uint64_t> i = 0; i < fieldType.fields.size(); ++i)
            {
                const auto& [subFieldName, subFieldType] = fieldType.fields.at(i);
                const nautilus::val<const char*> namePtr{subFieldName.c_str()};
                const nautilus::val<uint64_t> prefixWritten = nautilus::invoke(
                    writeStructFieldNamePrefix,
                    i == nautilus::val<uint64_t>(0),
                    namePtr,
                    currentRemainingSize,
                    recordBuffer.getReference(),
                    bufferProvider,
                    fieldPointer + written);
                written += prefixWritten;
                currentRemainingSize -= prefixWritten;

                const VarVal subValue = structValue.at(subFieldName);
                writeValue(subFieldType, subValue, fieldPointer, recordBuffer, bufferProvider, written, currentRemainingSize);
            }
            const nautilus::val<uint64_t> endWritten = nautilus::invoke(
                writeStructEnd, fieldPointer + written, currentRemainingSize, recordBuffer.getReference(), bufferProvider);
            written += endWritten;
            currentRemainingSize -= endWritten;
            break;
        }
        case DataType::Type::UNDEFINED: {
            throw UnknownDataType("JSON-OutputFormatting for type UNDEFINED is not supported.");
        }
    }
}
}

JSONOutputFormatter::JSONOutputFormatter(const std::vector<Record::RecordFieldIdentifier>& fieldNames) : OutputFormatter(fieldNames)
{
}

nautilus::val<uint64_t> JSONOutputFormatter::writeFormattedValue(
    const VarVal& value,
    const DataType& fieldType,
    const nautilus::static_val<uint64_t>& fieldIndex,
    const nautilus::val<int8_t*>& fieldPointer,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    nautilus::val<uint64_t> written{0};
    nautilus::val<uint64_t> currentRemainingSize = remainingSize;

    /// The identifier of the current field, which should be prepended to the value
    const nautilus::val<const char*> fieldName{fieldNames.at(fieldIndex).c_str()};
    /// Write the pre-value content
    const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
        writePreValueContents,
        fieldIndex == nautilus::val<uint64_t>(0),
        fieldName,
        currentRemainingSize,
        recordBuffer.getReference(),
        bufferProvider,
        fieldPointer);
    written += amountWritten;
    currentRemainingSize -= amountWritten;

    /// Handle NULL values and write value
    if (value.isNullable())
    {
        if (value.isNull())
        {
            const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                writeValueToBuffer,
                nautilus::val<const char*>{"NULL"},
                currentRemainingSize,
                recordBuffer.getReference(),
                bufferProvider,
                fieldPointer + written);
            written += amountWritten;
            currentRemainingSize -= amountWritten;
        }
        else
        {
            writeValue(fieldType, value, fieldPointer, recordBuffer, bufferProvider, written, currentRemainingSize);
        }
    }
    else
    {
        writeValue(fieldType, value, fieldPointer, recordBuffer, bufferProvider, written, currentRemainingSize);
    }

    /// Either write a , or a }\n depending on if this is the last value of the record
    const auto delimiter = nautilus::select(
        fieldIndex == nautilus::val<uint64_t>(fieldNames.size()) - 1, nautilus::val<const char*>{"}\n"}, nautilus::val<const char*>{","});

    written += nautilus::invoke(
        writeValueToBuffer, delimiter, currentRemainingSize, recordBuffer.getReference(), bufferProvider, fieldPointer + written);
    return written;
}

DescriptorConfig::Config JSONOutputFormatter::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<OutputFormatterConfig::ConfigParametersJSON>(std::move(config), "JSON");
}

std::ostream& operator<<(std::ostream& out, const JSONOutputFormatter&)
{
    return out << fmt::format("JSONOutputFormatter()");
}

OutputFormatterValidationRegistryReturnType
OutputFormatterValidationGeneratedRegistrar::RegisterJSONOutputFormatterValidation(OutputFormatterValidationRegistryArguments args)
{
    return JSONOutputFormatter::validateAndFormat(std::move(args.config));
}

OutputFormatterRegistryReturnType OutputFormatterGeneratedRegistrar::RegisterJSONOutputFormatter(OutputFormatterRegistryArguments args)
{
    return std::make_unique<JSONOutputFormatter>(std::move(args.fieldNames));
}
}
