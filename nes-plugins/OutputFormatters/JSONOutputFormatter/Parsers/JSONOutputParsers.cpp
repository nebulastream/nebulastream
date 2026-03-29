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

#include <JSONOutputParsers.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <OutputParserRegistry.hpp>
#include <static.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES::JSONOutputParser
{

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

std::string escapeAsJsonString(std::string_view input)
{
    std::string out;
    out.reserve(input.size() + 2);
    out.push_back('"');
    for (const char raw : input)
    {
        const auto byte = static_cast<unsigned char>(raw);
        switch (raw)
        {
            case '"':
                out.append("\\\"");
                break;
            case '\\':
                out.append("\\\\");
                break;
            case '\b':
                out.append("\\b");
                break;
            case '\f':
                out.append("\\f");
                break;
            case '\n':
                out.append("\\n");
                break;
            case '\r':
                out.append("\\r");
                break;
            case '\t':
                out.append("\\t");
                break;
            default:
                if (byte < 0x20)
                {
                    fmt::format_to(std::back_inserter(out), "\\u{:04x}", byte);
                }
                else
                {
                    out.push_back(raw);
                }
                break;
        }
    }
    out.push_back('"');
    return out;
}

uint64_t parseChar(
    const char value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = escapeAsJsonString(std::string(1, value));
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

uint64_t parseVarsized(
    const int8_t* valueAddress,
    const uint64_t valueSize,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const std::string parsedValue = escapeAsJsonString(std::string(reinterpret_cast<const char*>(valueAddress), valueSize));
    return writeValueToBuffer(parsedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}
}

namespace NES
{
nautilus::val<uint64_t> JSONCHAROutputParser::parseAndWrite(
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
        JSONOutputParser::parseChar, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> JSONVARSIZEDOutputParser::parseAndWrite(
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
        JSONOutputParser::parseVarsized,
        castedVal.getContent(),
        castedVal.getSize(),
        startingAddress,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider);
}

nautilus::val<uint64_t> JSONFIXEDSIZEDOutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>& parserTypes,
    const DataType&) const
{
    nautilus::val<uint64_t> amountWritten = 0;
    /// Write the opening bracket. Maybe we should inline this
    amountWritten += nautilus::invoke(
        writeValueToBuffer, nautilus::val<const char*>{"["}, remainingSize, recordBuffer.getReference(), bufferProvider, startingAddress);

    const auto fixedSized = value.getRawValueAs<FixedSizedData>();
    /// Get the parser for the underlying elements
    if (fixedSized.getElementType() == DataType::Type::UNDEFINED || fixedSized.getElementType() == DataType::Type::FIXEDSIZED
        || fixedSized.getElementType() == DataType::Type::VARSIZED || fixedSized.getElementType() == DataType::Type::STRUCT)
    {
        throw UnknownDataType(
            "JSON-OutputFormatting for FIXEDSIZED arrays of {} is not supported", magic_enum::enum_name(fixedSized.getElementType()));
    }
    const auto elementParser = provideOutputParser(parserTypes.at(fixedSized.getElementType()));

    for (nautilus::static_val<uint64_t> i = 0; i < fixedSized.getNumElements(); ++i)
    {
        if (i > nautilus::val<uint64_t>{0})
        {
            amountWritten += nautilus::invoke(
                writeValueToBuffer,
                nautilus::val<const char*>{","},
                remainingSize - amountWritten,
                recordBuffer.getReference(),
                bufferProvider,
                startingAddress + amountWritten);
        }
        /// Parse ith value of the array
        amountWritten += elementParser->parseAndWrite(
            fixedSized.at(nautilus::val<uint64_t>{i}),
            remainingSize - amountWritten,
            recordBuffer,
            bufferProvider,
            startingAddress + amountWritten,
            parserTypes,
            DataType{fixedSized.getElementType(), DataType::NULLABLE::NOT_NULLABLE});
    }
    amountWritten += nautilus::invoke(
        writeValueToBuffer,
        nautilus::val<const char*>{"]"},
        remainingSize - amountWritten,
        recordBuffer.getReference(),
        bufferProvider,
        startingAddress + amountWritten);

    return amountWritten;
}

nautilus::val<uint64_t> JSONSTRUCTOutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>& parserTypes,
    const DataType& valueType) const
{
    nautilus::val<uint64_t> amountWritten{0};
    const auto structVal = value.getRawValueAs<StructData>();
    for (nautilus::static_val<uint64_t> i = 0; i < structVal.getNumFields(); ++i)
    {
        /// Write the prefix, which is a field-delimiting symbol and the fieldname
        const auto& [subFieldName, subFieldType] = valueType.fields.at(i);
        const nautilus::val<const char*> namePtr{subFieldName.c_str()};
        amountWritten += nautilus::invoke(
            JSONOutputParser::writeStructFieldNamePrefix,
            i == nautilus::val<uint64_t>{0},
            namePtr,
            remainingSize - amountWritten,
            recordBuffer.getReference(),
            bufferProvider,
            startingAddress + amountWritten);

        /// Fetch output parser for this subfield and parse the field
        const std::unique_ptr<OutputParser> subFieldParser = provideOutputParser(parserTypes.at(subFieldType.type));
        amountWritten += subFieldParser->parseAndWrite(
            structVal.at(subFieldName),
            remainingSize - amountWritten,
            recordBuffer,
            bufferProvider,
            startingAddress + amountWritten,
            parserTypes,

            subFieldType);
    }
    /// Write struct suffix
    amountWritten += nautilus::invoke(
        writeValueToBuffer,
        nautilus::val<const char*>{"}"},
        remainingSize - amountWritten,
        recordBuffer.getReference(),
        bufferProvider,
        startingAddress + amountWritten);
    return amountWritten;
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterJSONCHAROutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<JSONCHAROutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterJSONVARSIZEDOutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<JSONVARSIZEDOutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterJSONFIXEDSIZEDOutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<JSONFIXEDSIZEDOutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterJSONSTRUCTOutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<JSONSTRUCTOutputParser>();
}
}
