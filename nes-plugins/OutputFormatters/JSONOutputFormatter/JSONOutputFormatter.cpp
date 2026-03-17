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
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
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
        default: {
            const nautilus::val<uint64_t> amountWritten
                = formatAndWriteVal(value, fieldType, fieldPointer + written, currentRemainingSize, recordBuffer, bufferProvider);
            written += amountWritten;
            currentRemainingSize -= amountWritten;
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
