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
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <std/cstring.h>

#include <Configurations/Descriptor.hpp>
#include <OutputFormatters/OutputParser.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nlohmann/json.hpp>
#include <ErrorHandling.hpp>
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

void writeValue(
    const DataType& fieldType,
    const VarVal& value,
    const nautilus::val<int8_t*>& fieldPointer,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    nautilus::val<uint64_t>& written,
    nautilus::val<uint64_t>& currentRemainingSize,
    const std::unordered_map<DataType::Type, std::string>& parserTypes)
{
    if (fieldType.type == DataType::Type::UNDEFINED)
    {
        throw UnknownDataType("JSON-OutputFormatting for type UNDEFINED is not supported.");
    }
    const std::unique_ptr<OutputParser> outputParser = provideOutputParser(parserTypes.at(fieldType.type));
    const nautilus::val<uint64_t> amountWritten = outputParser->parseAndWrite(
        value, currentRemainingSize, recordBuffer, bufferProvider, fieldPointer + written, parserTypes, fieldType);
    written += amountWritten;
    currentRemainingSize -= amountWritten;
}
}

JSONOutputFormatter::JSONOutputFormatter(const std::vector<Record::RecordFieldIdentifier>& fieldNames) : OutputFormatter(fieldNames)
{
    /// Set specific JSON parsers for char and varsized fields, which respect JSON delimiting rules
    parserTypes[DataType::Type::CHAR] = "JSONCHAR";
    parserTypes[DataType::Type::VARSIZED] = "JSONVARSIZED";
    parserTypes[DataType::Type::FIXEDSIZED] = "JSONFIXEDSIZED";
    parserTypes[DataType::Type::STRUCT] = "JSONSTRUCT";
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
            writeValue(fieldType, value, fieldPointer, recordBuffer, bufferProvider, written, currentRemainingSize, parserTypes);
        }
    }
    else
    {
        writeValue(fieldType, value, fieldPointer, recordBuffer, bufferProvider, written, currentRemainingSize, parserTypes);
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
