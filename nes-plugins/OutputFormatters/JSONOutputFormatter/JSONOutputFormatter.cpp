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
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <fmt/format.h>
#include <std/cstring.h>

#include <Configurations/Descriptor.hpp>
#include <OutputFormatters/OutputParser.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
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
    const VarVal& value,
    const nautilus::val<int8_t*>& fieldPointer,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    nautilus::val<uint64_t>& written,
    nautilus::val<uint64_t>& currentRemainingSize,
    const std::string& parserType)
{
    const OutputParserConfig config{.quoted = true};
    const std::unique_ptr<OutputParser> outputParser = provideOutputParser(parserType, config);
    const nautilus::val<uint64_t> amountWritten
        = outputParser->parseAndWrite(value, currentRemainingSize, recordBuffer, bufferProvider, fieldPointer);
    written += amountWritten;
    currentRemainingSize -= amountWritten;
}
}

JSONOutputFormatter::JSONOutputFormatter(
    const std::vector<Record::RecordFieldIdentifier>& fieldNames, const OutputFormatterDescriptor& descriptor)
    : OutputFormatter(fieldNames)
    , canonicalFieldNames(
          fieldNames | std::views::transform([](const auto& id) { return fmt::format("{}", id); }) | std::ranges::to<std::vector>())
{
    parserTypes[DataType::Type::UINT8] = "DefaultUINT8";
    parserTypes[DataType::Type::UINT16] = "DefaultUINT16";
    parserTypes[DataType::Type::UINT32] = "DefaultUINT32";
    parserTypes[DataType::Type::UINT64] = "DefaultUINT64";
    parserTypes[DataType::Type::INT8] = "DefaultINT8";
    parserTypes[DataType::Type::INT16] = "DefaultINT16";
    parserTypes[DataType::Type::INT32] = "DefaultINT32";
    parserTypes[DataType::Type::INT64] = "DefaultINT64";
    parserTypes[DataType::Type::FLOAT32] = "DefaultF32";
    parserTypes[DataType::Type::FLOAT64] = "DefaultF64";
    parserTypes[DataType::Type::BOOLEAN] = "DefaultBOOL";
    parserTypes[DataType::Type::CHAR] = "JSONCHAR";
    parserTypes[DataType::Type::VARSIZED] = "JSONVARSIZED";
    /// Set placeholder for UNDEFINED, we throw an error later if a field type is UNDEFINED.
    parserTypes[DataType::Type::UNDEFINED] = "";

    /// Override default parsers with user specified ones
    parseOutputParserOverrides(descriptor.getFromConfig(OutputFormatterDescriptor::OUTPUT_PARSERS), parserTypes);
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
    /// Important field name must be valid at execution time, thats why we don't calculate the canonicalisation during tracing but in ctor
    const nautilus::val<const char*> fieldName{canonicalFieldNames.at(fieldIndex).c_str()};
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
                nautilus::val<const char*>{"null"},
                currentRemainingSize,
                recordBuffer.getReference(),
                bufferProvider,
                fieldPointer + written);
            written += amountWritten;
            currentRemainingSize -= amountWritten;
        }
        else
        {
            writeValue(
                value, fieldPointer + written, recordBuffer, bufferProvider, written, currentRemainingSize, parserTypes.at(fieldType.type));
        }
    }
    else
    {
        writeValue(
            value, fieldPointer + written, recordBuffer, bufferProvider, written, currentRemainingSize, parserTypes.at(fieldType.type));
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
    return std::make_unique<JSONOutputFormatter>(std::move(args.fieldNames), std::move(args.descriptor));
}
}
