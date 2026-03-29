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

#include <CSVOutputFormatter.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <fmt/format.h>
#include <std/cstring.h>

#include <OutputFormatters/FloatOutputParser.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
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
uint64_t writeVarsized(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const bool quoteStrings,
    const int8_t* varSizedContent,
    const uint64_t contentSize,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    std::string stringFormattedValue{reinterpret_cast<const char*>(varSizedContent), contentSize};
    if (quoteStrings)
    {
        /// Replace all " instances in the string with ""
        std::string stringWithDoubledQuotes;
        for (const char character : stringFormattedValue)
        {
            if (character == '"')
            {
                stringWithDoubledQuotes.append("\"\"");
            }
            else
            {
                stringWithDoubledQuotes += character;
            }
        }
        stringFormattedValue = "\"" + stringWithDoubledQuotes + "\"";
    }
    return writeValueToBuffer(stringFormattedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

void writeValue(
    const VarVal& value,
    const DataType& fieldType,
    const nautilus::val<int8_t*>& fieldPointer,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<bool>& quoteStrings,
    nautilus::val<uint64_t>& written,
    nautilus::val<uint64_t>& currentRemainingSize,
    const std::unique_ptr<FloatOutputParser>& floatParser)
{
    switch (fieldType.type)
    {
        case DataType::Type::VARSIZED: {
            /// For varsized values, we cast to VariableSizedData and access the formatted string that way
            const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
            const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                writeVarsized,
                fieldPointer,
                currentRemainingSize,
                nautilus::val<bool>{quoteStrings},
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
        case DataType::Type::FLOAT64:
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR: {
            /// Convert the VarVal to a string and write it into the address.
            const nautilus::val<uint64_t> amountWritten
                = formatAndWriteVal(value, fieldType, fieldPointer, currentRemainingSize, recordBuffer, bufferProvider, floatParser);
            written += amountWritten;
            currentRemainingSize -= amountWritten;
            break;
        }
        case DataType::Type::UNDEFINED: {
            throw UnknownDataType("CSV-OutputFormatting for type UNDEFINED is not supported.");
        }
    }
}
}

CSVOutputFormatter::CSVOutputFormatter(
    const std::vector<Record::RecordFieldIdentifier>& fieldNames, const OutputFormatterDescriptor& descriptor)
    : OutputFormatter(fieldNames, descriptor.getFromConfig(OutputFormatterDescriptor::FLOAT_PARSER))
    , quoteStrings(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::QUOTE_STRINGS))
    , fieldDelimiter(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::FIELD_DELIMITER))
    , tupleDelimiter(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::TUPLE_DELIMITER))
{
}

nautilus::val<uint64_t> CSVOutputFormatter::writeFormattedValue(
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
            writeValue(
                value, fieldType, fieldPointer, recordBuffer, bufferProvider, quoteStrings, written, currentRemainingSize, floatParser);
        }
    }
    else
    {
        writeValue(value, fieldType, fieldPointer, recordBuffer, bufferProvider, quoteStrings, written, currentRemainingSize, floatParser);
    }

    /// Write either the field delimiter or the tuple delimiter, depending on the field index
    const auto delimiter = nautilus::select(
        fieldIndex == nautilus::val<uint64_t>{fieldNames.size()} - 1,
        nautilus::val<const char*>{tupleDelimiter.c_str()},
        nautilus::val<const char*>{fieldDelimiter.c_str()});

    /// As formatting is finished fo this value after this function, currentRemainingSize does not have to be adjusted anymore
    written += nautilus::invoke(
        writeValueToBuffer, delimiter, currentRemainingSize, recordBuffer.getReference(), bufferProvider, fieldPointer + written);
    return written;
}

std::ostream& operator<<(std::ostream& out, const CSVOutputFormatter& format)
{
    return out << fmt::format(
               "CSVOutputFormatter(Quote Strings: {}, Field Delimiter: {}, Tuple Delimiter: {})",
               format.quoteStrings,
               format.fieldDelimiter,
               format.tupleDelimiter);
}

DescriptorConfig::Config CSVOutputFormatter::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<OutputFormatterConfig::ConfigParametersCSV>(std::move(config), "CSV");
}

OutputFormatterValidationRegistryReturnType
OutputFormatterValidationGeneratedRegistrar::RegisterCSVOutputFormatterValidation(OutputFormatterValidationRegistryArguments args)
{
    return CSVOutputFormatter::validateAndFormat(args.config);
}

OutputFormatterRegistryReturnType OutputFormatterGeneratedRegistrar::RegisterCSVOutputFormatter(OutputFormatterRegistryArguments args)
{
    return std::make_unique<CSVOutputFormatter>(std::move(args.fieldNames), std::move(args.descriptor));
}

}
