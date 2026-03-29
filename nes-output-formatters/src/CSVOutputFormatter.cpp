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

#include <OutputFormatters/OutputFormatterDescriptor.hpp>
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
void writeValue(
    const VarVal& value,
    const DataType& fieldType,
    const nautilus::val<int8_t*>& fieldPointer,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    nautilus::val<uint64_t>& written,
    nautilus::val<uint64_t>& currentRemainingSize,
    const std::unordered_map<DataType::Type, std::string>& parserTypes)
{
    if (fieldType.type == DataType::Type::UNDEFINED)
    {
        throw UnknownDataType("CSV-OutputFormatting for type UNDEFINED is not supported.");
    }
    const std::unique_ptr<OutputParser> parser = provideOutputParser(parserTypes.at(fieldType.type));
    const nautilus::val<uint64_t> amountWritten
        = parser->parseAndWrite(value, currentRemainingSize, recordBuffer, bufferProvider, fieldPointer, parserTypes, fieldType);
    written += amountWritten;
    currentRemainingSize -= amountWritten;
}
}

CSVOutputFormatter::CSVOutputFormatter(
    const std::vector<Record::RecordFieldIdentifier>& fieldNames, const OutputFormatterDescriptor& descriptor)
    : OutputFormatter(fieldNames)
    , fieldDelimiter(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::FIELD_DELIMITER))
    , tupleDelimiter(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::TUPLE_DELIMITER))
{
    if (descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::QUOTE_STRINGS))
    {
        parserTypes[DataType::Type::VARSIZED] = "QuotedVARSIZED";
    }
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
            writeValue(value, fieldType, fieldPointer, recordBuffer, bufferProvider, written, currentRemainingSize, parserTypes);
        }
    }
    else
    {
        writeValue(value, fieldType, fieldPointer, recordBuffer, bufferProvider, written, currentRemainingSize, parserTypes);
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
    return out << fmt::format("CSVOutputFormatter(Field Delimiter: {}, Tuple Delimiter: {})", format.fieldDelimiter, format.tupleDelimiter);
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
