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
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <OutputFormatterRegistry.hpp>
#include <OutputFormatterValidationRegistry.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
CSVOutputFormatter::CSVOutputFormatter(const size_t numberOfFields, const OutputFormatterDescriptor& descriptor)
    : OutputFormatter(numberOfFields)
    , escapeStrings(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::ESCAPE_STRINGS))
    , fieldDelimiter(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::FIELD_DELIMITER))
    , tupleDelimiter(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::TUPLE_DELIMITER))
{
}

nautilus::val<uint64_t> CSVOutputFormatter::writeFormattedValue(
    VarVal value,
    const Record::RecordFieldIdentifier&,
    const DataType& fieldType,
    const nautilus::static_val<uint64_t>& fieldIndex,
    const nautilus::val<int8_t*>& fieldPointer,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    nautilus::val<uint64_t> written(0);
    nautilus::val<uint64_t> currentRemainingSize = remainingSize;
    if (fieldType.type != DataType::Type::VARSIZED)
    {
        /// Convert the VarVal to a string and write it into the address.
        const nautilus::val<uint64_t> amountWritten
            = formatAndWriteVal(value, fieldType, fieldPointer, currentRemainingSize, recordBuffer, bufferProvider);
        written += amountWritten;
        currentRemainingSize -= amountWritten;
    }
    else
    {
        /// For varsized values, we cast to VariableSizedData and access the formatted string that way
        const auto varSizedValue = value.cast<VariableSizedData>();
        /// Calculate the size of the content that needs to be written
        const nautilus::val<uint64_t> amountToWrite
            = varSizedValue.getSize() + nautilus::val<uint8_t>(static_cast<uint8_t>(escapeStrings)) * 2;
        if (amountToWrite > currentRemainingSize)
        {
            /// Convert the varsized value to a string and allocate child memory to fully write it
            nautilus::invoke(
                +[](int8_t* bufferStartingAddress,
                    const uint64_t remainingSpace,
                    const bool escapeStrings,
                    const int8_t* varSizedContent,
                    const uint64_t contentSize,
                    TupleBuffer* tupleBuffer,
                    AbstractBufferProvider* bufferProvider)
                {
                    std::string stringFormattedValue(reinterpret_cast<const char*>(varSizedContent), contentSize);
                    if (escapeStrings)
                    {
                        stringFormattedValue = "\"" + stringFormattedValue + "\"";
                    }
                    writeWithChildBuffers(stringFormattedValue, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
                },
                fieldPointer,
                currentRemainingSize,
                nautilus::val<bool>(escapeStrings),
                varSizedValue.getContent(),
                varSizedValue.getSize(),
                recordBuffer.getReference(),
                bufferProvider);
            /// We only keep track of the bytes written in the main record buffer, so we only increment by the remaining size
            written += currentRemainingSize;
            currentRemainingSize -= currentRemainingSize;
        }
        else
        {
            /// Copy varsized value into the record buffer without invoke
            if (escapeStrings)
            {
                nautilus::memcpy(fieldPointer, nautilus::val<const char*>("\""), 1);
                written += 1;
                currentRemainingSize -= 1;
            }
            nautilus::memcpy(fieldPointer + written, varSizedValue.getContent(), varSizedValue.getSize());
            written += varSizedValue.getSize();
            currentRemainingSize -= varSizedValue.getSize();
            if (escapeStrings)
            {
                nautilus::memcpy(fieldPointer + written, nautilus::val<const char*>("\""), 1);
                written += 1;
                currentRemainingSize -= 1;
            }
        }
    }
    /// Write either the field delimiter or the tuple delimiter, depending on the field index
    nautilus::val<const char*> delimiter("");
    if (fieldIndex == nautilus::val<uint64_t>(numberOfFields) - 1)
    {
        delimiter = tupleDelimiter.c_str();
    }
    else
    {
        delimiter = fieldDelimiter.c_str();
    }

    nautilus::val delimiterLength(nautilus::strlen(delimiter));
    if (delimiterLength > currentRemainingSize)
    {
        nautilus::invoke(
            +[](const char* delimiterPointer,
                const uint64_t remainingSpace,
                int8_t* bufferStartingAddress,
                TupleBuffer* tupleBuffer,
                AbstractBufferProvider* bufferProvider)
            {
                const std::string delimiterString(delimiterPointer);
                writeWithChildBuffers(delimiterString, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
            },
            delimiter,
            currentRemainingSize,
            fieldPointer + written,
            recordBuffer.getReference(),
            bufferProvider);
        written += currentRemainingSize;
    }
    else
    {
        nautilus::memcpy(fieldPointer + written, delimiter, delimiterLength);
        written += delimiterLength;
    }
    return written;
}

std::ostream& operator<<(std::ostream& out, const CSVOutputFormatter& format)
{
    return out << fmt::format(
               "CSVOutputFormatter(Escape Strings: {}, Field Delimiter: {}, Tuple Delimiter: {})",
               format.escapeStrings,
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
    return std::make_unique<CSVOutputFormatter>(args.numberOfFields, args.descriptor);
}

}
