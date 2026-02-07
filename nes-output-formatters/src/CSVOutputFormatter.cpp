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
#include <ranges>
#include <sstream>
#include <string>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <std/cstring.h>

#include <ErrorHandling.hpp>
#include <OutputFormatterRegistry.hpp>
#include <OutputFormatterValidationRegistry.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>

namespace NES
{
CSVOutputFormatter::CSVOutputFormatter(const size_t numberOfFields, const OutputFormatterDescriptor& descriptor)
    : OutputFormatter(numberOfFields), escapeStrings(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::ESCAPE_STRINGS))
{
}

nautilus::val<size_t> CSVOutputFormatter::getFormattedValue(
    VarVal value,
    const Record::RecordFieldIdentifier&,
    const DataType& fieldType,
    const nautilus::static_val<uint64_t>& fieldIndex,
    const nautilus::val<int8_t*>& fieldPointer,
    const nautilus::val<uint64_t>& remainingSize,
    const nautilus::val<bool>& allowChildren,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    nautilus::val<size_t> written(0);
    nautilus::val<uint64_t> currentRemainingSize = remainingSize;
    if (fieldType.type != DataType::Type::VARSIZED)
    {
        /// Convert the VarVal to a string and write it into the address.
        nautilus::val<size_t> amountWritten
            = formatAndWriteVal(value, fieldType, fieldPointer, currentRemainingSize, allowChildren, recordBuffer, bufferProvider);
        if (amountWritten == std::string::npos)
        {
            return std::string::npos;
        }
        written += amountWritten;
        currentRemainingSize -= amountWritten;
    }
    else
    {
        /// For varsized values, we cast to VariableSizedData and access the formatted string that way
        const auto varSizedValue = value.cast<VariableSizedData>();
        /// Calculate the size of the content that needs to be written
        const nautilus::val<size_t> amountToWrite = varSizedValue.getSize() + nautilus::val<uint8_t>(escapeStrings) * 2;
        if (amountToWrite > currentRemainingSize)
        {
            if (!allowChildren)
            {
                return std::string::npos;
            }
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
        delimiter = "\n";
    }
    else
    {
        delimiter = ",";
    }

    nautilus::val delimiterLength(nautilus::strlen(delimiter));
    if (delimiterLength > currentRemainingSize)
    {
        if (!allowChildren)
        {
            return std::string::npos;
        }
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
    return out << fmt::format("CSVOutputFormatter(Escape Strings: {})", format.escapeStrings);
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
