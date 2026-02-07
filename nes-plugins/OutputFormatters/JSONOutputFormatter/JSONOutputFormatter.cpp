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

#include "JSONOutputFormatter.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <fmt/format.h>
#include <std/cstring.h>

#include <OutputFormatterRegistry.hpp>
#include <OutputFormatterValidationRegistry.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>

namespace NES
{

JSONOutputFormatter::JSONOutputFormatter(const size_t numberOfFields) : OutputFormatter(numberOfFields)
{
}

nautilus::val<size_t> JSONOutputFormatter::getFormattedValue(
    VarVal value,
    const Record::RecordFieldIdentifier& fieldName,
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

    /// Write the pre-value content
    nautilus::val<bool> firstField = fieldIndex == nautilus::val<uint64_t>(0);
    /// Calculate the size of the pre-value content. Includes the size of the escaped fieldname, the colon and, optionally, the curly bracket
    nautilus::val<size_t> preFieldContentSize = fieldName.size() + 3 + nautilus::val<uint8_t>(firstField);

    if (preFieldContentSize > currentRemainingSize)
    {
        if (!allowChildren)
        {
            return INVALID_WRITE_RETURN;
        }
        nautilus::invoke(
            +[](const bool isFirstField,
                const std::string* fieldIdentifier,
                const uint64_t remainingSpace,
                TupleBuffer* buffer,
                AbstractBufferProvider* bufferProvider,
                int8_t* bufferAddress)
            {
                std::string preValueContentString = fmt::format("\"{}\":", *fieldIdentifier);
                if (isFirstField)
                {
                    preValueContentString = "{" + preValueContentString;
                }
                writeWithChildBuffers(preValueContentString, remainingSpace, buffer, bufferProvider, bufferAddress);
            },
            firstField,
            nautilus::val<const std::string*>(&fieldName),
            currentRemainingSize,
            recordBuffer.getReference(),
            bufferProvider,
            fieldPointer);
        written += currentRemainingSize;
        currentRemainingSize -= currentRemainingSize;
    }
    else
    {
        if (firstField)
        {
            nautilus::memcpy(fieldPointer, "{", 1);
            written += 1;
            currentRemainingSize -= 1;
        }
        nautilus::memcpy(fieldPointer + written, "\"", 1);
        written += 1;
        currentRemainingSize -= 1;
        nautilus::memcpy(fieldPointer + written, fieldName.c_str(), fieldName.size());
        written += fieldName.size();
        currentRemainingSize -= fieldName.size();
        nautilus::memcpy(fieldPointer + written, "\":", 2);
        written += 2;
        currentRemainingSize -= 2;
    }

    /// Write the value
    if (fieldType.type != DataType::Type::VARSIZED)
    {
        /// Convert the VarVal to a string and write it into the address.
        nautilus::val<size_t> amountWritten = formatAndWriteVal(
            value, fieldType, fieldPointer + written, currentRemainingSize, allowChildren, recordBuffer, bufferProvider);
        if (amountWritten == INVALID_WRITE_RETURN)
        {
            return INVALID_WRITE_RETURN;
        }
        written += amountWritten;
        currentRemainingSize -= amountWritten;
    }
    else
    {
        /// For varsized values, we cast to VariableSizedData and access the formatted string that way
        const auto varSizedValue = value.cast<VariableSizedData>();
        /// Calculate the size of the content that needs to be written, counting the escape characters
        const nautilus::val<size_t> amountToWrite = varSizedValue.getSize() + 2;
        if (amountToWrite > currentRemainingSize)
        {
            if (!allowChildren)
            {
                return INVALID_WRITE_RETURN;
            }
            /// Convert the varsized value to a string and allocate child memory to fully write it
            nautilus::invoke(
                +[](int8_t* bufferStartingAddress,
                    const uint64_t remainingSpace,
                    const int8_t* varSizedContent,
                    const uint64_t contentSize,
                    TupleBuffer* tupleBuffer,
                    AbstractBufferProvider* bufferProvider)
                {
                    std::string stringFormattedValue
                        = fmt::format("\"{}\"", std::string(reinterpret_cast<const char*>(varSizedContent), contentSize));
                    writeWithChildBuffers(stringFormattedValue, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
                },
                fieldPointer + written,
                currentRemainingSize,
                varSizedValue.getContent(),
                varSizedValue.getSize(),
                recordBuffer.getReference(),
                bufferProvider);

            written += currentRemainingSize;
            currentRemainingSize -= currentRemainingSize;
        }
        else
        {
            /// Copy varsized value into the record buffer without invoke
            nautilus::memcpy(fieldPointer, nautilus::val<const char*>("\""), 1);
            written += 1;
            currentRemainingSize -= 1;
            nautilus::memcpy(fieldPointer + written, varSizedValue.getContent(), varSizedValue.getSize());
            written += varSizedValue.getSize();
            currentRemainingSize -= varSizedValue.getSize();
            nautilus::memcpy(fieldPointer + written, nautilus::val<const char*>("\""), 1);
            written += 1;
            currentRemainingSize -= 1;
        }
    }
    /// Either write a , or a }\n depending on if the got the last value of the record
    nautilus::val<const char*> delimiter("");
    if (fieldIndex == nautilus::val<uint64_t>(numberOfFields) - 1)
    {
        delimiter = "}\n";
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
            return INVALID_WRITE_RETURN;
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
            recordBuffer.getReference(),
            bufferProvider,
            fieldPointer + written);
        written += currentRemainingSize;
    }
    else
    {
        nautilus::memcpy(fieldPointer + written, delimiter, delimiterLength);
        written += delimiterLength;
    }
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
    return std::make_unique<JSONOutputFormatter>(args.numberOfFields);
}

}
