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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
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
#include <OutputFormatterRegistry.hpp>
#include <OutputFormatterValidationRegistry.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
constexpr uint64_t INVALID_WRITE_RETURN = std::numeric_limits<uint64_t>::max();

nautilus::val<uint64_t> writePreFieldContent(
    const Record::RecordFieldIdentifier& fieldName,
    const nautilus::static_val<uint64_t>& fieldIndex,
    const nautilus::val<int8_t*>& fieldPointer,
    const nautilus::val<uint64_t>& remainingSize,
    const nautilus::val<bool>& allowChildren,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    const nautilus::val<bool> firstField = fieldIndex == nautilus::val<uint64_t>(0);
    /// Calculate the size of the pre-value content. Includes the size of the escaped fieldname, the colon and, optionally, the curly bracket
    const nautilus::val<uint64_t> preFieldContentSize = fieldName.size() + 3 + nautilus::val<uint8_t>(firstField);

    nautilus::val<uint64_t> result(0);
    if (preFieldContentSize > remainingSize)
    {
        if (!allowChildren)
        {
            result = INVALID_WRITE_RETURN;
        }
        else
        {
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
                remainingSize,
                recordBuffer.getReference(),
                bufferProvider,
                fieldPointer);
            result = remainingSize;
        }
    }
    else
    {
        nautilus::val<uint64_t> written(0);
        if (firstField)
        {
            nautilus::memcpy(fieldPointer, "{", 1);
            written += 1;
        }
        nautilus::memcpy(fieldPointer + written, "\"", 1);
        written += 1;
        nautilus::memcpy(fieldPointer + written, fieldName.c_str(), fieldName.size());
        written += fieldName.size();
        nautilus::memcpy(fieldPointer + written, "\":", 2);
        written += 2;
        result = written;
    }
    return result;
}

nautilus::val<uint64_t> writeVarsizedValue(
    const VariableSizedData& varSizedValue,
    const nautilus::val<int8_t*>& fieldPointer,
    const nautilus::val<uint64_t>& remainingSize,
    const nautilus::val<bool>& allowChildren,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    /// Calculate the size of the content that needs to be written, counting the escape characters
    const nautilus::val<uint64_t> amountToWrite = varSizedValue.getSize() + 2;

    nautilus::val<uint64_t> result(0);
    if (amountToWrite > remainingSize)
    {
        if (!allowChildren)
        {
            result = INVALID_WRITE_RETURN;
        }
        else
        {
            /// Convert the varsized value to a string and allocate child memory to fully write it
            nautilus::invoke(
                +[](int8_t* bufferStartingAddress,
                    const uint64_t remainingSpace,
                    const int8_t* varSizedContent,
                    const uint64_t contentSize,
                    TupleBuffer* tupleBuffer,
                    AbstractBufferProvider* bufferProvider)
                {
                    const std::string stringFormattedValue
                        = fmt::format("\"{}\"", std::string(reinterpret_cast<const char*>(varSizedContent), contentSize));
                    writeWithChildBuffers(stringFormattedValue, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
                },
                fieldPointer,
                remainingSize,
                varSizedValue.getContent(),
                varSizedValue.getSize(),
                recordBuffer.getReference(),
                bufferProvider);
            result = remainingSize;
        }
    }
    else
    {
        /// Copy varsized value into the record buffer without invoke
        nautilus::val<uint64_t> offset(0);
        nautilus::memcpy(fieldPointer + offset, nautilus::val<const char*>("\""), 1);
        offset += 1;
        nautilus::memcpy(fieldPointer + offset, varSizedValue.getContent(), varSizedValue.getSize());
        offset += varSizedValue.getSize();
        nautilus::memcpy(fieldPointer + offset, nautilus::val<const char*>("\""), 1);
        result = amountToWrite;
    }
    return result;
}

nautilus::val<uint64_t> writeFieldValue(
    VarVal value,
    const DataType& fieldType,
    const nautilus::val<int8_t*>& fieldPointer,
    const nautilus::val<uint64_t>& remainingSize,
    const nautilus::val<bool>& allowChildren,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    nautilus::val<uint64_t> result(0);
    if (fieldType.type != DataType::Type::VARSIZED)
    {
        /// Convert the VarVal to a string and write it into the address.
        result = formatAndWriteVal(value, fieldType, fieldPointer, remainingSize, allowChildren, recordBuffer, bufferProvider);
    }
    else
    {
        /// For varsized values, we cast to VariableSizedData and access the formatted string that way
        const auto varSizedValue = value.cast<VariableSizedData>();
        result = writeVarsizedValue(varSizedValue, fieldPointer, remainingSize, allowChildren, recordBuffer, bufferProvider);
    }
    return result;
}
}

JSONOutputFormatter::JSONOutputFormatter(const size_t numberOfFields) : OutputFormatter(numberOfFields)
{
}

nautilus::val<uint64_t> JSONOutputFormatter::writeFieldDelimiter(
    const nautilus::static_val<uint64_t>& fieldIndex,
    const nautilus::val<int8_t*>& fieldPointer,
    const nautilus::val<uint64_t>& remainingSize,
    const nautilus::val<bool>& allowChildren,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    /// Either write a , or a }\n depending on if we got the last value of the record
    nautilus::val<const char*> delimiter("");
    if (fieldIndex == nautilus::val<uint64_t>(numberOfFields) - 1)
    {
        delimiter = "}\n";
    }
    else
    {
        delimiter = ",";
    }

    const nautilus::val delimiterLength(nautilus::strlen(delimiter));

    nautilus::val<uint64_t> result(0);
    if (delimiterLength > remainingSize)
    {
        if (!allowChildren)
        {
            result = INVALID_WRITE_RETURN;
        }
        else
        {
            nautilus::invoke(
                +[](int8_t* bufferStartingAddress,
                    const uint64_t remainingSpace,
                    const char* delimiterPointer,
                    TupleBuffer* tupleBuffer,
                    AbstractBufferProvider* bufferProvider)
                {
                    const std::string delimiterString(delimiterPointer);
                    writeWithChildBuffers(delimiterString, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
                },
                fieldPointer,
                remainingSize,
                delimiter,
                recordBuffer.getReference(),
                bufferProvider);
            result = remainingSize;
        }
    }
    else
    {
        nautilus::memcpy(fieldPointer, delimiter, delimiterLength);
        result = delimiterLength;
    }
    return result;
}

nautilus::val<uint64_t> JSONOutputFormatter::getFormattedValue(
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
    nautilus::val<uint64_t> result(0);
    nautilus::val<uint64_t> currentRemainingSize = remainingSize;

    /// Write the pre-value content
    const nautilus::val<uint64_t> preFieldBytesWritten
        = writePreFieldContent(fieldName, fieldIndex, fieldPointer, currentRemainingSize, allowChildren, recordBuffer, bufferProvider);
    result += preFieldBytesWritten;
    currentRemainingSize -= preFieldBytesWritten;

    /// Write the value
    const nautilus::val<uint64_t> valueBytesWritten
        = writeFieldValue(value, fieldType, fieldPointer + result, currentRemainingSize, allowChildren, recordBuffer, bufferProvider);
    result += valueBytesWritten;
    currentRemainingSize -= valueBytesWritten;

    /// Write the delimiter
    const nautilus::val<uint64_t> delimiterBytesWritten
        = writeFieldDelimiter(fieldIndex, fieldPointer + result, currentRemainingSize, allowChildren, recordBuffer, bufferProvider);

    if (preFieldBytesWritten != INVALID_WRITE_RETURN && valueBytesWritten != INVALID_WRITE_RETURN
        && delimiterBytesWritten != INVALID_WRITE_RETURN)
    {
        result += delimiterBytesWritten;
    }
    else
    {
        result = INVALID_WRITE_RETURN;
    }

    return result;
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
