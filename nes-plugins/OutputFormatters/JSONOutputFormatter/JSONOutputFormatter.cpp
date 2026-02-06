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
#include <fmt/format.h>
#include <OutputFormatterRegistry.hpp>
#include <OutputFormatterValidationRegistry.hpp>
#include <static.hpp>
#include <val.hpp>

namespace NES
{

JSONOutputFormatter::JSONOutputFormatter(const size_t numberOfFields) : OutputFormatter(numberOfFields)
{
}

template <typename T>
size_t formatValToString(
    const T val,
    const std::string* fieldName,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const bool isFirst,
    const bool isLast,
    const DataType* physicalType,
    const bool allowChildren,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    std::string stringFormattedValue = isFirst ? "{" : "";
    /// Add field name
    stringFormattedValue += fmt::format("\"{}\":", *fieldName);
    /// Add value
    stringFormattedValue += physicalType->formattedBytesToString(&val);
    stringFormattedValue += isLast ? "}\n" : ",";

    /// Write bytes of the string into the buffer starting adress if space suffices.
    const size_t stringSize = stringFormattedValue.size();
    if (stringSize > remainingSpace)
    {
        if (allowChildren)
        {
            OutputFormatter::writeWithChildBuffers(
                stringFormattedValue, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
            return stringSize;
        }
        return std::string::npos;
    }
    std::memcpy(bufferStartingAddress, stringFormattedValue.c_str(), stringSize);
    return stringSize;
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
    nautilus::val<bool> isfirstField = fieldIndex == nautilus::val<uint64_t>(0);
    nautilus::val<bool> isLastField = fieldIndex == (nautilus::val<uint64_t>(numberOfFields) - 1);
    if (fieldType.type != DataType::Type::VARSIZED)
    {
        /// Invoke c++ function that parses value into a string and writes it to the field address
        /// The function will return the number of written bytes
        /// It is also responsible for writing any format specific characters
        switch (fieldType.type)
        {
            case DataType::Type::BOOLEAN: {
                const nautilus::val<bool> castedVal = value.cast<nautilus::val<bool>>();
                return nautilus::invoke(
                    formatValToString<bool>,
                    castedVal,
                    nautilus::val<const std::string*>(&fieldName),
                    fieldPointer,
                    remainingSize,
                    isfirstField,
                    isLastField,
                    nautilus::val<const DataType*>(&fieldType),
                    allowChildren,
                    recordBuffer.getReference(),
                    bufferProvider);
            }
            case DataType::Type::INT8: {
                const nautilus::val<int8_t> castedVal = value.cast<nautilus::val<int8_t>>();
                return nautilus::invoke(
                    formatValToString<int8_t>,
                    castedVal,
                    nautilus::val<const std::string*>(&fieldName),
                    fieldPointer,
                    remainingSize,
                    isfirstField,
                    isLastField,
                    nautilus::val<const DataType*>(&fieldType),
                    allowChildren,
                    recordBuffer.getReference(),
                    bufferProvider);
            }
            case DataType::Type::INT16: {
                const nautilus::val<int16_t> castedVal = value.cast<nautilus::val<int16_t>>();
                return nautilus::invoke(
                    formatValToString<int16_t>,
                    castedVal,
                    nautilus::val<const std::string*>(&fieldName),
                    fieldPointer,
                    remainingSize,
                    isfirstField,
                    isLastField,
                    nautilus::val<const DataType*>(&fieldType),
                    allowChildren,
                    recordBuffer.getReference(),
                    bufferProvider);
            }
            case DataType::Type::INT32: {
                const nautilus::val<int32_t> castedVal = value.cast<nautilus::val<int32_t>>();
                return nautilus::invoke(
                    formatValToString<int32_t>,
                    castedVal,
                    nautilus::val<const std::string*>(&fieldName),
                    fieldPointer,
                    remainingSize,
                    isfirstField,
                    isLastField,
                    nautilus::val<const DataType*>(&fieldType),
                    allowChildren,
                    recordBuffer.getReference(),
                    bufferProvider);
            }
            case DataType::Type::INT64: {
                const nautilus::val<int64_t> castedVal = value.cast<nautilus::val<int64_t>>();
                return nautilus::invoke(
                    formatValToString<int64_t>,
                    castedVal,
                    nautilus::val<const std::string*>(&fieldName),
                    fieldPointer,
                    remainingSize,
                    isfirstField,
                    isLastField,
                    nautilus::val<const DataType*>(&fieldType),
                    allowChildren,
                    recordBuffer.getReference(),
                    bufferProvider);
            }
            case DataType::Type::CHAR: {
                const nautilus::val<char> castedVal = value.cast<nautilus::val<char>>();
                return nautilus::invoke(
                    formatValToString<char>,
                    castedVal,
                    nautilus::val<const std::string*>(&fieldName),
                    fieldPointer,
                    remainingSize,
                    isfirstField,
                    isLastField,
                    nautilus::val<const DataType*>(&fieldType),
                    allowChildren,
                    recordBuffer.getReference(),
                    bufferProvider);
            }
            case DataType::Type::UINT8: {
                const nautilus::val<uint8_t> castedVal = value.cast<nautilus::val<uint8_t>>();
                return nautilus::invoke(
                    formatValToString<uint8_t>,
                    castedVal,
                    nautilus::val<const std::string*>(&fieldName),
                    fieldPointer,
                    remainingSize,
                    isfirstField,
                    isLastField,
                    nautilus::val<const DataType*>(&fieldType),
                    allowChildren,
                    recordBuffer.getReference(),
                    bufferProvider);
            }
            case DataType::Type::UINT16: {
                const nautilus::val<uint16_t> castedVal = value.cast<nautilus::val<uint16_t>>();
                return nautilus::invoke(
                    formatValToString<uint16_t>,
                    castedVal,
                    nautilus::val<const std::string*>(&fieldName),
                    fieldPointer,
                    remainingSize,
                    isfirstField,
                    isLastField,
                    nautilus::val<const DataType*>(&fieldType),
                    allowChildren,
                    recordBuffer.getReference(),
                    bufferProvider);
            }
            case DataType::Type::UINT32: {
                const nautilus::val<uint32_t> castedVal = value.cast<nautilus::val<uint32_t>>();
                return nautilus::invoke(
                    formatValToString<uint32_t>,
                    castedVal,
                    nautilus::val<const std::string*>(&fieldName),
                    fieldPointer,
                    remainingSize,
                    isfirstField,
                    isLastField,
                    nautilus::val<const DataType*>(&fieldType),
                    allowChildren,
                    recordBuffer.getReference(),
                    bufferProvider);
            }
            case DataType::Type::UINT64: {
                const nautilus::val<uint64_t> castedVal = value.cast<nautilus::val<uint64_t>>();
                return nautilus::invoke(
                    formatValToString<uint64_t>,
                    castedVal,
                    nautilus::val<const std::string*>(&fieldName),
                    fieldPointer,
                    remainingSize,
                    isfirstField,
                    isLastField,
                    nautilus::val<const DataType*>(&fieldType),
                    allowChildren,
                    recordBuffer.getReference(),
                    bufferProvider);
            }
            case DataType::Type::FLOAT32: {
                const nautilus::val<float> castedVal = value.cast<nautilus::val<float>>();
                return nautilus::invoke(
                    formatValToString<float>,
                    castedVal,
                    nautilus::val<const std::string*>(&fieldName),
                    fieldPointer,
                    remainingSize,
                    isfirstField,
                    isLastField,
                    nautilus::val<const DataType*>(&fieldType),
                    allowChildren,
                    recordBuffer.getReference(),
                    bufferProvider);
            }
            case DataType::Type::FLOAT64: {
                const nautilus::val<double> castedVal = value.cast<nautilus::val<double>>();
                return nautilus::invoke(
                    formatValToString<double>,
                    castedVal,
                    nautilus::val<const std::string*>(&fieldName),
                    fieldPointer,
                    remainingSize,
                    isfirstField,
                    isLastField,
                    nautilus::val<const DataType*>(&fieldType),
                    allowChildren,
                    recordBuffer.getReference(),
                    bufferProvider);
            }
            default: {
                return std::string::npos;
            }
        }
    }
    /// For varsized values, we cast to VariableSizedData.
    const auto varSizedValue = value.cast<VariableSizedData>();
    return nautilus::invoke(
        +[](int8_t* bufferStartingAddress,
            const uint64_t remainingSpace,
            const bool isFirst,
            const bool isLast,
            const int8_t* varSizedContent,
            const std::string* fieldName,
            const uint64_t contentSize,
            const bool allowChildren,
            TupleBuffer* tupleBuffer,
            AbstractBufferProvider* bufferProvider)
        {
            /// Convert the content to a string
            std::string stringFormattedValue = isFirst ? "{" : "";
            stringFormattedValue += fmt::format("\"{}\":", *fieldName);
            stringFormattedValue += fmt::format("\"{}\"", std::string(reinterpret_cast<const char*>(varSizedContent), contentSize));
            stringFormattedValue += isLast ? "}\n" : ",";

            const size_t stringSize = stringFormattedValue.size();
            if (stringSize > remainingSpace)
            {
                if (allowChildren)
                {
                    writeWithChildBuffers(stringFormattedValue, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
                    return stringSize;
                }
                return std::string::npos;
            }
            std::memcpy(bufferStartingAddress, stringFormattedValue.c_str(), stringSize);
            return stringSize;
        },
        fieldPointer,
        remainingSize,
        isfirstField,
        isLastField,
        varSizedValue.getContent(),
        nautilus::val<const std::string*>(&fieldName),
        varSizedValue.getSize(),
        allowChildren,
        recordBuffer.getReference(),
        bufferProvider);
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
