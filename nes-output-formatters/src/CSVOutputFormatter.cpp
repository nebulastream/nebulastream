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

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <OutputFormatterRegistry.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Configurations/Descriptor.hpp>
#include <OutputFormatterValidationRegistry.hpp>

namespace NES
{
CSVOutputFormatter::CSVOutputFormatter(const size_t numberOfFields, const OutputFormatterDescriptor& descriptor)
    : OutputFormatter(numberOfFields), escapeStrings(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::ESCAPE_STRINGS))
{
}

template <typename T>
size_t formatValToString(
    const T val,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const bool isLast,
    const DataType* physicalType,
    const bool allowChildren,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    std::string stringFormattedValue(physicalType->formattedBytesToString(&val));
    stringFormattedValue += isLast ? "\n" : ",";

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
    nautilus::val<bool> isLastField = fieldIndex == nautilus::val<uint64_t>(numberOfFields) - 1;
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
                    fieldPointer,
                    remainingSize,
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
                    fieldPointer,
                    remainingSize,
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
                    fieldPointer,
                    remainingSize,
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
                    fieldPointer,
                    remainingSize,
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
                    fieldPointer,
                    remainingSize,
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
                    fieldPointer,
                    remainingSize,
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
                    fieldPointer,
                    remainingSize,
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
                    fieldPointer,
                    remainingSize,
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
                    fieldPointer,
                    remainingSize,
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
                    fieldPointer,
                    remainingSize,
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
                    fieldPointer,
                    remainingSize,
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
                    fieldPointer,
                    remainingSize,
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
            const bool isLast,
            const bool escapeStrings,
            const int8_t* varSizedContent,
            const uint64_t contentSize,
            const bool allowChildren,
            TupleBuffer* tupleBuffer,
            AbstractBufferProvider* bufferProvider)
        {
            /// Convert the content to a string
            std::string stringFormattedValue(reinterpret_cast<const char*>(varSizedContent), contentSize);
            if (escapeStrings)
            {
                stringFormattedValue = "\"" + stringFormattedValue + "\"";
            }
            stringFormattedValue += isLast ? "\n" : ",";

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
        isLastField,
        nautilus::val<bool>(escapeStrings),
        varSizedValue.getContent(),
        varSizedValue.getSize(),
        allowChildren,
        recordBuffer.getReference(),
        bufferProvider);
}

std::ostream& operator<<(std::ostream& out, const CSVOutputFormatter& format)
{
    return out << fmt::format("CSVOutputFormatter(Escape Strings: {})", format.escapeStrings);
}

DescriptorConfig::Config CSVOutputFormatter::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<OutputFormatterConfig::ConfigParametersCSV>(std::move(config), "CSV");
}

OutputFormatterValidationRegistryReturnType OutputFormatterValidationGeneratedRegistrar::RegisterCSVOutputFormatterValidation(OutputFormatterValidationRegistryArguments args)
{
    return CSVOutputFormatter::validateAndFormat(args.config);
}

OutputFormatterRegistryReturnType OutputFormatterGeneratedRegistrar::RegisterCSVOutputFormatter(OutputFormatterRegistryArguments args)
{
    return std::make_unique<CSVOutputFormatter>(args.numberOfFields, args.descriptor);
}

}
