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
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <fmt/format.h>
#include <std/cstring.h>

#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <OutputFormatterRegistry.hpp>
#include <OutputFormatterValidationRegistry.hpp>
#include <function.hpp>
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

/// Formats one fixed-size field (or "NULL") plus its trailing delimiter and writes both with a single buffer write.
template <typename T>
uint64_t writeFieldAndDelimiterProxy(
    const T value,
    const bool isNull,
    int8_t* fieldPointer,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider,
    const char* delimiter)
{
    std::string formattedValue = isNull ? std::string{"NULL"} : formatValueAsString(value);
    formattedValue += delimiter;
    return writeValueToBuffer(formattedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, fieldPointer);
}

/// Formats a varsized value via the writeVarsized proxy and advances the caller's written/remaining counters.
void writeVarsizedField(
    const VarVal& value,
    const bool quoteStrings,
    const nautilus::val<int8_t*>& fieldPointer,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    nautilus::val<uint64_t>& written,
    nautilus::val<uint64_t>& currentRemainingSize)
{
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
}
}

CSVOutputFormatter::CSVOutputFormatter(
    const std::vector<Record::RecordFieldIdentifier>& fieldNames, const OutputFormatterDescriptor& descriptor)
    : OutputFormatter(fieldNames)
    , quoteStrings(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::QUOTE_STRINGS))
    , fieldDelimiter(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::FIELD_DELIMITER))
    , tupleDelimiter(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::TUPLE_DELIMITER))
{
}

nautilus::val<uint64_t> CSVOutputFormatter::writeFormattedValue(
    CompilationContext& compilationContext,
    const VarVal& value,
    const DataType& fieldType,
    uint64_t fieldIndex,
    const nautilus::val<int8_t*>& fieldPointer,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    /// The delimiter following the value is a trace-time constant: the tuple delimiter after the last field, the
    /// field delimiter otherwise. Both strings are members of this formatter, which outlives the pipeline.
    const char* delimiter = (fieldIndex == fieldNames.size() - 1) ? tupleDelimiter.c_str() : fieldDelimiter.c_str();

    /// Formats a fixed-size field via a nautilus function shared by all like-typed fields, instead of inlining the
    /// formatting at every column -- that collapses the traced IR and cuts JIT compile time on wide schemas.
    /// The delimiter is a trace-time constant per column, so it travels as a plain argument into the shared function.
    const auto writeField
        = [&compilationContext, &value, &fieldPointer, &remainingSize, &recordBuffer, &bufferProvider, delimiter]<typename T>(
              const std::string& functionName)
    {
        auto& writeFunction = compilationContext.registerTracedInvoke(functionName, writeFieldAndDelimiterProxy<T>);
        const nautilus::val<bool> isNull = value.isNullable() ? value.isNull() : nautilus::val<bool>{false};
        return writeFunction(
            value.getRawValueAs<nautilus::val<T>>(),
            isNull,
            fieldPointer,
            remainingSize,
            recordBuffer.getReference(),
            bufferProvider,
            nautilus::val<const char*>{delimiter});
    };

    switch (fieldType.type)
    {
        case DataType::Type::BOOLEAN:
            return writeField.operator()<bool>("CSVWriteField_BOOLEAN");
        /// INT8 and INT16 are formatted through int32_t: casting them to their own C++ types makes to_string treat
        /// them as unsigned, so all three share the int32_t formatting function.
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
            return writeField.operator()<int32_t>("CSVWriteField_INT32");
        case DataType::Type::INT64:
            return writeField.operator()<int64_t>("CSVWriteField_INT64");
        case DataType::Type::UINT8:
            return writeField.operator()<uint8_t>("CSVWriteField_UINT8");
        case DataType::Type::UINT16:
            return writeField.operator()<uint16_t>("CSVWriteField_UINT16");
        case DataType::Type::UINT32:
            return writeField.operator()<uint32_t>("CSVWriteField_UINT32");
        case DataType::Type::UINT64:
            return writeField.operator()<uint64_t>("CSVWriteField_UINT64");
        case DataType::Type::FLOAT32:
            return writeField.operator()<float>("CSVWriteField_FLOAT32");
        case DataType::Type::FLOAT64:
            return writeField.operator()<double>("CSVWriteField_FLOAT64");
        case DataType::Type::CHAR:
            return writeField.operator()<char>("CSVWriteField_CHAR");
        case DataType::Type::VARSIZED: {
            nautilus::val<uint64_t> written{0};
            nautilus::val<uint64_t> currentRemainingSize = remainingSize;
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
                    writeVarsizedField(value, quoteStrings, fieldPointer, recordBuffer, bufferProvider, written, currentRemainingSize);
                }
            }
            else
            {
                writeVarsizedField(value, quoteStrings, fieldPointer, recordBuffer, bufferProvider, written, currentRemainingSize);
            }

            /// As formatting is finished for this value after this function, currentRemainingSize does not have to be adjusted anymore
            written += nautilus::invoke(
                writeValueToBuffer,
                nautilus::val<const char*>{delimiter},
                currentRemainingSize,
                recordBuffer.getReference(),
                bufferProvider,
                fieldPointer + written);
            return written;
        }
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("CSV-OutputFormatting for type UNDEFINED is not supported.");
    }
    std::unreachable();
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
