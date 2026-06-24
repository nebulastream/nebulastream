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
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>
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
NAUTILUS_INLINE uint64_t writeVarsized(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const bool quoteStrings,
    const int8_t* varSizedContent,
    const uint64_t contentSize,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    /// Fast path (no quoting): copy the raw source bytes straight into the output buffer -- no per-value
    /// std::string allocation, no strlen. A child buffer is touched only if the row genuinely overflows
    /// the main buffer (handled inside writeBytesToBuffer).
    const char* const data = reinterpret_cast<const char*>(varSizedContent);
    /// Fast path (no quoting): copy the raw source bytes straight into the output buffer -- no per-value
    /// std::string allocation, no strlen. A child buffer is touched only on genuine main-buffer overflow.
    if (!quoteStrings)
    {
        return writeBytesToBuffer(data, contentSize, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
    }
    /// Quoting requested. The common case has no embedded quote to escape, so we can still avoid the
    /// std::string: write `"` + raw bytes + `"` directly when the quoted value fits in the main buffer.
    if (std::memchr(varSizedContent, '"', contentSize) == nullptr && tupleBuffer->getNumberOfChildBuffers() == 0
        && contentSize + 2 <= remainingSpace)
    {
        bufferStartingAddress[0] = static_cast<int8_t>('"');
        std::memcpy(bufferStartingAddress + 1, data, contentSize);
        bufferStartingAddress[1 + contentSize] = static_cast<int8_t>('"');
        return contentSize + 2;
    }
    /// Slow path: embedded quotes need doubling, or the value spans into a child buffer.
    std::string stringFormattedValue{data, contentSize};
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
    const std::string& parserType)
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
            /// Resolved during compile-time
            if (value.isLazyValue())
            {
                /// Treat lazyValue like a variable-sized value
                /// Todo: There might be cases where the desired parsed representation in the sink differs from the one in the source. For these cases, we should parse anyway.
                const auto lazyValue = value.getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
                const auto amountWritten = nautilus::invoke(
                    writeVarsized,
                    fieldPointer,
                    currentRemainingSize,
                    nautilus::val<bool>{false},
                    lazyValue->getContent(),
                    lazyValue->getSize(),
                    recordBuffer.getReference(),
                    bufferProvider);
                written += amountWritten;
                currentRemainingSize -= amountWritten;
            }
            else
            {
                /// Convert the VarVal to a string and write it into the address.
                const nautilus::val<uint64_t> amountWritten
                    = formatAndWriteVal(value, fieldPointer, currentRemainingSize, recordBuffer, bufferProvider, parserType);
                written += amountWritten;
                currentRemainingSize -= amountWritten;
            }
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
    : OutputFormatter(fieldNames, descriptor)
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
                value,
                fieldType,
                fieldPointer,
                recordBuffer,
                bufferProvider,
                quoteStrings,
                written,
                currentRemainingSize,
                parserTypes.at(fieldType.type));
        }
    }
    else
    {
        writeValue(
            value,
            fieldType,
            fieldPointer,
            recordBuffer,
            bufferProvider,
            quoteStrings,
            written,
            currentRemainingSize,
            parserTypes.at(fieldType.type));
    }

    /// Write either the field delimiter or the tuple delimiter, depending on the field index.
    /// The delimiter length is known at construction time, so write the raw bytes directly (no strlen).
    const auto isLastField = fieldIndex == nautilus::val<uint64_t>{fieldNames.size()} - 1;
    const auto delimiter = nautilus::select(
        isLastField, nautilus::val<const char*>{tupleDelimiter.c_str()}, nautilus::val<const char*>{fieldDelimiter.c_str()});
    const auto delimiterLength
        = nautilus::select(isLastField, nautilus::val<uint64_t>{tupleDelimiter.size()}, nautilus::val<uint64_t>{fieldDelimiter.size()});

    /// As formatting is finished fo this value after this function, currentRemainingSize does not have to be adjusted anymore
    written += nautilus::invoke(
        writeBytesToBuffer,
        delimiter,
        delimiterLength,
        currentRemainingSize,
        recordBuffer.getReference(),
        bufferProvider,
        fieldPointer + written);
    return written;
}

std::optional<nautilus::val<uint64_t>> CSVOutputFormatter::tryWriteCoalescedRecord(
    const Record& record,
    const nautilus::val<int8_t*>& recordAddress,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    /// Host-time gate: coalescing only applies when the field delimiter is a single byte and every output
    /// field is a lazy passthrough value (raw input bytes, written unquoted -- like the lazy branch of
    /// writeValue). Otherwise decline and let the caller format field-by-field.
    if (fieldDelimiter.size() != 1)
    {
        return std::nullopt;
    }
    for (const auto& name : fieldNames)
    {
        const auto& value = record.read(name);
        /// Nullable fields need per-field NULL handling (write "NULL"); a raw byte copy would emit the
        /// underlying bytes instead. Only coalesce non-nullable lazy passthrough fields.
        if (not value.isLazyValue() or value.isNullable())
        {
            return std::nullopt;
        }
    }

    /// Runtime check: is the whole row contiguous in the source buffer, with the field delimiter as the byte
    /// between consecutive fields? If so, "f0<delim>f1...<delim>fN-1" already exists verbatim in the input and
    /// can be emitted with a single copy (the in-between delimiters ride along), then the tuple delimiter.
    const auto fieldDelimByte = nautilus::val<int8_t>(static_cast<int8_t>(fieldDelimiter[0]));
    const auto firstLazy = record.read(fieldNames.front()).getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
    nautilus::val<bool> contiguous{true};
    auto prevEnd = firstLazy->getContent() + firstLazy->getSize();
    for (nautilus::static_val<uint64_t> i = 1; i < fieldNames.size(); ++i)
    {
        const auto lazy = record.read(fieldNames.at(i)).getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
        contiguous = contiguous and (lazy->getContent() == prevEnd + nautilus::val<uint64_t>{1})
            and (readValueFromMemRef<int8_t>(prevEnd) == fieldDelimByte);
        prevEnd = lazy->getContent() + lazy->getSize();
    }

    nautilus::val<uint64_t> written{0};
    if (contiguous)
    {
        const auto lastLazy = record.read(fieldNames.back()).getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
        const nautilus::val<int8_t*> spanBegin = firstLazy->getContent();
        const nautilus::val<uint64_t> spanLen = (lastLazy->getContent() + lastLazy->getSize()) - spanBegin;
        /// One copy of the whole row's fields + their in-between delimiters (quoteStrings=false: byte path).
        written += nautilus::invoke(
            writeVarsized,
            recordAddress,
            remainingSize,
            nautilus::val<bool>{false},
            spanBegin,
            spanLen,
            recordBuffer.getReference(),
            bufferProvider);
        written += nautilus::invoke(
            writeBytesToBuffer,
            nautilus::val<const char*>{tupleDelimiter.c_str()},
            nautilus::val<uint64_t>{tupleDelimiter.size()},
            remainingSize - written,
            recordBuffer.getReference(),
            bufferProvider,
            recordAddress + written);
        return written;
    }

    /// Not contiguous (e.g. fields produced/rearranged by an upstream operator): still no parse/serialize --
    /// write each lazy field's raw bytes plus its delimiter, exactly as the per-field lazy path would.
    for (nautilus::static_val<uint64_t> i = 0; i < fieldNames.size(); ++i)
    {
        const auto lazy = record.read(fieldNames.at(i)).getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
        written += nautilus::invoke(
            writeVarsized,
            recordAddress + written,
            remainingSize - written,
            nautilus::val<bool>{false},
            lazy->getContent(),
            lazy->getSize(),
            recordBuffer.getReference(),
            bufferProvider);
        const auto& delimiter = (i + 1 == fieldNames.size()) ? tupleDelimiter : fieldDelimiter;
        written += nautilus::invoke(
            writeBytesToBuffer,
            nautilus::val<const char*>{delimiter.c_str()},
            nautilus::val<uint64_t>{delimiter.size()},
            remainingSize - written,
            recordBuffer.getReference(),
            bufferProvider,
            recordAddress + written);
    }
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
