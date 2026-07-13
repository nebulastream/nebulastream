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
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
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
#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>
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
/// Append `character` to `out` with JSON string escaping (RFC 8259: `"`, `\`, and control
/// characters below 0x20 must be escaped; everything else passes through byte-for-byte).
void appendJsonEscaped(std::string& out, const char character)
{
    switch (character)
    {
        case '"':
            out += "\\\"";
            return;
        case '\\':
            out += "\\\\";
            return;
        case '\b':
            out += "\\b";
            return;
        case '\f':
            out += "\\f";
            return;
        case '\n':
            out += "\\n";
            return;
        case '\r':
            out += "\\r";
            return;
        case '\t':
            out += "\\t";
            return;
        default:
            if (static_cast<unsigned char>(character) < 0x20)
            {
                constexpr std::string_view hexDigits = "0123456789abcdef";
                out += "\\u00";
                out += hexDigits[(static_cast<unsigned char>(character) >> 4U) & 0xFU];
                out += hexDigits[static_cast<unsigned char>(character) & 0xFU];
                return;
            }
            out += character;
    }
}

/// Host-side (construction-time) JSON escaping for field names.
std::string jsonEscapeString(const std::string_view raw)
{
    std::string escaped;
    escaped.reserve(raw.size());
    for (const char character : raw)
    {
        appendJsonEscaped(escaped, character);
    }
    return escaped;
}

/// Write the escape sequence for one escapable byte into `out`; returns its length. Callers have
/// already established that `character` needs escaping (`"`, `\`, or a control byte < 0x20).
uint64_t writeEscapeSequence(int8_t* out, const char character)
{
    char second = 0;
    switch (character)
    {
        case '"':
            second = '"';
            break;
        case '\\':
            second = '\\';
            break;
        case '\b':
            second = 'b';
            break;
        case '\f':
            second = 'f';
            break;
        case '\n':
            second = 'n';
            break;
        case '\r':
            second = 'r';
            break;
        case '\t':
            second = 't';
            break;
        default: {
            constexpr std::string_view hexDigits = "0123456789abcdef";
            out[0] = '\\';
            out[1] = 'u';
            out[2] = '0';
            out[3] = '0';
            out[4] = static_cast<int8_t>(hexDigits[(static_cast<unsigned char>(character) >> 4U) & 0xFU]);
            out[5] = static_cast<int8_t>(hexDigits[static_cast<unsigned char>(character) & 0xFU]);
            return 6;
        }
    }
    out[0] = '\\';
    out[1] = static_cast<int8_t>(second);
    return 2;
}

/// Write a JSON string value: `"` + content + `"`, escaping where required. Fast path: even the
/// worst-case escape expansion (6x, \u00XX) fits the remaining main-buffer space -- virtually
/// always mid-buffer -- so escape straight into the output buffer in clean-run chunks (no
/// intermediate std::string). Degenerates to quote + one memcpy + quote when nothing needs
/// escaping. Slow path only when the value might span into a child buffer: build the escaped
/// string and let writeBytesToBuffer handle the spilling.
uint64_t writeJsonStringValue(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const int8_t* content,
    const uint64_t contentSize,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const char* const data = reinterpret_cast<const char*>(content);
    if (tupleBuffer->getNumberOfChildBuffers() == 0 && contentSize * 6 + 2 <= remainingSpace)
    {
        int8_t* out = bufferStartingAddress;
        *out++ = static_cast<int8_t>('"');
        uint64_t runStart = 0;
        for (uint64_t i = 0; i < contentSize; ++i)
        {
            const unsigned char character = static_cast<unsigned char>(data[i]);
            if (character == '"' || character == '\\' || character < 0x20)
            {
                std::memcpy(out, data + runStart, i - runStart);
                out += i - runStart;
                out += writeEscapeSequence(out, data[i]);
                runStart = i + 1;
            }
        }
        std::memcpy(out, data + runStart, contentSize - runStart);
        out += contentSize - runStart;
        *out++ = static_cast<int8_t>('"');
        return static_cast<uint64_t>(out - bufferStartingAddress);
    }
    /// Slow path: the value is near the main-buffer boundary (or children already exist).
    std::string escaped;
    escaped.reserve(contentSize + 2);
    escaped.push_back('"');
    for (uint64_t i = 0; i < contentSize; ++i)
    {
        appendJsonEscaped(escaped, data[i]);
    }
    escaped.push_back('"');
    return writeBytesToBuffer(escaped.data(), escaped.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// Write a single CHAR as a JSON string ("x", escaped if needed).
uint64_t writeJsonChar(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const char content,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    std::string escaped;
    escaped.reserve(8);
    escaped.push_back('"');
    appendJsonEscaped(escaped, content);
    escaped.push_back('"');
    return writeBytesToBuffer(escaped.data(), escaped.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// Write raw pass-through bytes (lazy numeric/bool values: the source text is already a valid
/// JSON number/literal, so copy it verbatim).
NAUTILUS_INLINE uint64_t writeRawBytes(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const int8_t* content,
    const uint64_t contentSize,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeBytesToBuffer(
        reinterpret_cast<const char*>(content), contentSize, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

void writeValue(
    const DataType& fieldType,
    const VarVal& value,
    const nautilus::val<int8_t*>& fieldPointer,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    nautilus::val<uint64_t>& written,
    nautilus::val<uint64_t>& currentRemainingSize,
    const std::string& parserType)
{
    switch (fieldType.type)
    {
        case DataType::Type::BOOLEAN: {
            /// JSON enforces true/false literals, so lazy bools are parsed (never passed through raw:
            /// the CSV source text may be 1/0).
            const auto boolValue = value.getRawValueAs<nautilus::val<bool>>();
            const auto literal = nautilus::select(boolValue, nautilus::val<const char*>{"true"}, nautilus::val<const char*>{"false"});
            const auto literalLength = nautilus::select(boolValue, nautilus::val<uint64_t>{4}, nautilus::val<uint64_t>{5});
            const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                writeBytesToBuffer,
                literal,
                literalLength,
                currentRemainingSize,
                recordBuffer.getReference(),
                bufferProvider,
                fieldPointer + written);
            written += amountWritten;
            currentRemainingSize -= amountWritten;
            break;
        }
        case DataType::Type::CHAR: {
            if (value.isLazyValue())
            {
                const auto lazyValue = value.getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
                const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                    writeJsonStringValue,
                    fieldPointer + written,
                    currentRemainingSize,
                    lazyValue->getContent(),
                    lazyValue->getSize(),
                    recordBuffer.getReference(),
                    bufferProvider);
                written += amountWritten;
                currentRemainingSize -= amountWritten;
            }
            else
            {
                const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                    writeJsonChar,
                    fieldPointer + written,
                    currentRemainingSize,
                    value.getRawValueAs<nautilus::val<char>>(),
                    recordBuffer.getReference(),
                    bufferProvider);
                written += amountWritten;
                currentRemainingSize -= amountWritten;
            }
            break;
        }
        case DataType::Type::VARSIZED: {
            const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
            const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                writeJsonStringValue,
                fieldPointer + written,
                currentRemainingSize,
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
        case DataType::Type::FLOAT64: {
            if (value.isLazyValue())
            {
                /// The source text of a numeric field is already a valid JSON number: copy it raw.
                const auto lazyValue = value.getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
                const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                    writeRawBytes,
                    fieldPointer + written,
                    currentRemainingSize,
                    lazyValue->getContent(),
                    lazyValue->getSize(),
                    recordBuffer.getReference(),
                    bufferProvider);
                written += amountWritten;
                currentRemainingSize -= amountWritten;
            }
            else
            {
                const nautilus::val<uint64_t> amountWritten
                    = formatAndWriteVal(value, fieldPointer + written, currentRemainingSize, recordBuffer, bufferProvider, parserType);
                written += amountWritten;
                currentRemainingSize -= amountWritten;
            }
            break;
        }
        case DataType::Type::UNDEFINED: {
            throw UnknownDataType("JSON-OutputFormatting for type UNDEFINED is not supported.");
        }
    }
}
}

JSONOutputFormatter::JSONOutputFormatter(
    const std::vector<Record::RecordFieldIdentifier>& fieldNames, const OutputFormatterDescriptor& descriptor)
    : OutputFormatter(fieldNames, descriptor)
{
    fieldPrefixes.reserve(fieldNames.size());
    for (size_t fieldIndex = 0; fieldIndex < fieldNames.size(); ++fieldIndex)
    {
        fieldPrefixes.push_back(fmt::format("{}\"{}\":", fieldIndex == 0 ? "{" : ",", jsonEscapeString(fieldNames[fieldIndex])));
    }
}

nautilus::val<uint64_t> JSONOutputFormatter::writeFormattedValue(
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

    /// Pre-value glue (`{"name":` for the first field, `,"name":` otherwise): precomputed at
    /// construction, so this is one constant-length byte copy -- no per-row string allocation.
    const std::string& prefix = fieldPrefixes.at(fieldIndex);
    const nautilus::val<uint64_t> prefixWritten = nautilus::invoke(
        writeBytesToBuffer,
        nautilus::val<const char*>{prefix.c_str()},
        nautilus::val<uint64_t>{prefix.size()},
        currentRemainingSize,
        recordBuffer.getReference(),
        bufferProvider,
        fieldPointer);
    written += prefixWritten;
    currentRemainingSize -= prefixWritten;

    /// Handle NULL values and write value
    if (value.isNullable() && value.isNull())
    {
        const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
            writeBytesToBuffer,
            nautilus::val<const char*>{"null"},
            nautilus::val<uint64_t>{4},
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
            fieldType, value, fieldPointer, recordBuffer, bufferProvider, written, currentRemainingSize, parserTypes.at(fieldType.type));
    }

    /// The record suffix after the last field; interior fields need no trailing delimiter (the next
    /// field's prefix carries the comma). The field index is host-known, so this is compile-time
    /// control flow -- no runtime select.
    if (static_cast<uint64_t>(fieldIndex) == fieldNames.size() - 1)
    {
        written += nautilus::invoke(
            writeBytesToBuffer,
            nautilus::val<const char*>{"}\n"},
            nautilus::val<uint64_t>{2},
            currentRemainingSize,
            recordBuffer.getReference(),
            bufferProvider,
            fieldPointer + written);
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
    return std::make_unique<JSONOutputFormatter>(std::move(args.fieldNames), std::move(args.descriptor));
}
}
