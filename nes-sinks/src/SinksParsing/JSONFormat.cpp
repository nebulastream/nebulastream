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

#include <SinksParsing/JSONFormat.hpp>

#include <algorithm>
#include <bit>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ostream>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>
#include <zmij.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <SinksParsing/Format.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Resolve the output codec once (at sink construction) from the NES_OUTPUT_CODEC env var.
/// "original"/"legacy" -> the genuine pre-optimization stringstream writer; anything else -> Fast.
OutputCodec outputCodecFromEnv()
{
    const char* const raw = std::getenv("NES_OUTPUT_CODEC");
    if (raw != nullptr)
    {
        const std::string_view value{raw};
        if (value == "original" || value == "legacy" || value == "Original" || value == "ORIGINAL")
        {
            return OutputCodec::Original;
        }
    }
    return OutputCodec::Fast;
}

/// Construction-time JSON escaping for field names (RFC 8259: `"`, `\`, control bytes < 0x20).
/// Mirrors the compiled JSONOutputFormatter's jsonEscapeString so the per-field glue is
/// byte-identical.
std::string jsonEscapeString(const std::string_view raw)
{
    std::string escaped;
    escaped.reserve(raw.size());
    for (const char character : raw)
    {
        switch (character)
        {
            case '"':
                escaped += "\\\"";
                break;
            case '\\':
                escaped += "\\\\";
                break;
            case '\b':
                escaped += "\\b";
                break;
            case '\f':
                escaped += "\\f";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(character) < 0x20)
                {
                    constexpr std::string_view hexDigits = "0123456789abcdef";
                    escaped += "\\u00";
                    escaped += hexDigits[(static_cast<unsigned char>(character) >> 4U) & 0xFU];
                    escaped += hexDigits[static_cast<unsigned char>(character) & 0xFU];
                }
                else
                {
                    escaped += character;
                }
        }
    }
    return escaped;
}

bool needsJsonEscape(const char character)
{
    return character == '"' || character == '\\' || static_cast<unsigned char>(character) < 0x20;
}

/// Write one escapable byte at `off` (caller ensured >= 6 free bytes); returns the new offset.
size_t writeJsonEscapedByte(std::vector<char>& out, size_t off, const char character)
{
    const auto putTwo = [&](const char second)
    {
        out[off++] = '\\';
        out[off++] = second;
    };
    switch (character)
    {
        case '"':
            putTwo('"');
            break;
        case '\\':
            putTwo('\\');
            break;
        case '\b':
            putTwo('b');
            break;
        case '\f':
            putTwo('f');
            break;
        case '\n':
            putTwo('n');
            break;
        case '\r':
            putTwo('r');
            break;
        case '\t':
            putTwo('t');
            break;
        default: {
            constexpr std::string_view hexDigits = "0123456789abcdef";
            out[off++] = '\\';
            out[off++] = 'u';
            out[off++] = '0';
            out[off++] = '0';
            out[off++] = hexDigits[(static_cast<unsigned char>(character) >> 4U) & 0xFU];
            out[off++] = hexDigits[static_cast<unsigned char>(character) & 0xFU];
        }
    }
    return off;
}

template <typename T>
void writeIntDirect(std::vector<char>& out, size_t& off, const void* data)
{
    T value;
    std::memcpy(&value, data, sizeof(T));
    const auto [ptr, ec] = std::to_chars(out.data() + off, out.data() + out.size(), value);
    off = static_cast<size_t>(ptr - out.data());
}

template <typename T>
void writeFloatDirect(std::vector<char>& out, size_t& off, const void* data)
{
    T value;
    std::memcpy(&value, data, sizeof(T));
    off += zmij::write(out.data() + off, out.size() - off, value);
}
}

JSONFormat::JSONFormat(const Schema& pSchema) : Format(pSchema), codec(outputCodecFromEnv())
{
    PRECONDITION(schema.getNumberOfFields() != 0, "Formatter expected a non-empty schema");
    size_t offset = 0;
    size_t index = 0;
    for (const auto& field : schema.getFields())
    {
        const auto physicalType = field.dataType;
        formattingContext.offsets.push_back(offset);
        formattingContext.sizesWithNull.push_back(physicalType.getSizeInBytesWithNull());
        offset += physicalType.getSizeInBytesWithNull();
        formattingContext.physicalTypes.emplace_back(physicalType);
        /// Same glue as the compiled formatter's fieldPrefixes: `{"NAME":` for field 0, `,"NAME":` after.
        formattingContext.prefixes.push_back(fmt::format("{}\"{}\":", index == 0 ? "{" : ",", jsonEscapeString(field.name)));
        formattingContext.names.emplace_back(field.name);
        ++index;
    }
    formattingContext.schemaSizeInBytes = schema.getSizeOfSchemaInBytes();
}

std::string JSONFormat::getFormattedBuffer(const TupleBuffer& inputBuffer) const
{
    if (codec == OutputCodec::Original)
    {
        return formatOriginalStringstream(inputBuffer);
    }
    thread_local std::vector<char> scratch;
    const auto bytes = formatToBuffer(inputBuffer, scratch);
    return std::string(scratch.data(), bytes);
}

size_t JSONFormat::formatToBuffer(const TupleBuffer& tbuffer, std::vector<char>& out) const
{
    if (codec == OutputCodec::Original)
    {
        /// The genuine pre-optimization naive output (recovered from 5f7bb111d1): the whole-buffer string
        /// via the stringstream path, copied into the caller buffer. This is the naive baseline -- it
        /// measures the full output-serializer rewrite (codec + direct-buffer structure) as one number;
        /// the fast path below is the optimized target.
        const std::string formatted = formatOriginalStringstream(tbuffer);
        if (out.size() < formatted.size())
        {
            out.resize(formatted.size());
        }
        std::memcpy(out.data(), formatted.data(), formatted.size());
        return formatted.size();
    }
    const auto& fc = formattingContext;
    const auto numberOfTuples = tbuffer.getNumberOfTuples();
    const auto buffer = tbuffer.getAvailableMemoryArea().subspan(0, numberOfTuples * fc.schemaSizeInBytes);

    size_t off = 0;
    const auto ensure = [&out, &off](const size_t need)
    {
        if (out.size() < off + need)
        {
            out.resize(std::max(out.size() * 2, off + need + 64));
        }
    };
    /// Room for the widest single value (a double's shortest form, also >= any int64) + `null`.
    constexpr size_t valueSlack = zmij::double_buffer_size + 24;

    for (size_t i = 0; i < numberOfTuples; i++)
    {
        auto tuple = buffer.subspan(i * fc.schemaSizeInBytes, fc.schemaSizeInBytes);
        for (size_t index = 0; index < fc.offsets.size(); ++index)
        {
            const auto& prefix = fc.prefixes[index];
            ensure(prefix.size() + valueSlack);
            std::memcpy(out.data() + off, prefix.data(), prefix.size());
            off += prefix.size();
            const auto& physicalType = fc.physicalTypes[index];
            auto fieldValueStart = tuple.subspan(fc.offsets[index], fc.sizesWithNull[index]);
            if (physicalType.nullable)
            {
                const bool isNull = static_cast<bool>(std::to_integer<int>(fieldValueStart[0]));
                fieldValueStart = fieldValueStart.subspan(1);
                if (isNull)
                {
                    /// Lowercase `null`, like the compiled formatter.
                    std::memcpy(out.data() + off, "null", 4);
                    off += 4;
                    continue;
                }
            }
            const auto* data = fieldValueStart.data();
            switch (physicalType.type)
            {
                case DataType::Type::INT8:
                    writeIntDirect<int8_t>(out, off, data);
                    break;
                case DataType::Type::UINT8:
                    writeIntDirect<uint8_t>(out, off, data);
                    break;
                case DataType::Type::INT16:
                    writeIntDirect<int16_t>(out, off, data);
                    break;
                case DataType::Type::UINT16:
                    writeIntDirect<uint16_t>(out, off, data);
                    break;
                case DataType::Type::INT32:
                    writeIntDirect<int32_t>(out, off, data);
                    break;
                case DataType::Type::UINT32:
                    writeIntDirect<uint32_t>(out, off, data);
                    break;
                case DataType::Type::INT64:
                    writeIntDirect<int64_t>(out, off, data);
                    break;
                case DataType::Type::UINT64:
                    writeIntDirect<uint64_t>(out, off, data);
                    break;
                case DataType::Type::FLOAT32:
                    writeFloatDirect<float>(out, off, data);
                    break;
                case DataType::Type::FLOAT64:
                    writeFloatDirect<double>(out, off, data);
                    break;
                case DataType::Type::BOOLEAN:
                    /// '1'/'0' like the compiled path's materialized-bool codec (writeIntegralValue(int)).
                    out[off++] = (std::to_integer<int>(*data) != 0) ? '1' : '0';
                    break;
                case DataType::Type::CHAR:
                    out[off++] = static_cast<char>(std::to_integer<unsigned char>(*data));
                    break;
                case DataType::Type::VARSIZED: {
                    const auto base = fc.offsets[index] + physicalType.nullable;
                    const auto* indexPtr = std::bit_cast<const uint32_t*>(&tuple[base + offsetof(VariableSizedAccess, index)]);
                    const auto* offsetPtr = std::bit_cast<const uint32_t*>(&tuple[base + offsetof(VariableSizedAccess, offset)]);
                    const auto* sizePtr = std::bit_cast<const uint64_t*>(&tuple[base + offsetof(VariableSizedAccess, size)]);
                    const VariableSizedAccess variableSizedAccess{
                        VariableSizedAccess::Index(*indexPtr),
                        VariableSizedAccess::Offset(*offsetPtr),
                        VariableSizedAccess::Size(*sizePtr)};
                    const auto varSizedData = readVarSizedDataAsString(tbuffer, variableSizedAccess);
                    /// Quoted + RFC 8259 escaped. Fast path: a single scan finds no escapable byte ->
                    /// one memcpy. Slow path reserves the worst case (6x, \u00XX) and escapes per byte.
                    const bool clean = std::ranges::none_of(varSizedData, needsJsonEscape);
                    if (clean)
                    {
                        ensure(varSizedData.size() + 2);
                        out[off++] = '"';
                        std::memcpy(out.data() + off, varSizedData.data(), varSizedData.size());
                        off += varSizedData.size();
                        out[off++] = '"';
                    }
                    else
                    {
                        ensure((varSizedData.size() * 6) + 2);
                        out[off++] = '"';
                        for (const char character : varSizedData)
                        {
                            if (needsJsonEscape(character))
                            {
                                off = writeJsonEscapedByte(out, off, character);
                            }
                            else
                            {
                                out[off++] = character;
                            }
                        }
                        out[off++] = '"';
                    }
                    break;
                }
                case DataType::Type::UNDEFINED:
                    throw CannotFormatMalformedStringValue("Cannot format undefined type in legacy JSON output.");
            }
        }
        ensure(2);
        out[off++] = '}';
        out[off++] = '\n';
    }
    return off;
}

std::string JSONFormat::formatOriginalStringstream(const TupleBuffer& tbuffer) const
{
    /// Verbatim pre-optimization writer (recovered from commit 5f7bb111d1): a std::stringstream, a per-field
    /// std::string built with fmt::format, and DataType::formattedBytesToString for the value codec. Its
    /// conventions differ from the Fast path (uppercase NULL, unescaped strings, fmt "{:.6f}" float format) --
    /// intentional: this IS the original naive serializer, structure and codec together.
    const auto& fc = formattingContext;
    std::stringstream ss;
    const auto numberOfTuples = tbuffer.getNumberOfTuples();
    const auto buffer = tbuffer.getAvailableMemoryArea().subspan(0, numberOfTuples * fc.schemaSizeInBytes);
    for (size_t i = 0; i < numberOfTuples; i++)
    {
        auto tuple = buffer.subspan(i * fc.schemaSizeInBytes, fc.schemaSizeInBytes);
        auto fields
            = std::views::iota(static_cast<size_t>(0), fc.offsets.size())
            | std::views::transform(
                  [&fc, &tuple, &tbuffer](const auto& index) -> std::string
                  {
                      auto type = fc.physicalTypes[index];
                      auto fieldValueStart = tuple.subspan(fc.offsets[index]);
                      if (type.nullable)
                      {
                          const bool isNull = static_cast<bool>(std::to_integer<int>(fieldValueStart[0]));
                          fieldValueStart = fieldValueStart.subspan(1);
                          if (isNull)
                          {
                              return "NULL";
                          }
                      }
                      if (type.type == DataType::Type::VARSIZED)
                      {
                          const auto base = fc.offsets[index] + type.nullable;
                          const auto* indexPtr = std::bit_cast<const uint32_t*>(&tuple[base + offsetof(VariableSizedAccess, index)]);
                          const auto* offsetPtr = std::bit_cast<const uint32_t*>(&tuple[base + offsetof(VariableSizedAccess, offset)]);
                          const auto* sizePtr = std::bit_cast<const uint64_t*>(&tuple[base + offsetof(VariableSizedAccess, size)]);
                          const VariableSizedAccess variableSizedAccess{
                              VariableSizedAccess::Index(*indexPtr),
                              VariableSizedAccess::Offset(*offsetPtr),
                              VariableSizedAccess::Size(*sizePtr)};
                          const auto varSizedData = readVarSizedDataAsString(tbuffer, variableSizedAccess);
                          return fmt::format(R"("{}":"{}")", fc.names.at(index), varSizedData);
                      }
                      return fmt::format("\"{}\":{}", fc.names.at(index), type.formattedBytesToString(fieldValueStart.data()));
                  });
        ss << fmt::format("{{{}}}\n", fmt::join(fields, ","));
    }
    return ss.str();
}

std::ostream& operator<<(std::ostream& out, const JSONFormat& format)
{
    return out << fmt::format("JSONFormat(Schema: {})", format.schema);
}

}
