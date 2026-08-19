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

#include <SinksParsing/HL7Format.hpp>

#include <algorithm>
#include <bit>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <string>
#include <utility>
#include <vector>
#include <zmij.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <SinksParsing/Format.hpp>
#include <fmt/format.h>

#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
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

HL7Format::Preset HL7Format::hl7Preset()
{
    /// The compiled HL7OutputFormatter's config DEFAULTS, pre-concatenated the same way
    /// (prologue = frame_start + message_header + segment_delimiter + segment_name +
    /// field_delimiter; suffix = segment_delimiter + frame_end).
    return Preset{
        .prologue = "\x0B"
                    "MSH|^~\\&|NES|NES|BENCH|BENCH|20260101000000||ORU^R01|1|P|2.5"
                    "\r"
                    "SEG"
                    "|",
        .fieldDelimiter = "|",
        .suffix = "\r"
                  "\x1C"
                  "\r"};
}

HL7Format::Preset HL7Format::xmlPreset()
{
    /// The packed-XML config of the same emission model (see formatter/XML/XMLOutput.test):
    /// frame_start `<rec>` + segment_name `<f>` + field_delimiter `</f><f>` -> the prologue opens
    /// the well-formedness arity-pad element; suffix closes the last element and the record.
    return Preset{.prologue = "<rec><f></f><f>", .fieldDelimiter = "</f><f>", .suffix = "</f></rec>\n"};
}

HL7Format::HL7Format(const Schema& pSchema, Preset pPreset) : Format(pSchema), preset(std::move(pPreset))
{
    PRECONDITION(schema.getNumberOfFields() != 0, "Formatter expected a non-empty schema");
    size_t offset = 0;
    for (const auto& field : schema.getFields())
    {
        const auto physicalType = field.dataType;
        formattingContext.offsets.push_back(offset);
        formattingContext.sizesWithNull.push_back(physicalType.getSizeInBytesWithNull());
        offset += physicalType.getSizeInBytesWithNull();
        formattingContext.physicalTypes.emplace_back(physicalType);
    }
    formattingContext.schemaSizeInBytes = schema.getSizeOfSchemaInBytes();
}

std::string HL7Format::getFormattedBuffer(const TupleBuffer& inputBuffer) const
{
    thread_local std::vector<char> scratch;
    const auto bytes = formatToBuffer(inputBuffer, scratch);
    return std::string(scratch.data(), bytes);
}

size_t HL7Format::formatToBuffer(const TupleBuffer& tbuffer, std::vector<char>& out) const
{
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
    /// Room for the longest glue + the widest single value (a double's shortest form, >= any int64).
    const size_t fieldSlack
        = std::max(preset.prologue.size(), preset.fieldDelimiter.size() + preset.suffix.size()) + zmij::double_buffer_size + 24;

    for (size_t i = 0; i < numberOfTuples; i++)
    {
        auto tuple = buffer.subspan(i * fc.schemaSizeInBytes, fc.schemaSizeInBytes);
        for (size_t index = 0; index < fc.offsets.size(); ++index)
        {
            ensure(fieldSlack);
            if (index == 0)
            {
                std::memcpy(out.data() + off, preset.prologue.data(), preset.prologue.size());
                off += preset.prologue.size();
            }
            else
            {
                std::memcpy(out.data() + off, preset.fieldDelimiter.data(), preset.fieldDelimiter.size());
                off += preset.fieldDelimiter.size();
            }
            const auto& physicalType = fc.physicalTypes[index];
            auto fieldValueStart = tuple.subspan(fc.offsets[index], fc.sizesWithNull[index]);
            if (physicalType.nullable)
            {
                const bool isNull = static_cast<bool>(std::to_integer<int>(fieldValueStart[0]));
                fieldValueStart = fieldValueStart.subspan(1);
                if (isNull)
                {
                    /// NULL emits zero bytes: HL7's empty-field convention (`||`), like the
                    /// compiled formatter.
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
                    /// Forwarded verbatim, like the compiled formatter (no HL7 escape sequences; a
                    /// value containing a structural byte re-parses with the wrong arity and the
                    /// input indexer rejects it loudly).
                    ensure(varSizedData.size());
                    std::memcpy(out.data() + off, varSizedData.data(), varSizedData.size());
                    off += varSizedData.size();
                    break;
                }
                case DataType::Type::UNDEFINED:
                    throw CannotFormatMalformedStringValue("Cannot format undefined type in legacy HL7 output.");
            }
        }
        ensure(preset.suffix.size());
        std::memcpy(out.data() + off, preset.suffix.data(), preset.suffix.size());
        off += preset.suffix.size();
    }
    return off;
}

std::ostream& operator<<(std::ostream& out, const HL7Format& format)
{
    return out << fmt::format("HL7Format(Schema: {})", format.schema);
}

}
