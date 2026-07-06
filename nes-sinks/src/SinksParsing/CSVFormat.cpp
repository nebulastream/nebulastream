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

#include <SinksParsing/CSVFormat.hpp>

#include <algorithm>
#include <bit>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <span>
#include <string>
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
CSVFormat::CSVFormat(const Schema& schema) : CSVFormat(schema, false)
{
}

CSVFormat::CSVFormat(const Schema& pSchema, const bool escapeStrings) : Format(pSchema), escapeStrings(escapeStrings)
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

std::string CSVFormat::getFormattedBuffer(const TupleBuffer& inputBuffer) const
{
    /// Convenience API: delegate to the fast direct-write path, materialize a string only here.
    thread_local std::vector<char> scratch;
    const auto bytes = formatToBuffer(inputBuffer, scratch);
    return std::string(scratch.data(), bytes);
}

/// Write one fixed-size field DIRECTLY into `out` at `off` (capacity already ensured by the caller), using the
/// fast serializers: std::to_chars for integers, zmij::write (Ryu/Schubfach shortest == ZMIJF64) for floats.
/// No per-field std::string, no append -- the value bytes go straight into the output buffer.
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

size_t CSVFormat::formatToBuffer(const TupleBuffer& tbuffer, std::vector<char>& out) const
{
    const auto& fc = formattingContext;
    const auto numberOfTuples = tbuffer.getNumberOfTuples();
    const auto buffer = tbuffer.getAvailableMemoryArea().subspan(0, numberOfTuples * fc.schemaSizeInBytes);

    /// `out` is a reused (thread_local) buffer we write into by raw pointer. `ensure(n)` guarantees n free
    /// bytes at `off`; it grows geometrically and NEVER shrinks, so after warmup it never reallocates and
    /// never re-zeroes (unlike a fresh std::string per call). Return value is the exact byte length.
    size_t off = 0;
    const auto ensure = [&out, &off](const size_t need)
    {
        if (out.size() < off + need)
        {
            out.resize(std::max(out.size() * 2, off + need + 64));
        }
    };
    /// Room for the leading ',' + the widest single value (a double's shortest form, also >= any int64).
    constexpr size_t fieldSlack = zmij::double_buffer_size + 24;

    for (size_t i = 0; i < numberOfTuples; i++)
    {
        auto tuple = buffer.subspan(i * fc.schemaSizeInBytes, fc.schemaSizeInBytes);
        for (size_t index = 0; index < fc.offsets.size(); ++index)
        {
            ensure(fieldSlack);
            if (index != 0)
            {
                out[off++] = ',';
            }
            const auto& physicalType = fc.physicalTypes[index];
            auto fieldValueStart = tuple.subspan(fc.offsets[index], fc.sizesWithNull[index]);
            if (physicalType.nullable)
            {
                /// Convert byte to bool: true if byte is non-zero, false otherwise
                const bool isNull = static_cast<bool>(std::to_integer<int>(fieldValueStart[0]));
                fieldValueStart = fieldValueStart.subspan(1);
                if (isNull)
                {
                    /// We need to write null, as otherwise, we can not detect a single null in our output
                    std::memcpy(out.data() + off, "NULL", 4);
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
                    ensure(varSizedData.size() + 2);
                    if (escapeStrings)
                    {
                        out[off++] = '"';
                    }
                    std::memcpy(out.data() + off, varSizedData.data(), varSizedData.size());
                    off += varSizedData.size();
                    if (escapeStrings)
                    {
                        out[off++] = '"';
                    }
                    break;
                }
                case DataType::Type::UNDEFINED:
                    throw CannotFormatMalformedStringValue("Cannot format undefined type in legacy CSV output.");
            }
        }
        ensure(1);
        out[off++] = '\n';
    }
    return off;
}

std::ostream& operator<<(std::ostream& out, const CSVFormat& format)
{
    return out << fmt::format("CSVFormat(Schema: {})", format.schema);
}

}
