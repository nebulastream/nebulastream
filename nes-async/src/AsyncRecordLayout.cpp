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

#include <Async/AsyncRecordLayout.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

/// Reads a trivially copyable value out of the record's field slot.
template <typename T>
T readAs(const std::span<const std::byte> slot)
{
    INVARIANT(slot.size() >= sizeof(T), "Field slot of {}B is too small for a {}B value", slot.size(), sizeof(T));
    T value{};
    std::memcpy(&value, slot.data(), sizeof(T));
    return value;
}

}

AsyncRecordLayout::AsyncRecordLayout(const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    size_t offset = 0;
    for (const auto& field : schema)
    {
        const auto& type = field.getDataType();
        if (type.nullable)
        {
            throw NotImplemented(
                "Asynchronous operators do not support nullable fields yet, but field '{}' is nullable", field.getFullyQualifiedName());
        }

        fields.emplace_back(
            Field{
                .name = static_cast<const Identifier&>(field.getFullyQualifiedName()).asCanonicalString(),
                .type = type,
                .offset = offset});
        offset += type.getSizeInBytesWithNull();
    }

    tupleSizeInBytes = offset;
    INVARIANT(tupleSizeInBytes > 0, "A record layout needs at least one field");
}

uint64_t AsyncRecordLayout::capacity(const uint64_t bufferSize) const
{
    return bufferSize / tupleSizeInBytes;
}

std::optional<size_t> AsyncRecordLayout::indexOf(const std::string_view fieldName) const
{
    const auto it = std::ranges::find_if(fields, [&](const Field& field) { return field.name == fieldName; });
    if (it == fields.end())
    {
        return std::nullopt;
    }
    return static_cast<size_t>(std::distance(fields.begin(), it));
}

std::optional<size_t> AsyncRecordLayout::indexOfColumn(const std::string_view columnName) const
{
    const auto identifier = Identifier::tryParse(std::string{columnName});
    if (!identifier.has_value())
    {
        return std::nullopt;
    }
    return indexOf(identifier->asCanonicalString());
}

const std::string& AsyncRecordLayout::nameOf(const size_t fieldIndex) const
{
    PRECONDITION(fieldIndex < fields.size(), "Field index {} is out of range ({} fields)", fieldIndex, fields.size());
    return fields[fieldIndex].name;
}

const DataType& AsyncRecordLayout::typeOf(const size_t fieldIndex) const
{
    PRECONDITION(fieldIndex < fields.size(), "Field index {} is out of range ({} fields)", fieldIndex, fields.size());
    return fields[fieldIndex].type;
}

size_t AsyncRecordLayout::offsetOf(const size_t fieldIndex) const
{
    PRECONDITION(fieldIndex < fields.size(), "Field index {} is out of range ({} fields)", fieldIndex, fields.size());
    return fields[fieldIndex].offset;
}

AsyncRecordView::AsyncRecordView(const AsyncRecordLayout& layout, const TupleBuffer& buffer, const uint64_t recordIndex)
    : layout(&layout), buffer(&buffer), recordIndex(recordIndex)
{
}

std::span<const std::byte> AsyncRecordView::readRaw(const size_t fieldIndex) const
{
    const auto fieldSize = layout->typeOf(fieldIndex).getSizeInBytesWithNull();
    const auto slotStart = (recordIndex * layout->tupleSize()) + layout->offsetOf(fieldIndex);
    const auto memory = buffer->getAvailableMemoryArea<const std::byte>();
    INVARIANT(slotStart + fieldSize <= memory.size(), "Record {} does not fit into the buffer", recordIndex);
    return memory.subspan(slotStart, fieldSize);
}

std::string_view AsyncRecordView::readText(const size_t fieldIndex) const
{
    PRECONDITION(
        layout->typeOf(fieldIndex).isType(DataType::Type::VARSIZED),
        "readText expects a VARSIZED field, but '{}' is not",
        layout->nameOf(fieldIndex));

    /// The slot holds a VariableSizedAccess verbatim; it points at a child buffer.
    const auto access = readAs<VariableSizedAccess>(readRaw(fieldIndex));
    const auto content = TupleBufferRef::loadAssociatedVarSizedValue(*buffer, access);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) bytes to text
    return {reinterpret_cast<const char*>(content.data()), content.size()};
}

std::string AsyncRecordView::readAsText(const size_t fieldIndex) const
{
    const auto& type = layout->typeOf(fieldIndex);
    const auto slot = readRaw(fieldIndex);

    switch (type.type)
    {
        case DataType::Type::VARSIZED:
            return std::string{readText(fieldIndex)};
        case DataType::Type::BOOLEAN:
            return readAs<bool>(slot) ? "true" : "false";
        case DataType::Type::CHAR:
            return std::string(1, readAs<char>(slot));
        case DataType::Type::INT8:
            return fmt::format("{}", readAs<int8_t>(slot));
        case DataType::Type::INT16:
            return fmt::format("{}", readAs<int16_t>(slot));
        case DataType::Type::INT32:
            return fmt::format("{}", readAs<int32_t>(slot));
        case DataType::Type::INT64:
            return fmt::format("{}", readAs<int64_t>(slot));
        case DataType::Type::UINT8:
            return fmt::format("{}", readAs<uint8_t>(slot));
        case DataType::Type::UINT16:
            return fmt::format("{}", readAs<uint16_t>(slot));
        case DataType::Type::UINT32:
            return fmt::format("{}", readAs<uint32_t>(slot));
        case DataType::Type::UINT64:
            return fmt::format("{}", readAs<uint64_t>(slot));
        case DataType::Type::FLOAT32:
            return fmt::format("{}", readAs<float>(slot));
        case DataType::Type::FLOAT64:
            return fmt::format("{}", readAs<double>(slot));
        case DataType::Type::UNDEFINED:
            break;
    }
    throw NotImplemented("Field '{}' has a type that cannot be rendered as text", layout->nameOf(fieldIndex));
}

AsyncRecordWriter::AsyncRecordWriter(
    const AsyncRecordLayout& layout, TupleBuffer& buffer, AbstractBufferProvider& bufferProvider, const uint64_t recordIndex)
    : layout(&layout), buffer(&buffer), bufferProvider(&bufferProvider), recordIndex(recordIndex)
{
}

void AsyncRecordWriter::writeRaw(const size_t fieldIndex, const std::span<const std::byte> value)
{
    const auto fieldSize = layout->typeOf(fieldIndex).getSizeInBytesWithNull();
    PRECONDITION(value.size() == fieldSize, "Field '{}' expects {}B but got {}B", layout->nameOf(fieldIndex), fieldSize, value.size());

    const auto slotStart = (recordIndex * layout->tupleSize()) + layout->offsetOf(fieldIndex);
    const auto memory = buffer->getAvailableMemoryArea<std::byte>();
    INVARIANT(slotStart + fieldSize <= memory.size(), "Record {} does not fit into the buffer", recordIndex);
    std::memcpy(memory.data() + slotStart, value.data(), fieldSize);
}

void AsyncRecordWriter::writeText(const size_t fieldIndex, const std::string_view value)
{
    PRECONDITION(
        layout->typeOf(fieldIndex).isType(DataType::Type::VARSIZED),
        "writeText expects a VARSIZED field, but '{}' is not",
        layout->nameOf(fieldIndex));

    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) text to bytes
    const std::span content{reinterpret_cast<const std::byte*>(value.data()), value.size()};
    const auto access = TupleBufferRef::writeVarSized(*buffer, *bufferProvider, content);

    const std::span accessBytes{
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) the slot holds the access verbatim
        reinterpret_cast<const std::byte*>(&access),
        sizeof(VariableSizedAccess)};
    writeRaw(fieldIndex, accessBytes);
}

namespace
{

/// Parses `text` as T and stores it in the slot, or throws when it does not parse.
template <typename T>
void writeParsed(AsyncRecordWriter& writer, const size_t fieldIndex, const std::string_view text, const std::string& fieldName)
{
    const auto parsed = from_chars<T>(text);
    if (!parsed.has_value())
    {
        throw CannotFormatMalformedStringValue("Cannot store '{}' in field '{}': it does not parse as the declared type", text, fieldName);
    }
    const T value = parsed.value();
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    writer.writeRaw(fieldIndex, std::span{reinterpret_cast<const std::byte*>(&value), sizeof(value)});
}

}

void AsyncRecordWriter::writeAsText(const size_t fieldIndex, const std::string_view value)
{
    const auto& type = layout->typeOf(fieldIndex);
    const auto& name = layout->nameOf(fieldIndex);

    switch (type.type)
    {
        case DataType::Type::VARSIZED:
            writeText(fieldIndex, value);
            return;
        case DataType::Type::BOOLEAN:
            writeParsed<bool>(*this, fieldIndex, value, name);
            return;
        case DataType::Type::CHAR:
            writeParsed<char>(*this, fieldIndex, value, name);
            return;
        case DataType::Type::INT8:
            writeParsed<int8_t>(*this, fieldIndex, value, name);
            return;
        case DataType::Type::INT16:
            writeParsed<int16_t>(*this, fieldIndex, value, name);
            return;
        case DataType::Type::INT32:
            writeParsed<int32_t>(*this, fieldIndex, value, name);
            return;
        case DataType::Type::INT64:
            writeParsed<int64_t>(*this, fieldIndex, value, name);
            return;
        case DataType::Type::UINT8:
            writeParsed<uint8_t>(*this, fieldIndex, value, name);
            return;
        case DataType::Type::UINT16:
            writeParsed<uint16_t>(*this, fieldIndex, value, name);
            return;
        case DataType::Type::UINT32:
            writeParsed<uint32_t>(*this, fieldIndex, value, name);
            return;
        case DataType::Type::UINT64:
            writeParsed<uint64_t>(*this, fieldIndex, value, name);
            return;
        case DataType::Type::FLOAT32:
            writeParsed<float>(*this, fieldIndex, value, name);
            return;
        case DataType::Type::FLOAT64:
            writeParsed<double>(*this, fieldIndex, value, name);
            return;
        case DataType::Type::UNDEFINED:
            break;
    }
    throw NotImplemented("Field '{}' has a type that cannot be written from text", name);
}

void AsyncRecordWriter::copyMatchingFields(const AsyncRecordView& source)
{
    const auto& sourceLayout = source.getLayout();
    for (size_t sourceIndex = 0; sourceIndex < sourceLayout.fieldCount(); ++sourceIndex)
    {
        const auto targetIndex = layout->indexOf(sourceLayout.nameOf(sourceIndex));
        if (!targetIndex.has_value())
        {
            continue;
        }

        if (sourceLayout.typeOf(sourceIndex).isType(DataType::Type::VARSIZED))
        {
            /// Copied into this buffer's own child buffer, so the result does not depend on
            /// the lifetime of the source buffer.
            writeText(*targetIndex, source.readText(sourceIndex));
        }
        else
        {
            writeRaw(*targetIndex, source.readRaw(sourceIndex));
        }
    }
}

}
