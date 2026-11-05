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

#include <Interface/BufferRef/LowerSchemaProvider.hpp>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Interface/BufferRef/ColumnTupleBufferRef.hpp>
#include <Interface/BufferRef/OutputFormatterBufferRef.hpp>
#include <Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <OutputFormatters/OutputFormatterProvider.hpp>
#include <Schema/SchemaFwd.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

std::shared_ptr<TupleBufferRef> LowerSchemaProvider::lowerSchemaWithOutputFormat(
    const uint64_t bufferSize, const Schema<QualifiedUnboundField, Ordered>& schema, const OutputFormatterDescriptor& descriptor)
{
    std::vector<OutputFormatterBufferRef::Field> fields;
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    fields.reserve(std::ranges::size(schema));
    fieldNames.reserve(std::ranges::size(schema));
    for (const auto& field : schema)
    {
        fields.emplace_back(field.getFullyQualifiedName(), field.getDataType());
        fieldNames.emplace_back(field.getFullyQualifiedName());
    }

    /// Create an output formatter instance by calling the registry
    const std::shared_ptr<OutputFormatter> outputFormatter
        = OutputFormatterProvider::provideOutputFormatter(descriptor.getOutputFormatterType().asCanonicalString(), fieldNames, descriptor);

    return std::make_shared<OutputFormatterBufferRef>(OutputFormatterBufferRef{std::move(fields), outputFormatter, bufferSize});
}

std::shared_ptr<TupleBufferRef> LowerSchemaProvider::lowerSchema(
    const uint64_t bufferSize, const Schema<QualifiedUnboundField, Ordered>& schema, const MemoryLayoutType layoutType)
{
    PRECONDITION(!std::ranges::empty(schema), "We can not lower an empty schema!");

    /// For now, we assume that the fields lie in the exact same order as in the Schema. Later on, we can have a separate optimizer phase
    /// that can change the order, alignment or even the datatype implementation, e.g., u32 instead of u8.
    switch (layoutType)
    {
        case MemoryLayoutType::ARROW_LAYOUT:
            return lowerSchemaToArrow(bufferSize, schema);
        case MemoryLayoutType::ROW_LAYOUT: {
            std::vector<RowTupleBufferRef::Field> fields;
            fields.reserve(std::ranges::size(schema));
            uint64_t fieldOffset = 0;
            for (const auto& field : schema)
            {
                fields.emplace_back(field.getFullyQualifiedName(), field.getDataType(), fieldOffset);
                fieldOffset += field.getDataType().getSizeInBytesWithNull();
            }
            const auto tupleSize = std::accumulate(
                fields.begin(),
                fields.end(),
                0UL,
                [](auto size, const RowTupleBufferRef::Field& field) { return size + field.type.getSizeInBytesWithNull(); });
            INVARIANT(tupleSize > 0, "Tuplesize must be larger than 0B");
            return std::make_shared<RowTupleBufferRef>(RowTupleBufferRef{std::move(fields), tupleSize, bufferSize});
        }

        case MemoryLayoutType::COLUMNAR_LAYOUT: {
            const auto tupleSize = std::accumulate(
                schema.begin(),
                schema.end(),
                0UL,
                [](auto size, const QualifiedUnboundField& field) { return size + field.getDataType().getSizeInBytesWithNull(); });
            INVARIANT(tupleSize > 0, "Tuplesize must be larger than 0B");

            const uint64_t capacity = bufferSize / tupleSize;
            std::vector<ColumnTupleBufferRef::Field> fields;
            fields.reserve(std::ranges::size(schema));
            uint64_t columnOffset = 0;
            for (const auto& field : schema)
            {
                fields.emplace_back(
                    field.getFullyQualifiedName(), field.getDataType(), field.getDataType().getSizeInBytesWithNull(), columnOffset);
                columnOffset += (field.getDataType().getSizeInBytesWithNull() * capacity);
            }

            return std::make_shared<ColumnTupleBufferRef>(ColumnTupleBufferRef{std::move(fields), tupleSize, bufferSize});
        }
    }
    std::unreachable();
}

std::shared_ptr<TupleBufferRef>
LowerSchemaProvider::lowerSchemaToArrow(const uint64_t bufferSize, const Schema<QualifiedUnboundField, Ordered>& schema)
{
    PRECONDITION(!std::ranges::empty(schema), "We can not lower an empty schema!");

    const auto align8 = [](const uint64_t value) { return (value + 7) & ~uint64_t{7}; };
    const auto bitmapSize = [&](const uint64_t capacity) { return align8((capacity + 7) / 8); };
    /// Bytes per record in the data region while records are written: a VARSIZED value is a pointer and a size until the buffer is
    /// finalized, which leaves room for the capacity + 1 int32 offsets of Arrow binary.
    constexpr uint64_t pendingVarSizedSize = 16;
    /// Bytes the data region of a field occupies for the given capacity.
    const auto dataSize = [&](const DataType& type, const uint64_t capacity) -> uint64_t
    {
        switch (type.type)
        {
            case DataType::Type::BOOLEAN:
                return bitmapSize(capacity);
            case DataType::Type::VARSIZED:
                return align8(std::max(capacity * pendingVarSizedSize, (capacity + 1) * sizeof(int32_t)));
            default:
                return align8(capacity * type.getSizeInBytesWithoutNull());
        }
    };
    /// Three locations per field: validity, data and values.
    const uint64_t headerSize = align8(std::ranges::size(schema) * 3 * sizeof(ArrowBufferRef::Location));
    const auto layoutSize = [&](const uint64_t capacity)
    {
        uint64_t size = headerSize;
        for (const auto& field : schema)
        {
            size += (field.getDataType().nullable ? bitmapSize(capacity) : 0) + dataSize(field.getDataType(), capacity);
        }
        return size;
    };

    /// Start from the estimate without alignment padding, then shrink until the aligned layout fits.
    uint64_t bitsPerRecord = 0;
    for (const auto& field : schema)
    {
        const auto& type = field.getDataType();
        bitsPerRecord += (type.nullable ? 1 : 0)
            + (type.type == DataType::Type::BOOLEAN        ? 1
                   : type.type == DataType::Type::VARSIZED ? pendingVarSizedSize * 8
                                                           : uint64_t{type.getSizeInBytesWithoutNull()} * 8);
    }
    INVARIANT(bufferSize > headerSize, "The header of the Arrow layout does not fit into a buffer of {} bytes", bufferSize);
    uint64_t capacity = (bufferSize - headerSize) * 8 / bitsPerRecord;
    while (capacity > 0 && layoutSize(capacity) > bufferSize)
    {
        --capacity;
    }
    INVARIANT(capacity > 0, "Arrow layout does not fit a single record into a buffer of {} bytes", bufferSize);

    std::vector<ArrowBufferRef::Field> fields;
    fields.reserve(std::ranges::size(schema));
    uint64_t offset = headerSize;
    for (const auto& field : schema)
    {
        const auto& type = field.getDataType();
        if (type.nullable)
        {
            fields.push_back(
                {.name = field.getFullyQualifiedName(), .type = type, .dataTypeSize = 0, .bitmapOffset = offset, .dataOffset = 0});
            offset += bitmapSize(capacity);
        }
        else
        {
            fields.push_back(
                {.name = field.getFullyQualifiedName(),
                 .type = type,
                 .dataTypeSize = 0,
                 .bitmapOffset = ArrowBufferRef::Field::NO_BITMAP,
                 .dataOffset = 0});
        }
    }
    for (auto& field : fields)
    {
        field.dataTypeSize = field.type.type == DataType::Type::VARSIZED
            ? pendingVarSizedSize
            : (field.type.type == DataType::Type::BOOLEAN ? 0 : field.type.getSizeInBytesWithoutNull());
        field.dataOffset = offset;
        offset += dataSize(field.type, capacity);
    }
    /// bufferSize / capacity is the effective size of one record including the header, bitmaps and padding.
    return std::make_shared<ArrowBufferRef>(std::move(fields), headerSize, capacity, bufferSize, bufferSize / capacity);
}
}
