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

#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>

#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/ArrowBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/ColumnTupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/OutputFormatterBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <OutputFormatters/OutputFormatterProvider.hpp>
#include <OutputFormatters/OutputFormatterValidationProvider.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

std::shared_ptr<TupleBufferRef> LowerSchemaProvider::lowerSchemaWithOutputFormat(
    const uint64_t bufferSize,
    const Schema& schema,
    const std::string& outputFormatterType,
    const std::unordered_map<std::string, std::string>& config)
{
    /// Validate config through the plugin registry regardless of format type
    auto descriptorConfigOpt = OutputFormatterValidationProvider::provide(outputFormatterType, config);
    INVARIANT(descriptorConfigOpt.has_value(), "Parameter config for output format of type {} could not be validated", outputFormatterType);

    /// Arrow format uses ArrowBufferRef (columnar Arrow-compatible layout) instead of OutputFormatterBufferRef
    if (outputFormatterType == "Arrow")
    {
        const auto numFields = schema.getNumberOfFields();

        /// Compute capacity: each record needs sum(fieldSizes) data bytes + numFields/8 bitmap bits.
        /// Arrow layout stores values without inline null bytes — nullable fields use the bitmap instead.
        /// VARSIZED fields store int32 offsets in the main buffer (4 bytes per record); actual string data lives in child buffers.
        /// BOOLEAN data is bit-packed (like the validity bitmap), so it contributes 1/8 byte per record
        /// rather than the 1 byte that getSizeInBytesWithoutNull() would return.
        const uint64_t dataSizePerRecord = std::accumulate(
            schema.begin(),
            schema.end(),
            uint64_t{0},
            [](uint64_t acc, const Schema::Field& f)
            {
                if (f.dataType.type == DataType::Type::BOOLEAN)
                {
                    return acc; /// handled as bitmap overhead below
                }
                return acc + (f.dataType.type == DataType::Type::VARSIZED ? sizeof(int32_t) : f.dataType.getSizeInBytesWithoutNull());
            });

        const uint64_t numBoolColumns = std::count_if(
            schema.begin(), schema.end(), [](const Schema::Field& f) { return f.dataType.type == DataType::Type::BOOLEAN; });
        INVARIANT(dataSizePerRecord > 0 || numBoolColumns > 0, "Data size per record must be larger than 0B");

        /// Each column has a validity bitmap (1 bit/record). BOOLEAN data columns also use 1 bit/record.
        const double bitmapOverheadPerRecord = static_cast<double>(numFields + numBoolColumns) / 8.0;
        uint64_t capacity = static_cast<uint64_t>(
            static_cast<double>(bufferSize) / (static_cast<double>(dataSizePerRecord) + bitmapOverheadPerRecord));
        INVARIANT(capacity > 0, "Arrow buffer capacity must be > 0 for bufferSize={}", bufferSize);

        /// Pre-compute per-column offsets: [bitmap0][bitmap1]...[data0][data1]...
        constexpr auto align8 = [](uint64_t v) -> uint64_t { return (v + 7) & ~uint64_t{7}; };

        /// The initial capacity estimate ignores 8-byte alignment padding on each column region.
        /// Shrink capacity until the aligned layout fits within the buffer.
        auto computeAlignedSize = [&](uint64_t cap) -> uint64_t
        {
            uint64_t size = numFields * align8((cap + 7) / 8); /// validity bitmaps
            for (const auto& field : schema)
            {
                if (field.dataType.type == DataType::Type::BOOLEAN)
                {
                    size += align8((cap + 7) / 8); /// BOOLEAN data is bit-packed
                }
                else if (field.dataType.type == DataType::Type::VARSIZED)
                {
                    size += align8(static_cast<uint64_t>(cap + 1) * sizeof(int32_t));
                }
                else
                {
                    size += align8(cap * field.dataType.getSizeInBytesWithoutNull());
                }
            }
            return size;
        };
        while (computeAlignedSize(capacity) > bufferSize && capacity > 1)
        {
            --capacity;
        }

        const uint64_t bitmapBytesPerColumn = align8((capacity + 7) / 8);

        std::vector<ArrowBufferRef::Field> fields;
        fields.reserve(numFields);
        uint64_t offset = 0;

        /// First pass: bitmap offsets
        std::vector<uint64_t> bitmapOffsets;
        bitmapOffsets.reserve(numFields);
        for (uint64_t i = 0; i < numFields; ++i)
        {
            bitmapOffsets.push_back(offset);
            offset += bitmapBytesPerColumn;
        }

        /// Second pass: data offsets
        uint64_t fieldIdx = 0;
        uint32_t nextChildSlot = 0;
        for (const auto& field : schema)
        {
            if (field.dataType.type == DataType::Type::BOOLEAN)
            {
                /// BOOLEAN: data is bit-packed (1 bit per record), same layout as a validity bitmap.
                /// dataTypeSize is 0 — BOOLEAN uses bit-addressed read/write, not calculateArrowFieldAddress.
                fields.emplace_back(field.name, field.dataType, 0, bitmapOffsets[fieldIdx], offset,
                                    ArrowBufferRef::Field::NO_CHILD_BUFFER);
                offset += align8((capacity + 7) / 8);
            }
            else if (field.dataType.type == DataType::Type::VARSIZED)
            {
                /// VARSIZED: store int32 offsets array (cap+1 entries for sentinel) in main buffer
                constexpr uint32_t offsetEntrySize = sizeof(int32_t);
                const uint32_t childSlot = nextChildSlot++;
                fields.emplace_back(field.name, field.dataType, offsetEntrySize, bitmapOffsets[fieldIdx], offset, childSlot);
                offset += align8(static_cast<uint64_t>(capacity + 1) * offsetEntrySize);
            }
            else
            {
                const uint32_t fieldSize = field.dataType.getSizeInBytesWithoutNull();
                fields.emplace_back(field.name, field.dataType, fieldSize, bitmapOffsets[fieldIdx], offset,
                                    ArrowBufferRef::Field::NO_CHILD_BUFFER);
                offset += align8(capacity * fieldSize);
            }
            ++fieldIdx;
        }

        return std::make_shared<ArrowBufferRef>(ArrowBufferRef{std::move(fields), nextChildSlot, capacity, bufferSize, dataSizePerRecord});
    }

    /// For all other formats, use the OutputFormatterBufferRef path
    std::vector<OutputFormatterBufferRef::Field> fields;
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    fields.reserve(schema.getNumberOfFields());
    fieldNames.reserve(schema.getNumberOfFields());
    for (const auto& field : schema)
    {
        fields.emplace_back(field.name, field.dataType);
        fieldNames.emplace_back(field.name);
    }

    const OutputFormatterDescriptor descriptor(descriptorConfigOpt.value());

    /// Create a output formatter instance by calling the registry
    const std::shared_ptr<OutputFormatter> outputFormatter
        = OutputFormatterProvider::provideOutputFormatter(outputFormatterType, fieldNames, descriptor);

    return std::make_shared<OutputFormatterBufferRef>(OutputFormatterBufferRef{std::move(fields), outputFormatter, bufferSize});
}

std::shared_ptr<TupleBufferRef>
LowerSchemaProvider::lowerSchema(const uint64_t bufferSize, const Schema& schema, const MemoryLayoutType layoutType)
{
    PRECONDITION(schema.hasFields(), "We can not lower an empty schema!");

    /// For now, we assume that the fields lie in the exact same order as in the Schema. Later on, we can have a separate optimizer phase
    /// that can change the order, alignment or even the datatype implementation, e.g., u32 instead of u8.
    switch (layoutType)
    {
        case MemoryLayoutType::ROW_LAYOUT: {
            std::vector<RowTupleBufferRef::Field> fields;
            fields.reserve(schema.getNumberOfFields());
            uint64_t fieldOffset = 0;
            for (const auto& field : schema)
            {
                fields.emplace_back(field.name, field.dataType, fieldOffset);
                fieldOffset += field.dataType.getSizeInBytesWithNull();
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
                [](auto size, const Schema::Field& field) { return size + field.dataType.getSizeInBytesWithNull(); });
            INVARIANT(tupleSize > 0, "Tuplesize must be larger than 0B");

            const uint64_t capacity = bufferSize / tupleSize;
            std::vector<ColumnTupleBufferRef::Field> fields;
            fields.reserve(schema.getNumberOfFields());
            uint64_t columnOffset = 0;
            for (const auto& field : schema)
            {
                fields.emplace_back(field.name, field.dataType, field.dataType.getSizeInBytesWithNull(), columnOffset);
                columnOffset += (field.dataType.getSizeInBytesWithNull() * capacity);
            }

            return std::make_shared<ColumnTupleBufferRef>(ColumnTupleBufferRef{std::move(fields), tupleSize, bufferSize});
        }
    }
    std::unreachable();
}
}
