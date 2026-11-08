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

#include <ArrowBatches.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/format.h>
#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
using Region = ArrowBufferRef::Region;
using Location = ArrowBufferRef::Location;

void check(const ArrowErrorCode code, const ArrowError& error, const std::string_view what)
{
    if (code != NANOARROW_OK)
    {
        throw UdfExecutionError("{}: {}", what, error.message);
    }
}

ArrowType arrowType(const DataType& type)
{
    switch (type.type)
    {
        case DataType::Type::INT8:
            return NANOARROW_TYPE_INT8;
        case DataType::Type::UINT8:
            return NANOARROW_TYPE_UINT8;
        case DataType::Type::INT16:
            return NANOARROW_TYPE_INT16;
        case DataType::Type::UINT16:
            return NANOARROW_TYPE_UINT16;
        case DataType::Type::INT32:
            return NANOARROW_TYPE_INT32;
        case DataType::Type::UINT32:
            return NANOARROW_TYPE_UINT32;
        case DataType::Type::INT64:
            return NANOARROW_TYPE_INT64;
        case DataType::Type::UINT64:
            return NANOARROW_TYPE_UINT64;
        case DataType::Type::FLOAT32:
            return NANOARROW_TYPE_FLOAT;
        case DataType::Type::FLOAT64:
            return NANOARROW_TYPE_DOUBLE;
        case DataType::Type::BOOLEAN:
            return NANOARROW_TYPE_BOOL;
        case DataType::Type::VARSIZED:
            return NANOARROW_TYPE_BINARY;
        case DataType::Type::CHAR:
            return NANOARROW_TYPE_FIXED_SIZE_BINARY;
        case DataType::Type::UNDEFINED:
            break;
    }
    throw CannotLoadUdf("UDF processes do not support fields of type {}", type);
}

void releaseTupleBuffer(ArrowBufferAllocator* allocator, uint8_t*, int64_t)
{
    delete static_cast<TupleBuffer*>(allocator->private_data); /// NOLINT(cppcoreguidelines-owning-memory)
}

/// Hands `size` bytes at `data` to `array` as its buffer `index`, keeping `owner` alive until the array releases them.
void setBuffer(ArrowArray* array, const int64_t index, const std::byte* data, const int64_t size, const TupleBuffer& owner)
{
    if (data == nullptr || size == 0)
    {
        return;
    }
    ArrowError error{};
    ArrowBuffer buffer{};
    ArrowBufferInit(&buffer);
    check(
        ArrowBufferSetAllocator(&buffer, ArrowBufferDeallocator(&releaseTupleBuffer, new TupleBuffer(owner))), /// NOLINT
        error,
        "Failed to wrap a buffer for Arrow");
    buffer.data = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(data)); /// NOLINT(cppcoreguidelines-pro-type-const-cast)
    buffer.size_bytes = size;
    buffer.capacity_bytes = size;
    check(ArrowArraySetBuffer(array, index, &buffer), error, "Failed to export a buffer to Arrow");
}
}

nanoarrow::UniqueSchema makeArrowSchema(const ArrowBufferRef& layout)
{
    const auto& fields = layout.getFields();
    nanoarrow::UniqueSchema schema;
    ArrowError error{};
    ArrowSchemaInit(schema.get());
    check(ArrowSchemaSetTypeStruct(schema.get(), static_cast<int64_t>(fields.size())), error, "Failed to create an Arrow schema");
    schema->flags = 0;
    for (size_t index = 0; index < fields.size(); ++index)
    {
        auto* const child = schema->children[index];
        /// A CHAR is one byte per record, which is fixed_size_binary(1) in Arrow.
        const auto type = arrowType(fields[index].type);
        check(
            type == NANOARROW_TYPE_FIXED_SIZE_BINARY ? ArrowSchemaSetTypeFixedSize(child, type, 1) : ArrowSchemaSetType(child, type),
            error,
            "Failed to create an Arrow schema");
        check(ArrowSchemaSetName(child, fmt::format("{}", fields[index].name).c_str()), error, "Failed to create an Arrow schema");
        child->flags = fields[index].type.nullable ? ARROW_FLAG_NULLABLE : 0;
    }
    return schema;
}

nanoarrow::UniqueArray exportArrowArray(const TupleBuffer& buffer, const ArrowBufferRef& layout, const ArrowSchema& schema)
{
    const auto& fields = layout.getFields();
    const auto rows = static_cast<int64_t>(buffer.getNumberOfTuples());
    const auto bitmapBytes = (rows + 7) / 8;

    nanoarrow::UniqueArray array;
    ArrowError error{};
    check(ArrowArrayInitFromSchema(array.get(), &schema, &error), error, "Failed to export a batch to Arrow");
    array->length = rows;
    array->null_count = 0;
    for (size_t index = 0; index < fields.size(); ++index)
    {
        const auto& field = fields[index];
        auto* const child = array->children[index];
        child->length = rows;

        const auto* const validity = ArrowBufferRef::resolve(buffer, layout.getLocation(buffer, index, Region::VALIDITY));
        child->null_count = validity == nullptr ? 0 : rows - ArrowBitCountSet(reinterpret_cast<const uint8_t*>(validity), 0, rows);
        setBuffer(child, 0, validity, bitmapBytes, buffer);

        const auto* const data = ArrowBufferRef::resolve(buffer, layout.getLocation(buffer, index, Region::DATA));
        switch (field.type.type)
        {
            case DataType::Type::BOOLEAN:
                setBuffer(child, 1, data, bitmapBytes, buffer);
                break;
            case DataType::Type::VARSIZED: {
                setBuffer(child, 1, data, (rows + 1) * static_cast<int64_t>(sizeof(int32_t)), buffer);
                int32_t valuesSize = 0;
                std::memcpy(&valuesSize, data + (rows * static_cast<int64_t>(sizeof(int32_t))), sizeof(int32_t));
                setBuffer(child, 2, ArrowBufferRef::resolve(buffer, layout.getLocation(buffer, index, Region::VALUES)), valuesSize, buffer);
                break;
            }
            default:
                setBuffer(child, 1, data, rows * static_cast<int64_t>(field.type.getSizeInBytesWithoutNull()), buffer);
                break;
        }
    }
    check(ArrowArrayFinishBuildingDefault(array.get(), &error), error, "Failed to export a batch to Arrow");
    return array;
}

void importArrowArrayView(
    const ArrowArrayView& batch, const std::byte* body, const Location bodyLocation, TupleBuffer& buffer, const ArrowBufferRef& layout)
{
    const auto& fields = layout.getFields();
    if (batch.storage_type != NANOARROW_TYPE_STRUCT || batch.n_children != static_cast<int64_t>(fields.size()))
    {
        throw UdfExecutionError("The UDF process returned a batch with {} columns instead of {}", batch.n_children, fields.size());
    }
    const auto locate = [&](const ArrowBufferView& region) -> Location
    {
        if (region.size_bytes == 0)
        {
            return {.buffer = Location::NO_BUFFER, .offset = 0};
        }
        const auto offset = reinterpret_cast<const std::byte*>(region.data.data) - body;
        return {.buffer = bodyLocation.buffer, .offset = bodyLocation.offset + static_cast<uint32_t>(offset)};
    };
    for (size_t index = 0; index < fields.size(); ++index)
    {
        const auto& field = fields[index];
        const auto& column = *batch.children[index];
        if (column.offset != 0)
        {
            throw UdfExecutionError("The UDF process returned field {} with an offset, which NES cannot read", field.name);
        }
        if (!field.type.nullable && column.null_count != 0)
        {
            throw UdfExecutionError("The UDF process returned NULL for field {}, which is not nullable", field.name);
        }
        layout.setLocation(buffer, index, Region::VALIDITY, locate(column.buffer_views[0]));
        layout.setLocation(buffer, index, Region::DATA, locate(column.buffer_views[1]));
        layout.setLocation(
            buffer,
            index,
            Region::VALUES,
            field.type.type == DataType::Type::VARSIZED ? locate(column.buffer_views[2])
                                                        : Location{.buffer = Location::NO_BUFFER, .offset = 0});
    }
}

std::vector<ArrowRegion> arrowRegionsOf(const TupleBuffer& buffer, const ArrowBufferRef& layout)
{
    /// Read off the header of `buffer`, sized as exportArrowArray sizes them, without building Arrow structures.
    const auto& fields = layout.getFields();
    const auto rows = static_cast<size_t>(buffer.getNumberOfTuples());
    const auto bitmapBytes = (rows + 7) / 8;
    std::vector<ArrowRegion> regions;
    regions.reserve(3 * fields.size());
    const auto add = [&](const size_t field, const Region which, const size_t size) -> const ArrowRegion&
    {
        const auto location = layout.getLocation(buffer, field, which);
        const auto* const data = ArrowBufferRef::resolve(buffer, location);
        const auto bytes = data == nullptr || size == 0 ? std::span<const std::byte>{} : std::span{data, size};
        return regions.emplace_back(ArrowRegion{.field = field, .region = which, .bytes = bytes, .location = location});
    };
    for (size_t index = 0; index < fields.size(); ++index)
    {
        const auto& field = fields[index];
        add(index, Region::VALIDITY, bitmapBytes);
        switch (field.type.type)
        {
            case DataType::Type::BOOLEAN:
                add(index, Region::DATA, bitmapBytes);
                break;
            case DataType::Type::VARSIZED: {
                const auto& offsets = add(index, Region::DATA, (rows + 1) * sizeof(int32_t));
                int32_t valuesSize = 0;
                if (!offsets.bytes.empty())
                {
                    std::memcpy(&valuesSize, offsets.bytes.data() + (rows * sizeof(int32_t)), sizeof(int32_t));
                }
                add(index, Region::VALUES, static_cast<size_t>(valuesSize));
                break;
            }
            default:
                add(index, Region::DATA, rows * field.type.getSizeInBytesWithoutNull());
                break;
        }
    }
    return regions;
}

std::vector<std::span<const std::byte>> arrowBuffersOf(const TupleBuffer& buffer, const ArrowBufferRef& layout)
{
    return arrowRegionsOf(buffer, layout) | std::views::transform(&ArrowRegion::bytes) | std::ranges::to<std::vector>();
}

void validateArrowBuffers(
    const uint64_t rows, const std::span<const std::span<const std::byte>> buffers, const ArrowBufferRef& layout, const ArrowSchema& schema)
{
    ArrowError error{};
    nanoarrow::UniqueArrayView view;
    if (ArrowArrayViewInitFromSchema(view.get(), &schema, &error) != NANOARROW_OK)
    {
        throw UdfExecutionError("Failed to read a batch of the UDF process: {}", error.message);
    }
    view->length = static_cast<int64_t>(rows);
    view->null_count = 0;
    size_t next = 0;
    for (int64_t field = 0; field < view->n_children; ++field)
    {
        auto& column = *view->children[field];
        column.length = static_cast<int64_t>(rows);
        for (int64_t index = 0; index < NANOARROW_MAX_FIXED_BUFFERS && column.layout.buffer_type[index] != NANOARROW_BUFFER_TYPE_NONE;
             ++index)
        {
            if (next == buffers.size())
            {
                throw UdfExecutionError("The UDF process returned a batch with too few buffers");
            }
            const auto& buffer = buffers[next++];
            column.buffer_views[index]
                = {.data = {.data = buffer.empty() ? nullptr : buffer.data()}, .size_bytes = static_cast<int64_t>(buffer.size())};
        }
    }
    if (next != buffers.size())
    {
        throw UdfExecutionError("The UDF process returned a batch with {} buffers instead of {}", buffers.size(), next);
    }
    if (ArrowArrayViewValidate(view.get(), NANOARROW_VALIDATION_LEVEL_FULL, &error) != NANOARROW_OK)
    {
        throw UdfExecutionError("The UDF process returned an invalid batch: {}", error.message);
    }
    for (int64_t field = 0; field < view->n_children; ++field)
    {
        const auto& column = *view->children[field];
        const auto& validity = column.buffer_views[0];
        if (!layout.getFields()[field].type.nullable && validity.size_bytes != 0
            && ArrowBitCountSet(static_cast<const uint8_t*>(validity.data.data), 0, column.length) != column.length)
        {
            throw UdfExecutionError("The UDF process returned NULL for field {}, which is not nullable", layout.getFields()[field].name);
        }
    }
}

uint64_t rawBodySize(const std::span<const std::span<const std::byte>> buffers)
{
    uint64_t size = 0;
    for (const auto& buffer : buffers)
    {
        size += sizeof(uint64_t) + paddedSize(buffer.size());
    }
    return size;
}

void importRawBody(
    const std::byte* body,
    const uint64_t bodySize,
    const uint64_t rows,
    const Location bodyLocation,
    TupleBuffer& buffer,
    const ArrowBufferRef& layout,
    const ArrowSchema& schema)
{
    /// Points a view of the batch at the buffers in the body, and validates it as the IPC decoder would.
    ArrowError error{};
    nanoarrow::UniqueArrayView view;
    if (ArrowArrayViewInitFromSchema(view.get(), &schema, &error) != NANOARROW_OK)
    {
        throw UdfExecutionError("Failed to read a batch of the UDF process: {}", error.message);
    }
    view->length = static_cast<int64_t>(rows);
    view->null_count = 0;
    uint64_t position = 0;
    for (int64_t field = 0; field < view->n_children; ++field)
    {
        auto& column = *view->children[field];
        column.length = static_cast<int64_t>(rows);
        for (int64_t index = 0; index < NANOARROW_MAX_FIXED_BUFFERS && column.layout.buffer_type[index] != NANOARROW_BUFFER_TYPE_NONE;
             ++index)
        {
            uint64_t size = 0;
            if (position + sizeof(size) > bodySize)
            {
                throw UdfExecutionError("The UDF process returned a batch with too few buffers");
            }
            std::memcpy(&size, body + position, sizeof(size));
            position += sizeof(size);
            if (size > bodySize - position)
            {
                throw UdfExecutionError("The UDF process returned a buffer of {} bytes beyond its batch", size);
            }
            column.buffer_views[index] = {.data = {.data = size == 0 ? nullptr : body + position}, .size_bytes = static_cast<int64_t>(size)};
            position += paddedSize(size);
        }
        column.null_count = column.buffer_views[0].size_bytes == 0 ? 0 : -1;
    }
    if (position != bodySize)
    {
        throw UdfExecutionError("The UDF process returned a batch with {} bytes after its buffers", bodySize - position);
    }
    if (ArrowArrayViewValidate(view.get(), NANOARROW_VALIDATION_LEVEL_FULL, &error) != NANOARROW_OK)
    {
        throw UdfExecutionError("The UDF process returned an invalid batch: {}", error.message);
    }
    for (int64_t field = 0; field < view->n_children; ++field)
    {
        auto& column = *view->children[field];
        if (column.null_count < 0)
        {
            const auto* const validity = static_cast<const uint8_t*>(column.buffer_views[0].data.data);
            column.null_count = column.length - ArrowBitCountSet(validity, 0, column.length);
        }
    }
    importArrowArrayView(*view.get(), body, bodyLocation, buffer, layout);
}

}
