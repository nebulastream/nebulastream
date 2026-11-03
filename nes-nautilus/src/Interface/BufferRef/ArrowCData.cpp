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

#include <Interface/BufferRef/ArrowCData.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES::Arrow
{

namespace
{
constexpr const char* STRUCT_FORMAT = "+s";

const char* arrowFormat(const DataType& type)
{
    switch (type.type)
    {
        case DataType::Type::INT8:
            return "c";
        case DataType::Type::UINT8:
        case DataType::Type::CHAR:
            return "C";
        case DataType::Type::INT16:
            return "s";
        case DataType::Type::UINT16:
            return "S";
        case DataType::Type::INT32:
            return "i";
        case DataType::Type::UINT32:
            return "I";
        case DataType::Type::INT64:
            return "l";
        case DataType::Type::UINT64:
            return "L";
        case DataType::Type::FLOAT32:
            return "f";
        case DataType::Type::FLOAT64:
            return "g";
        case DataType::Type::BOOLEAN:
            return "b";
        case DataType::Type::VARSIZED:
            return "u";
        case DataType::Type::UNDEFINED:
            break;
    }
    throw InvalidArrowData("Data type {} has no Arrow representation", type);
}

/// Every node of an exported tree holds a reference to the storage shared by the tree. Consumers may move children out of
/// their parent and release them individually, which is why each node needs its own release callback and reference.
template <typename Node>
void releaseNode(Node* node)
{
    for (int64_t i = 0; i < node->n_children; ++i)
    {
        if (node->children[i]->release != nullptr)
        {
            node->children[i]->release(node->children[i]);
        }
    }
    delete static_cast<std::shared_ptr<void>*>(node->private_data); /// NOLINT(cppcoreguidelines-owning-memory)
    node->release = nullptr;
}

struct ArrayStorage
{
    static constexpr int32_t EMPTY_OFFSETS = 0;

    TupleBuffer buffer;
    std::vector<std::vector<std::byte>> concatenatedPayloads;
    std::vector<std::array<const void*, 3>> columnBuffers;
    std::vector<ArrowArray> children;
    std::vector<ArrowArray*> childPointers;
    std::array<const void*, 1> structBuffers{nullptr};
};

struct SchemaStorage
{
    std::vector<std::string> names;
    std::vector<ArrowSchema> children;
    std::vector<ArrowSchema*> childPointers;
};

/// Returns the contiguous payload of a VARSIZED column
const std::byte* exportPayload(ArrayStorage& storage, const ArrowBufferRef& layout, const ArrowBufferRef::Field& field)
{
    auto children = layout.loadVarSizedChildBuffers(storage.buffer, field);
    std::erase_if(children, [](const TupleBuffer& child) { return child.getNumberOfTuples() == 0; });
    if (children.empty())
    {
        return nullptr;
    }
    if (children.size() == 1)
    {
        /// The child is kept alive by storage.buffer
        return std::as_const(children.front()).getAvailableMemoryArea<std::byte>().data();
    }
    auto& payload = storage.concatenatedPayloads.emplace_back();
    for (const auto& child : children)
    {
        const auto bytes = std::as_const(child).getAvailableMemoryArea<std::byte>().first(child.getNumberOfTuples());
        payload.insert(payload.end(), bytes.begin(), bytes.end());
    }
    return payload.data();
}

bool getBit(const uint8_t* bitmap, const uint64_t index)
{
    return ((bitmap[index / 8] >> (index % 8)) & 1) != 0;
}

void setBit(uint8_t* bitmap, const uint64_t index, const bool value)
{
    const auto mask = static_cast<uint8_t>(1U << (index % 8));
    bitmap[index / 8] = value ? (bitmap[index / 8] | mask) : (bitmap[index / 8] & static_cast<uint8_t>(~mask));
}

bool isValid(const ArrowArray& column, const uint64_t physicalIndex)
{
    const auto* validity = static_cast<const uint8_t*>(column.buffers[0]);
    return validity == nullptr || column.null_count == 0 || getBit(validity, physicalIndex);
}

void validate(const ArrowArray& array, const ArrowSchema& schema, const ArrowBufferRef& layout)
{
    if (array.release == nullptr || schema.release == nullptr)
    {
        throw InvalidArrowData("The batch was already released");
    }
    if (std::string_view{schema.format} != STRUCT_FORMAT)
    {
        throw InvalidArrowData("Expected a record batch (format {}) but got format {}", STRUCT_FORMAT, schema.format);
    }
    if (array.null_count != 0 && array.n_buffers > 0 && array.buffers[0] != nullptr)
    {
        throw InvalidArrowData("A record batch must not have null rows");
    }
    const auto& fields = layout.getFields();
    if (array.n_children != static_cast<int64_t>(fields.size()) || schema.n_children != static_cast<int64_t>(fields.size()))
    {
        throw InvalidArrowData("Expected {} columns but got {} (schema: {})", fields.size(), array.n_children, schema.n_children);
    }
    for (size_t i = 0; i < fields.size(); ++i)
    {
        const auto* expectedFormat = arrowFormat(fields[i].type);
        if (std::string_view{schema.children[i]->format} != expectedFormat)
        {
            throw InvalidArrowData(
                "Column {} ({}) must have Arrow format {} but has {}", i, fields[i].name, expectedFormat, schema.children[i]->format);
        }
        if (array.children[i]->length < array.offset + array.length)
        {
            throw InvalidArrowData("Column {} ({}) is shorter than the batch", i, fields[i].name);
        }
    }
}

/// Copies @param count rows of a column starting at the batch-relative row @param first, which already includes the batch's offset.
void importColumn(
    const ArrowArray& column,
    const uint64_t first,
    const uint64_t count,
    const ArrowBufferRef::Field& field,
    const uint32_t numVarSizedColumns,
    TupleBuffer& target,
    AbstractBufferProvider& bufferProvider)
{
    auto* memory = target.getAvailableMemoryArea<uint8_t>().data();
    const auto physical = static_cast<uint64_t>(column.offset) + first;

    /// The validity bitmap of non-nullable fields is unused, so we only have to verify that there are no nulls.
    for (uint64_t row = 0; row < count; ++row)
    {
        const auto valid = isValid(column, physical + row);
        if (field.type.nullable)
        {
            setBit(memory + field.bitmapOffset, row, valid);
        }
        else if (not valid)
        {
            throw InvalidArrowData("Field {} is not nullable but row {} is null", field.name, first + row);
        }
    }

    if (field.type.type == DataType::Type::BOOLEAN)
    {
        const auto* values = static_cast<const uint8_t*>(column.buffers[1]);
        for (uint64_t row = 0; row < count; ++row)
        {
            setBit(memory + field.dataOffset, row, getBit(values, physical + row));
        }
    }
    else if (field.type.type == DataType::Type::VARSIZED)
    {
        const auto* sourceOffsets = static_cast<const int32_t*>(column.buffers[1]);
        const auto* payload = static_cast<const std::byte*>(column.buffers[2]);
        auto* targetOffsets = reinterpret_cast<int32_t*>(memory + field.dataOffset); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        targetOffsets[0] = 0;
        for (uint64_t row = 0; row < count; ++row)
        {
            /// Arrow allows null values to span bytes, which we do not need to carry over.
            uint64_t length = 0;
            if (isValid(column, physical + row))
            {
                const auto start = sourceOffsets[physical + row];
                length = static_cast<uint64_t>(sourceOffsets[physical + row + 1] - start);
                if (length > static_cast<uint64_t>(std::numeric_limits<int32_t>::max() - targetOffsets[row]))
                {
                    throw InvalidArrowData("Values of field {} exceed the int32 offsets of a single buffer", field.name);
                }
                if (length > 0)
                {
                    ArrowBufferRef::appendVarSized(target, bufferProvider, field.childBufferSlot, numVarSizedColumns, {payload + start, length});
                }
            }
            targetOffsets[row + 1] = targetOffsets[row] + static_cast<int32_t>(length);
        }
    }
    else
    {
        const auto* values = static_cast<const uint8_t*>(column.buffers[1]);
        std::memcpy(memory + field.dataOffset, values + (physical * field.dataTypeSize), count * field.dataTypeSize);
    }
}
}

void exportBatch(const TupleBuffer& buffer, const ArrowBufferRef& layout, ArrowArray& outArray, ArrowSchema& outSchema)
{
    const auto& fields = layout.getFields();
    const auto numFields = fields.size();
    const auto numRecords = static_cast<int64_t>(buffer.getNumberOfTuples());
    const auto* memory = buffer.getAvailableMemoryArea<uint8_t>().data();

    /// ponytail: allocates per batch, reuse the schema across batches and pool the storage if this shows up in a profile
    auto arrayStorage = std::make_shared<ArrayStorage>();
    arrayStorage->buffer = buffer;
    arrayStorage->columnBuffers.reserve(numFields);
    arrayStorage->concatenatedPayloads.reserve(numFields);
    arrayStorage->children.resize(numFields);
    auto schemaStorage = std::make_shared<SchemaStorage>();
    schemaStorage->children.resize(numFields);

    for (size_t i = 0; i < numFields; ++i)
    {
        const auto& field = fields[i];
        const auto isVarSized = field.type.type == DataType::Type::VARSIZED;

        const void* values = memory + field.dataOffset;
        const void* payload = nullptr;
        if (isVarSized)
        {
            payload = exportPayload(*arrayStorage, layout, field);
            if (numRecords == 0)
            {
                /// The offsets of an empty buffer were never written
                values = &ArrayStorage::EMPTY_OFFSETS;
            }
        }
        /// Non-nullable fields do not maintain their bitmap, which Arrow permits by a null pointer and a null count of 0.
        arrayStorage->columnBuffers.push_back({field.type.nullable ? memory + field.bitmapOffset : nullptr, values, payload});

        arrayStorage->children[i] = ArrowArray{
            .length = numRecords,
            .null_count = field.type.nullable ? -1 : 0,
            .offset = 0,
            .n_buffers = isVarSized ? 3 : 2,
            .n_children = 0,
            .buffers = arrayStorage->columnBuffers[i].data(),
            .children = nullptr,
            .dictionary = nullptr,
            .release = &releaseNode<ArrowArray>,
            .private_data = new std::shared_ptr<void>(arrayStorage)}; /// NOLINT(cppcoreguidelines-owning-memory)
        arrayStorage->childPointers.push_back(&arrayStorage->children[i]);

        schemaStorage->names.push_back(fmt::format("{}", field.name));
    }
    for (size_t i = 0; i < numFields; ++i)
    {
        /// A second loop because growing names could invalidate the c_str() of short strings
        schemaStorage->children[i] = ArrowSchema{
            .format = arrowFormat(fields[i].type),
            .name = schemaStorage->names[i].c_str(),
            .metadata = nullptr,
            .flags = fields[i].type.nullable ? ARROW_FLAG_NULLABLE : 0,
            .n_children = 0,
            .children = nullptr,
            .dictionary = nullptr,
            .release = &releaseNode<ArrowSchema>,
            .private_data = new std::shared_ptr<void>(schemaStorage)}; /// NOLINT(cppcoreguidelines-owning-memory)
        schemaStorage->childPointers.push_back(&schemaStorage->children[i]);
    }

    outArray = ArrowArray{
        .length = numRecords,
        .null_count = 0,
        .offset = 0,
        .n_buffers = 1,
        .n_children = static_cast<int64_t>(numFields),
        .buffers = arrayStorage->structBuffers.data(),
        .children = arrayStorage->childPointers.data(),
        .dictionary = nullptr,
        .release = &releaseNode<ArrowArray>,
        .private_data = new std::shared_ptr<void>(arrayStorage)}; /// NOLINT(cppcoreguidelines-owning-memory)
    outSchema = ArrowSchema{
        .format = STRUCT_FORMAT,
        .name = "",
        .metadata = nullptr,
        .flags = 0,
        .n_children = static_cast<int64_t>(numFields),
        .children = schemaStorage->childPointers.data(),
        .dictionary = nullptr,
        .release = &releaseNode<ArrowSchema>,
        .private_data = new std::shared_ptr<void>(schemaStorage)}; /// NOLINT(cppcoreguidelines-owning-memory)
}

std::vector<TupleBuffer>
importBatch(const ArrowArray& array, const ArrowSchema& schema, const ArrowBufferRef& layout, AbstractBufferProvider& bufferProvider)
{
    PRECONDITION(bufferProvider.getBufferSize() >= layout.getBufferSize(), "Buffers are too small for the layout");
    validate(array, schema, layout);

    const auto& fields = layout.getFields();
    const auto numVarSizedColumns
        = static_cast<uint32_t>(std::ranges::count_if(fields, [](const auto& field) { return field.childBufferSlot != ArrowBufferRef::Field::NO_CHILD_BUFFER; }));
    const auto rows = static_cast<uint64_t>(array.length);

    std::vector<TupleBuffer> buffers;
    uint64_t first = 0;
    do
    {
        const auto count = std::min(rows - first, layout.getCapacity());
        auto buffer = bufferProvider.getBufferBlocking();
        for (size_t i = 0; i < fields.size(); ++i)
        {
            importColumn(*array.children[i], static_cast<uint64_t>(array.offset) + first, count, fields[i], numVarSizedColumns, buffer, bufferProvider);
        }
        buffer.setNumberOfTuples(count);
        buffers.push_back(std::move(buffer));
        first += count;
    } while (first < rows);
    return buffers;
}

}
