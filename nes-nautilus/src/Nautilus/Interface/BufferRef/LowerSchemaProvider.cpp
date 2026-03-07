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
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/ColumnTupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
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
