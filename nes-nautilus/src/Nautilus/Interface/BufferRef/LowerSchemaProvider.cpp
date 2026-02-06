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
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/ColumnTupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/OutputFormatterBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
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
    std::vector<OutputFormatterBufferRef::Field> fields;
    fields.reserve(schema.getNumberOfFields());
    uint64_t fieldOffset = 0;
    for (const auto& field : schema)
    {
        fields.emplace_back(field.name, field.dataType, fieldOffset);
        fieldOffset += field.dataType.getSizeInBytes();
    }
    const auto tupleSize = std::accumulate(
        fields.begin(),
        fields.end(),
        0UL,
        [](auto size, const OutputFormatterBufferRef::Field& field) { return size + field.type.getSizeInBytes(); });

    /// Create the output formatter descriptor
    auto descriptorConfigOpt = OutputFormatterValidationProvider::provide(outputFormatterType, config);
    INVARIANT(descriptorConfigOpt.has_value(), "Parameter config for output format of type {} could not be validated", outputFormatterType);
    const OutputFormatterDescriptor descriptor(descriptorConfigOpt.value());

    /// Create a output formatter instance by calling the registry
    const std::shared_ptr<OutputFormatter> outputFormatter
        = OutputFormatterProvider::provideOutputFormatter(outputFormatterType, schema.getNumberOfFields(), descriptor);

    return std::make_shared<OutputFormatterBufferRef>(OutputFormatterBufferRef{std::move(fields), outputFormatter, tupleSize, bufferSize});
}

std::shared_ptr<TupleBufferRef>
LowerSchemaProvider::lowerSchema(const uint64_t bufferSize, const Schema& schema, const MemoryLayoutType layoutType)
{
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
                fieldOffset += field.dataType.getSizeInBytes();
            }
            const auto tupleSize = std::accumulate(
                fields.begin(),
                fields.end(),
                0UL,
                [](auto size, const RowTupleBufferRef::Field& field) { return size + field.type.getSizeInBytes(); });
            return std::make_shared<RowTupleBufferRef>(RowTupleBufferRef{std::move(fields), tupleSize, bufferSize});
        }

        case MemoryLayoutType::COLUMNAR_LAYOUT: {
            const uint64_t capacity = bufferSize / schema.getSizeOfSchemaInBytes();
            std::vector<ColumnTupleBufferRef::Field> fields;
            fields.reserve(schema.getNumberOfFields());
            uint64_t columnOffset = 0;
            for (const auto& field : schema)
            {
                fields.emplace_back(field.name, field.dataType, columnOffset);
                columnOffset += (field.dataType.getSizeInBytes() * capacity);
            }
            const auto tupleSize = std::accumulate(
                fields.begin(),
                fields.end(),
                0UL,
                [](auto size, const ColumnTupleBufferRef::Field& field) { return size + field.type.getSizeInBytes(); });

            return std::make_shared<ColumnTupleBufferRef>(ColumnTupleBufferRef{std::move(fields), tupleSize, bufferSize});
        }
    }
    std::unreachable();
}
}
