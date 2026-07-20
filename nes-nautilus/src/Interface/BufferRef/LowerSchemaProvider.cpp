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

#include <cstdint>
#include <memory>
#include <numeric>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/BufferRef/ColumnTupleBufferRef.hpp>
#include <Interface/BufferRef/OutputFormatterBufferRef.hpp>
#include <Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <OutputFormatters/OutputFormatterProvider.hpp>
#include <OutputFormatters/OutputFormatterValidationProvider.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

std::shared_ptr<TupleBufferRef> LowerSchemaProvider::lowerSchemaWithOutputFormat(
    const Schema<QualifiedUnboundField, Ordered>& schema,
    const std::string& outputFormatterType,
    const std::unordered_map<Identifier, std::string>& config)
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

    /// Create the output formatter descriptor
    auto descriptorConfigOpt = OutputFormatterValidationProvider::provide(outputFormatterType, config);
    INVARIANT(descriptorConfigOpt.has_value(), "Parameter config for output format of type {} could not be validated", outputFormatterType);
    const OutputFormatterDescriptor descriptor(descriptorConfigOpt.value());

    /// Create a output formatter instance by calling the registry
    const std::shared_ptr<OutputFormatter> outputFormatter
        = OutputFormatterProvider::provideOutputFormatter(outputFormatterType, fieldNames, descriptor);

    return std::make_shared<OutputFormatterBufferRef>(OutputFormatterBufferRef{std::move(fields), outputFormatter});
}

std::shared_ptr<TupleBufferRef> LowerSchemaProvider::lowerSchema(
    const uint64_t bufferSize, const Schema<QualifiedUnboundField, Ordered>& schema, const MemoryLayoutType layoutType)
{
    PRECONDITION(!std::ranges::empty(schema), "We can not lower an empty schema!");

    /// For now, we assume that the fields lie in the exact same order as in the Schema. Later on, we can have a separate optimizer phase
    /// that can change the order, alignment or even the datatype implementation, e.g., u32 instead of u8.
    switch (layoutType)
    {
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
            return std::make_shared<RowTupleBufferRef>(RowTupleBufferRef{std::move(fields), tupleSize});
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

            return std::make_shared<ColumnTupleBufferRef>(ColumnTupleBufferRef{std::move(fields), tupleSize});
        }
    }
    std::unreachable();
}
}
