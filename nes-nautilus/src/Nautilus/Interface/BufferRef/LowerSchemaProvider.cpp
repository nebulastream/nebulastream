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

#include <DataTypes/LogicalTypeBridge.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaLowering.hpp>
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
    /// Compound logical types (e.g. `Point`) reach this point when an upstream
    /// operator has not been lowered yet. Spread them into primitive components
    /// here so the formatter sees one column per physical field.
    const auto flat = NES::lowerSchema(schema);
    std::vector<OutputFormatterBufferRef::Field> fields;
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    fields.reserve(flat.getNumberOfFields());
    fieldNames.reserve(flat.getNumberOfFields());
    for (const auto& field : flat)
    {
        fields.emplace_back(field.name, toPhysical(field.logicalType).value());
        fieldNames.emplace_back(field.name);
    }

    /// Create the output formatter descriptor
    auto descriptorConfigOpt = OutputFormatterValidationProvider::provide(outputFormatterType, config);
    INVARIANT(descriptorConfigOpt.has_value(), "Parameter config for output format of type {} could not be validated", outputFormatterType);
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

    /// Spread compound logical types (e.g. `Point`) into primitive components
    /// before laying out the buffer. Idempotent for already-flat schemas.
    const auto flat = NES::lowerSchema(schema);

    /// For now, we assume that the fields lie in the exact same order as in the Schema. Later on, we can have a separate optimizer phase
    /// that can change the order, alignment or even the datatype implementation, e.g., u32 instead of u8.
    switch (layoutType)
    {
        case MemoryLayoutType::ROW_LAYOUT: {
            std::vector<RowTupleBufferRef::Field> fields;
            fields.reserve(flat.getNumberOfFields());
            uint64_t fieldOffset = 0;
            for (const auto& field : flat)
            {
                const auto physical = toPhysical(field.logicalType).value();
                fields.emplace_back(field.name, physical, fieldOffset);
                fieldOffset += physical.getSizeInBytesWithNull();
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
                flat.begin(),
                flat.end(),
                0UL,
                [](auto size, const Schema::Field& field) { return size + toPhysical(field.logicalType).value().getSizeInBytesWithNull(); });
            INVARIANT(tupleSize > 0, "Tuplesize must be larger than 0B");

            const uint64_t capacity = bufferSize / tupleSize;
            std::vector<ColumnTupleBufferRef::Field> fields;
            fields.reserve(flat.getNumberOfFields());
            uint64_t columnOffset = 0;
            for (const auto& field : flat)
            {
                const auto physical = toPhysical(field.logicalType).value();
                fields.emplace_back(field.name, physical, physical.getSizeInBytesWithNull(), columnOffset);
                columnOffset += (physical.getSizeInBytesWithNull() * capacity);
            }

            return std::make_shared<ColumnTupleBufferRef>(ColumnTupleBufferRef{std::move(fields), tupleSize, bufferSize});
        }
    }
    std::unreachable();
}
}
