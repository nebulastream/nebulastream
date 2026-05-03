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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Nullable.hpp>
#include <DataTypes/PhysicalLayout.hpp>
#include <DataTypes/PhysicalSchema.hpp>
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

namespace
{
/// Resolve a `PhysicalSchema::Field` into the per-cell list of
/// `(record-field-name, DataType)` pairs that the runtime layouts consume.
/// Compound fields (e.g. a `Point` with `_X`/`_Y`/`_Z`) yield one entry per
/// component; scalar fields yield a single entry whose name equals the
/// field's logical name.
std::vector<std::pair<std::string, DataType>> spreadComponents(const PhysicalSchema::Field& field)
{
    const auto nullable = field.physicalType.nullable ? Nullable::IS_NULLABLE : Nullable::NOT_NULLABLE;
    std::vector<std::pair<std::string, DataType>> spread;
    spread.reserve(field.physicalType.components.size());
    for (const auto& component : field.physicalType.components)
    {
        spread.emplace_back(field.name + component.suffix, DataTypeProvider::provideDataType(component.type, nullable));
    }
    return spread;
}
}

std::shared_ptr<TupleBufferRef> LowerSchemaProvider::lowerSchemaWithOutputFormat(
    const uint64_t bufferSize,
    const PhysicalSchema& schema,
    const std::string& outputFormatterType,
    const std::unordered_map<std::string, std::string>& config)
{
    std::vector<OutputFormatterBufferRef::Field> fields;
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    for (const auto& field : schema)
    {
        for (auto& [recordName, dataType] : spreadComponents(field))
        {
            fieldNames.emplace_back(recordName);
            fields.emplace_back(std::move(recordName), std::move(dataType));
        }
    }

    auto descriptorConfigOpt = OutputFormatterValidationProvider::provide(outputFormatterType, config);
    INVARIANT(descriptorConfigOpt.has_value(), "Parameter config for output format of type {} could not be validated", outputFormatterType);
    const OutputFormatterDescriptor descriptor(descriptorConfigOpt.value());

    const std::shared_ptr<OutputFormatter> outputFormatter
        = OutputFormatterProvider::provideOutputFormatter(outputFormatterType, fieldNames, descriptor);

    return std::make_shared<OutputFormatterBufferRef>(OutputFormatterBufferRef{std::move(fields), outputFormatter, bufferSize});
}

std::shared_ptr<TupleBufferRef>
LowerSchemaProvider::lowerSchema(const uint64_t bufferSize, const PhysicalSchema& schema, const MemoryLayoutType layoutType)
{
    PRECONDITION(schema.hasFields(), "We can not lower an empty schema!");

    const auto tupleSize = physicalTupleByteSize(schema);
    INVARIANT(tupleSize > 0, "Tuplesize must be larger than 0B");

    switch (layoutType)
    {
        case MemoryLayoutType::ROW_LAYOUT: {
            std::vector<RowTupleBufferRef::Field> fields;
            uint64_t fieldOffset = 0;
            for (const auto& field : schema)
            {
                for (auto& [recordName, dataType] : spreadComponents(field))
                {
                    const auto cellSize = dataType.getSizeInBytesWithNull();
                    fields.emplace_back(std::move(recordName), std::move(dataType), fieldOffset);
                    fieldOffset += cellSize;
                }
            }
            return std::make_shared<RowTupleBufferRef>(RowTupleBufferRef{std::move(fields), tupleSize, bufferSize});
        }

        case MemoryLayoutType::COLUMNAR_LAYOUT: {
            const uint64_t capacity = bufferSize / tupleSize;
            std::vector<ColumnTupleBufferRef::Field> fields;
            uint64_t columnOffset = 0;
            for (const auto& field : schema)
            {
                for (auto& [recordName, dataType] : spreadComponents(field))
                {
                    const auto cellSize = dataType.getSizeInBytesWithNull();
                    fields.emplace_back(std::move(recordName), std::move(dataType), cellSize, columnOffset);
                    columnOffset += (cellSize * capacity);
                }
            }
            return std::make_shared<ColumnTupleBufferRef>(ColumnTupleBufferRef{std::move(fields), tupleSize, bufferSize});
        }
    }
    std::unreachable();
}
}
