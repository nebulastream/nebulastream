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

#include <LowerSchemaProvider.hpp>

#include <cstdint>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <ColumnTupleBufferRef.hpp>
#include <RowTupleBufferRef.hpp>
#include <TupleBufferRef.hpp>
#include <TupleBufferRefDescriptor.hpp>
#include <InputFormatterTupleBufferRefProvider.hpp>
#include <InputFormatIndexerRegistry.hpp>

namespace NES
{
std::shared_ptr<TupleBufferRef>
LowerSchemaProvider::lowerSchema(const uint64_t bufferSize, const Schema& schema, const MemoryLayoutType layoutType, TupleBufferRefDescriptor descriptor)
{
    /// For now, we assume that the fields lie in the exact same order as in the Schema. Later on, we can have a separate optimizer phase
    /// that can change the order, alignment or even the datatype implementation, e.g., u32 instead of u8.


    // Todo:
    // 1. Keep switch case, implement only INPUT_FORMATTER case
    // 2.  get rid of switch case, implement row/column logic in dedicated files via register calls
    //  - think of CSV/JSON/HL7/XML as memoryLayouts just like NES_ROW/NES_COLUMN, the InputFormatterTupleBufferRef then generates the Text layouts
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
        case MemoryLayoutType::INPUT_FORMATTER_LAYOUT: {
            // Todo: decide whether to use one or two registry
            // - could for now simply continue with InputFormatter registry and later replace
            const auto inputFormatterType = std::get<std::string>(descriptor.getConfig().at("input_format"));
            if (auto inputFormatter = InputFormatIndexerRegistry::instance().create(
                inputFormatterType, InputFormatIndexerRegistryArguments(descriptor, schema, bufferSize)))
            {
                return std::move(inputFormatter.value());
            }
            throw UnknownParserType("unknown type of input formatter: {}", inputFormatterType);
            // Todo:
            // Problem:
            // - registry for BufferRefs and for InputFormatters
            // - or: single shared registry
            //      - rewrite 'LowerSchemaProvider' and make it replace 'InputFormatterTupleBufferRefProvider'
            //          - probably also rename 'LowerSchemaProvider' to 'TupleBufferRefProvider' <-- check current return type(s) <-- matches!
            // --------------------------
            // Problem: should create InputFormatterTupleBufferRef, but cannot, because it would require dependency from
            //          nes-nautilus -> nes-input-formatters, but there is already a dependency from nes-input-formatters -> nes-nautilus
            // Solution S1:
            //  - move BufferRefs into separate module 'nes-tuple-buffer-refs' <-- memory layout component, essentially contains info on how to access buffers
            //  - move nes-input-formatters into 'nes-tuple-buffer-refs'
            //  - MAJOR PROBLEM:
            //      - circular dependency, buffer-refs need nautilus ?(and nautilus would need buffer refs)
            //      - NOT REALLY: only tests depend on buffer-refs and refs have their own library, so that library can depend on both
            // std::vector<RowTupleBufferRef::Field> fields;
            // fields.reserve(schema.getNumberOfFields());
            // uint64_t fieldOffset = 0;
            // for (const auto& field : schema)
            // {
            //     fields.emplace_back(field.name, field.dataType, fieldOffset);
            //     fieldOffset += field.dataType.getSizeInBytes();
            // }
            // const auto tupleSize = std::accumulate(
            //     fields.begin(),
            //     fields.end(),
            //     0UL,
            //     [](auto size, const RowTupleBufferRef::Field& field) { return size + field.type.getSizeInBytes(); });
            // return std::make_shared<RowTupleBufferRef>(RowTupleBufferRef{std::move(fields), tupleSize, bufferSize});
        }
    }
    std::unreachable();
}
}
