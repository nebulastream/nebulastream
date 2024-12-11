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
#include <API/AttributeField.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Memory::MemoryLayouts
{

ColumnLayout::ColumnLayout(SchemaPtr schema, uint64_t bufferSize) : MemoryLayout(bufferSize, schema)
{
    uint64_t offsetCounter = 0;
    for (auto& fieldSize : physicalFieldSizes)
    {
        columnOffsets.emplace_back(offsetCounter);
        offsetCounter += fieldSize * capacity;
    }
}

ColumnLayout::ColumnLayout(const ColumnLayout& other) /// NOLINT(*-copy-constructor-init)
    : MemoryLayout(other), columnOffsets(other.columnOffsets)
{
}

std::shared_ptr<ColumnLayout> ColumnLayout::create(SchemaPtr schema, uint64_t bufferSize)
{
    return std::make_shared<ColumnLayout>(schema, bufferSize);
}

uint64_t ColumnLayout::getFieldOffset(uint64_t tupleIndex, uint64_t fieldIndex) const
{
    if (fieldIndex >= physicalFieldSizes.size())
    {
        throw CannotAccessBuffer(
            "field index: {} is larger the number of field in the memory layout {}",
            std::to_string(fieldIndex),
            std::to_string(physicalFieldSizes.size()));
    }
    if (tupleIndex >= getCapacity())
    {
        throw CannotAccessBuffer(
            "tuple index: {} is larger the maximal capacity in the memory layout {}",
            std::to_string(tupleIndex),
            std::to_string(getCapacity()));
    }

    auto fieldOffset = (tupleIndex * physicalFieldSizes[fieldIndex]) + columnOffsets[fieldIndex];
    return fieldOffset;
}

const std::vector<uint64_t>& ColumnLayout::getColumnOffsets() const
{
    return columnOffsets;
}

std::shared_ptr<MemoryLayout> ColumnLayout::deepCopy() const
{
    return std::make_shared<ColumnLayout>(*this);
}
}
