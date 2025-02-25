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
#include <MemoryLayout/RowLayout.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Memory::MemoryLayouts
{

RowLayout::RowLayout(SchemaPtr schema, uint64_t bufferSize) : MemoryLayout(bufferSize, schema)
{
    uint64_t offsetCounter = 0;
    for (auto& fieldSize : physicalFieldSizes)
    {
        fieldOffSets.emplace_back(offsetCounter);
        offsetCounter += fieldSize;
    }
}

RowLayout::RowLayout(const RowLayout& other) : RowLayout(other.schema, other.bufferSize)
{
}

std::shared_ptr<RowLayout> RowLayout::create(SchemaPtr schema, uint64_t bufferSize)
{
    return std::make_shared<RowLayout>(schema, bufferSize);
}

const std::vector<FIELD_SIZE>& RowLayout::getFieldOffSets() const
{
    return fieldOffSets;
}

uint64_t RowLayout::getFieldOffset(uint64_t tupleIndex, uint64_t fieldIndex) const
{
    if (fieldIndex >= fieldOffSets.size())
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
    auto offSet = (tupleIndex * recordSize) + fieldOffSets[fieldIndex];
    NES_TRACE("DynamicRowLayoutBuffer.calcOffset: offSet = {}", offSet);
    return offSet;
}

std::shared_ptr<MemoryLayout> RowLayout::deepCopy() const
{
    return std::make_shared<RowLayout>(*this);
}
}
