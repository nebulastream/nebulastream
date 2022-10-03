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
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Exceptions/BufferAccessException.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutTupleBuffer.hpp>

namespace NES::Runtime::MemoryLayouts {

ColumnLayout::ColumnLayout(SchemaPtr schema, uint64_t bufferSize) : MemoryLayout(bufferSize, schema) {
    uint64_t offsetCounter = 0;
    for (auto& fieldSize : physicalFieldSizes) {
        columnOffsets.emplace_back(offsetCounter);
        offsetCounter += fieldSize * capacity;
    }
}

ColumnLayoutPtr ColumnLayout::create(SchemaPtr schema, uint64_t bufferSize) {
    return std::make_shared<ColumnLayout>(schema, bufferSize);
}

std::shared_ptr<ColumnLayoutTupleBuffer> ColumnLayout::bind(const TupleBuffer& tupleBuffer) {
    return std::make_shared<ColumnLayoutTupleBuffer>(tupleBuffer, capacity, this->shared_from_this(), columnOffsets);
}

uint64_t ColumnLayout::getFieldOffset(uint64_t tupleIndex, uint64_t fieldIndex) const {

    if (fieldIndex >= physicalFieldSizes.size()) {
        throw BufferAccessException("field index: " + std::to_string(fieldIndex)
                                    + " is larger the number of field in the memory layout "
                                    + std::to_string(physicalFieldSizes.size()));
    }
    if (tupleIndex >= getCapacity()) {
        throw BufferAccessException("tuple index: " + std::to_string(tupleIndex)
                                    + " is larger the maximal capacity in the memory layout " + std::to_string(getCapacity()));
    }

    auto fieldOffset = (tupleIndex * physicalFieldSizes[fieldIndex]) + columnOffsets[fieldIndex];
    NES_INFO("fieldOffset = " << fieldOffset);
    return fieldOffset;
}
const std::vector<uint64_t>& ColumnLayout::getColumnOffsets() const { return columnOffsets; }

}// namespace NES::Runtime::MemoryLayouts