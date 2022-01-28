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
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Exceptions/BufferAccessException.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::MemoryLayouts {

RowLayout::RowLayout(SchemaPtr schema, uint64_t bufferSize) : MemoryLayout(bufferSize, schema) {
    uint64_t offsetCounter = 0;
    for (auto& fieldSize : physicalFieldSizes) {
        fieldOffSets.emplace_back(offsetCounter);
        offsetCounter += fieldSize;
    }
}

std::shared_ptr<RowLayout> RowLayout::create(SchemaPtr schema, uint64_t bufferSize) {
    return std::make_shared<RowLayout>(schema, bufferSize);
}

std::shared_ptr<RowLayoutTupleBuffer> RowLayout::bind(const TupleBuffer& tupleBuffer) {
    uint64_t capacity = tupleBuffer.getBufferSize() / recordSize;
    return std::make_shared<RowLayoutTupleBuffer>(tupleBuffer, capacity, this->shared_from_this());
}

const std::vector<FIELD_SIZE>& RowLayout::getFieldOffSets() const { return fieldOffSets; }

uint64_t RowLayout::getFieldOffset(uint64_t tupleIndex, uint64_t fieldIndex) const {
    if (fieldIndex >= fieldOffSets.size()) {
        throw BufferAccessException("field index: " + std::to_string(fieldIndex)
                                    + " is larger the number of field in the memory layout "
                                    + std::to_string(physicalFieldSizes.size()));
    }
    if (tupleIndex >= getCapacity()) {
        throw BufferAccessException("tuple index: " + std::to_string(tupleIndex)
                                    + " is larger the maximal capacity in the memory layout " + std::to_string(getCapacity()));
    }
    auto offSet = (tupleIndex * recordSize) + fieldOffSets[fieldIndex];
    NES_TRACE("DynamicRowLayoutBuffer.calcOffset: offSet = " << offSet);
    return offSet;
}

}// namespace NES::Runtime::MemoryLayouts