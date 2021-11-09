/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutTupleBuffer.hpp>

namespace NES::Runtime::MemoryLayouts {

ColumnLayout::ColumnLayout(SchemaPtr schema, uint64_t bufferSize): MemoryLayout(bufferSize, schema) {
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

uint64_t ColumnLayout::getFieldOffset(uint64_t recordIndex, uint64_t fieldIndex) {

    if (fieldIndex >= physicalFieldSizes.size()) {
        NES_THROW_RUNTIME_ERROR("jthField" << fieldIndex << " is larger than fieldSize.size() " << physicalFieldSizes.size());
    }
    if (fieldIndex >= columnOffsets.size()) {
        NES_THROW_RUNTIME_ERROR("columnOffsets" << fieldIndex << " is larger than columnOffsets.size() " << columnOffsets.size());
    }

    auto fieldOffset = (recordIndex * physicalFieldSizes[fieldIndex]) + columnOffsets[fieldIndex];
    NES_DEBUG("fieldOffset = " << fieldOffset);
    return fieldOffset;
}

}// namespace NES::Runtime::MemoryLayouts