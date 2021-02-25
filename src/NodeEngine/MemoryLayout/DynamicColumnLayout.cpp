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

#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/MemoryLayout/DynamicColumnLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicColumnLayoutBuffer.hpp>

namespace NES::NodeEngine::DynamicMemoryLayout {

DynamicColumnLayout::DynamicColumnLayout(bool checkBoundaries, SchemaPtr schema) : DynamicMemoryLayout() {
    this->checkBoundaryFieldChecks = checkBoundaries;
    this->recordSize = schema->getSchemaSizeInBytes();
    this->fieldSizes = std::vector<FIELD_SIZE>();

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto const& field : schema->fields) {
        auto curFieldSize = physicalDataTypeFactory.getPhysicalType(field->getDataType())->size();
        fieldSizes.emplace_back(curFieldSize);
    }
}

/**
 *
 * @param schema
 * @param bufferSize
 * @return
 */
DynamicColumnLayoutPtr DynamicColumnLayout::create(SchemaPtr schema, bool checkBoundaries) {

    return std::make_shared<DynamicColumnLayout>(checkBoundaries, schema);
}

DynamicMemoryLayoutPtr DynamicColumnLayout::copy() const { return std::make_shared<DynamicColumnLayout>(*this); }

std::unique_ptr<DynamicLayoutBuffer> DynamicColumnLayout::map(TupleBuffer& tupleBuffer) {
    std::vector<COL_OFFSET_SIZE> columnOffsets;

    uint64_t capacity = tupleBuffer.getBufferSize() / recordSize;
    uint64_t offsetCounter = 0;
    for (auto it = fieldSizes.begin(); it != fieldSizes.end(); ++it) {
        columnOffsets.emplace_back(offsetCounter);
        offsetCounter += (*it) * capacity;
    }
    return std::make_unique<DynamicColumnLayoutBuffer>(tupleBuffer, capacity, *this, columnOffsets);
}
}// namespace NES::NodeEngine::DynamicMemoryLayout