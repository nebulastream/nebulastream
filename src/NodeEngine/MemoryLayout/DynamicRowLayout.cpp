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

#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <NodeEngine/TupleBuffer.hpp>

namespace NES::NodeEngine {

DynamicRowLayout::DynamicRowLayout(bool checkBoundaries, SchemaPtr schema) : DynamicMemoryLayout(){
    this->checkBoundaryFieldChecks = checkBoundaries;
    this->recordSize = schema->getSchemaSizeInBytes();
    this->fieldSizesOffSets = std::make_shared<std::vector<FIELD_SIZE>>();

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    uint64_t offsetCounter = 0;
    for (auto const& field : schema->fields) {
        auto curFieldSize = physicalDataTypeFactory.getPhysicalType(field->getDataType())->size();
        fieldSizesOffSets->emplace_back(offsetCounter);
        offsetCounter += curFieldSize;
    }
}

/**
 *
 * @param schema
 * @param bufferSize
 * @return
 */
DynamicRowLayoutPtr DynamicRowLayout::create(SchemaPtr schema, bool checkBoundaries) {

    return std::make_shared<DynamicRowLayout>(checkBoundaries, schema);
}

std::unique_ptr<DynamicLayoutBuffer> DynamicRowLayout::map(TupleBuffer& tupleBuffer) {

    uint64_t capacity = tupleBuffer.getBufferSize() / recordSize;
    return std::make_unique<DynamicRowLayoutBuffer>(std::make_shared<TupleBuffer>(tupleBuffer), capacity, std::make_shared<DynamicRowLayout>(*this));
}


DynamicMemoryLayoutPtr DynamicRowLayout::copy() const { return std::make_shared<DynamicRowLayout>(*this); }
}