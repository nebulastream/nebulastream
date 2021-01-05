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
#include  <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>

namespace NES {

DynamicRowLayout::DynamicRowLayout(bool checkBoundaries, SchemaPtr schema) : DynamicMemoryLayout(){
    this->recordSize = schema->getSchemaSizeInBytes();
    this->checkBoundaryFieldChecks = checkBoundaries;

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto const& field : schema->fields) {
        fieldSizes->emplace_back(physicalDataTypeFactory.getPhysicalType(field->getDataType())->size());
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

DynamicLayoutBuffer DynamicRowLayout::map(TupleBuffer tupleBuffer) {
    DynamicRowLayoutBuffer dynamicRowLayoutBuffer(recordSize, fieldSizes);


    return (DynamicLayoutBuffer) (dynamicRowLayoutBuffer);
}


DynamicMemoryLayoutPtr DynamicRowLayout::copy() const { return std::make_shared<DynamicRowLayout>(*this); }
}