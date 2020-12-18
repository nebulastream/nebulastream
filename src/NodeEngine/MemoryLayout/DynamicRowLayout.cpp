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

namespace NES {
/**
 *
 * @param schema
 * @param bufferSize
 * @return
 */
DynamicRowLayoutPtr DynamicRowLayout::create(SchemaPtr schema, uint64_t bufferSize, bool checkBoundaries) {

    auto recordSize = schema->getSchemaSizeInBytes();
    auto capacity = bufferSize / recordSize;

    return std::make_shared<DynamicRowLayout>(recordSize, capacity, checkBoundaries, schema);
}

DynamicMemoryLayoutPtr DynamicRowLayout::copy() const { return std::make_shared<DynamicRowLayout>(*this); }

DynamicRowLayout::DynamicRowLayout(uint64_t recordSize, uint64_t capacity, bool checkBoundaries, SchemaPtr schema) {
    this->recordSize = recordSize;
    this->numberOfRecords = 0;
    this->capacity = capacity;
    this->checkBoundaryFieldChecks = checkBoundaries;

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto const& field : schema->fields) {
        fieldSizes.emplace_back(physicalDataTypeFactory.getPhysicalType(field->getDataType())->size());
    }
}
uint64_t DynamicRowLayout::calcOffset(uint64_t ithRecord, uint64_t jthField) {
    NES_VERIFY(jthField < fieldSizes.size(), "jthField" << jthField << " is larger than fieldSizes.size() " << fieldSizes.size());

    return (ithRecord * recordSize) + fieldSizes[jthField];
}
}