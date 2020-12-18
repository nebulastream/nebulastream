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

#include <NodeEngine/MemoryLayout/DynamicColumnLayout.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES {

DynamicColumnLayout::DynamicColumnLayout(uint64_t capacity, bool checkBoundaries, SchemaPtr schema) : DynamicMemoryLayout() {
    this->capacity = capacity;
    this->numberOfRecords = 0;
    this->checkBoundaryFieldChecks = checkBoundaries;

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto const& field : schema->fields) {
        auto curFieldSize = physicalDataTypeFactory.getPhysicalType(field->getDataType())->size();
        this->fieldSizes.emplace_back(curFieldSize);
        this->columnOffsets.emplace_back(curFieldSize * capacity);
    }
}

/**
 *
 * @param schema
 * @param bufferSize
 * @return
 */
DynamicColumnLayoutPtr DynamicColumnLayout::create(SchemaPtr schema, uint64_t bufferSize, bool checkBoundaries) {

    auto recordSize = schema->getSchemaSizeInBytes();
    auto capacity = bufferSize / recordSize;

    return std::make_shared<DynamicColumnLayout>(capacity, checkBoundaries, schema);
}

uint64_t DynamicColumnLayout::calcOffset(uint64_t ithRecord, uint64_t jthField) {
    NES_VERIFY(jthField < fieldSizes.size(), "jthField" << jthField << " is larger than fieldSizes.size() " << fieldSizes.size());
    NES_VERIFY(jthField < columnOffsets.size(), "jthField" << jthField << " is larger than columnOffsets.size() " << columnOffsets.size());

    return (ithRecord * fieldSizes[jthField]) + columnOffsets[jthField];
}

DynamicMemoryLayoutPtr DynamicColumnLayout::copy() const { return std::make_shared<DynamicColumnLayout>(*this); }
}