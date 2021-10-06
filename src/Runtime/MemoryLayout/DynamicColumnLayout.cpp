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
#include <Runtime/MemoryLayout/DynamicColumnLayout.hpp>
#include <Runtime/MemoryLayout/DynamicColumnLayoutBuffer.hpp>

namespace NES::Runtime::DynamicMemoryLayout {

DynamicColumnLayout::DynamicColumnLayout(bool checkBoundaries, const SchemaPtr& schema) {
    this->checkBoundaryFieldChecks = checkBoundaries;
    this->recordSize = schema->getSchemaSizeInBytes();
    this->fieldSizes = std::vector<FIELD_SIZE>();
    this->nameFieldIndexMap = std::map<std::string, uint64_t>();

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (size_t fieldIndex = 0; fieldIndex < schema->fields.size(); ++fieldIndex) {
        auto const& field = schema->fields[fieldIndex];
        auto curFieldSize = physicalDataTypeFactory.getPhysicalType(field->getDataType())->size();
        fieldSizes.emplace_back(curFieldSize);
        nameFieldIndexMap[field->getName()] = fieldIndex;
    }
}

/**
 *
 * @param schema
 * @param bufferSize
 * @return
 */
DynamicColumnLayoutPtr DynamicColumnLayout::create(const SchemaPtr& schema, bool checkBoundaries) {

    return std::make_shared<DynamicColumnLayout>(checkBoundaries, schema);
}

DynamicMemoryLayoutPtr DynamicColumnLayout::copy() const { return std::make_shared<DynamicColumnLayout>(*this); }

DynamicColumnLayoutBufferPtr DynamicColumnLayout::bind(const TupleBuffer& tupleBuffer) {
    std::vector<COL_OFFSET_SIZE> columnOffsets;

    uint64_t capacity = tupleBuffer.getBufferSize() / recordSize;
    uint64_t offsetCounter = 0;
    for (auto& fieldSize : fieldSizes) {
        columnOffsets.emplace_back(offsetCounter);
        offsetCounter += fieldSize * capacity;
    }
    return std::make_shared<DynamicColumnLayoutBuffer>(tupleBuffer, capacity, this->shared_from_this(), columnOffsets);
}
}// namespace NES::Runtime::DynamicMemoryLayout