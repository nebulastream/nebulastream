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
#include <Runtime/MemoryLayout/DynamicRowLayout.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::DynamicMemoryLayout {

DynamicRowLayout::DynamicRowLayout(bool checkBoundaries, const SchemaPtr& schema) {
    this->checkBoundaryFieldChecks = checkBoundaries;
    this->recordSize = schema->getSchemaSizeInBytes();
    this->fieldOffSets = std::vector<FIELD_OFFSET>();
    this->fieldSizes = std::vector<FIELD_SIZE>();
    this->nameFieldIndexMap = std::map<std::string, uint64_t>();

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    uint64_t offsetCounter = 0;
    for (size_t fieldIndex = 0; fieldIndex < schema->fields.size(); ++fieldIndex) {
        auto const& field = schema->fields[fieldIndex];
        auto curFieldSize = physicalDataTypeFactory.getPhysicalType(field->getDataType())->size();
        fieldSizes.emplace_back(curFieldSize);
        fieldOffSets.emplace_back(offsetCounter);
        offsetCounter += curFieldSize;
        nameFieldIndexMap[field->getName()] = fieldIndex;
    }
}

/**
 *
 * @param schema
 * @param bufferSize
 * @return
 */
DynamicRowLayoutPtr DynamicRowLayout::create(const SchemaPtr& schema, bool checkBoundaries) {

    return std::make_shared<DynamicRowLayout>(checkBoundaries, schema);
}

DynamicRowLayoutBufferPtr DynamicRowLayout::bind(const TupleBuffer& tupleBuffer) {

    uint64_t capacity = tupleBuffer.getBufferSize() / recordSize;
    return std::make_shared<DynamicRowLayoutBuffer>(tupleBuffer, capacity, this->shared_from_this());
}

DynamicMemoryLayoutPtr DynamicRowLayout::copy() const { return std::make_shared<DynamicRowLayout>(*this); }
const std::vector<FIELD_SIZE>& DynamicRowLayout::getFieldOffSets() const { return fieldOffSets; }

}// namespace NES::Runtime::DynamicMemoryLayout