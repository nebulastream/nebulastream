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
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>

namespace NES::Runtime::MemoryLayouts {

uint64_t MemoryLayout::getRecordSize() const { return recordSize; }

const std::vector<FIELD_SIZE>& MemoryLayout::getFieldSizes() const { return physicalFieldSizes; }

MemoryLayout::MemoryLayout(uint64_t bufferSize, const SchemaPtr& schema) : bufferSize(bufferSize), schema(schema), recordSize(0) {
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (size_t fieldIndex = 0; fieldIndex < schema->fields.size(); fieldIndex++) {
        auto field = schema->fields[fieldIndex];
        auto physicalFieldSize = physicalDataTypeFactory.getPhysicalType(field->getDataType())->size();
        physicalFieldSizes.emplace_back(physicalFieldSize);
        recordSize += physicalFieldSize;
        nameFieldIndexMap[field->getName()] = fieldIndex;
    }

    capacity = bufferSize / recordSize;
}

std::optional<uint64_t> MemoryLayout::getFieldIndexFromName(const std::string& fieldName) const {
    auto nameFieldIt = nameFieldIndexMap.find(fieldName);
    if (nameFieldIt == nameFieldIndexMap.end()) {
        return std::nullopt;
    }
    return std::optional<uint64_t>(nameFieldIt->second);
}
}// namespace NES::Runtime::MemoryLayouts
