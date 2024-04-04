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
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <cstring>
#include <memory>

namespace NES::Runtime::MemoryLayouts {

std::string readVarSizedData(const TupleBuffer& buffer, uint64_t childBufferIdx) {
    auto childBuffer = buffer.loadChildBuffer(childBufferIdx);
    auto stringSize = *childBuffer.getBuffer<uint32_t>();
    std::string varSizedData(stringSize, '\0');
    std::memcpy(varSizedData.data(), childBuffer.getBuffer<char>() + sizeof(uint32_t), stringSize);
    return varSizedData;
}

std::optional<uint32_t> writeVarSizedData(const TupleBuffer& buffer, const std::string_view value, BufferManager& bufferManager) {
    const auto valueLength = value.length();
    auto childBuffer = bufferManager.getUnpooledBuffer(valueLength + sizeof(uint32_t));
    if (childBuffer.has_value()) {
        auto& childBufferVal = childBuffer.value();
        *childBufferVal.getBuffer<uint32_t>() = valueLength;
        std::memcpy(childBufferVal.getBuffer<char>() + sizeof(uint32_t), value.data(), valueLength);
        return buffer.storeChildBuffer(childBufferVal);
    }
    return {};
}

uint64_t MemoryLayout::getTupleSize() const { return recordSize; }

const std::vector<uint64_t>& MemoryLayout::getFieldSizes() const { return physicalFieldSizes; }

MemoryLayout::MemoryLayout(uint64_t bufferSize, SchemaPtr schema) : bufferSize(bufferSize), schema(schema), recordSize(0) {
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (size_t fieldIndex = 0; fieldIndex < schema->fields.size(); fieldIndex++) {
        auto field = schema->fields[fieldIndex];
        auto physicalFieldSize = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        physicalFieldSizes.emplace_back(physicalFieldSize->size());
        physicalTypes.emplace_back(physicalFieldSize);
        recordSize += physicalFieldSize->size();
        nameFieldIndexMap[field->getName()] = fieldIndex;
    }
    // calculate the buffer capacity only if the record size is larger then zero
    capacity = recordSize > 0 ? bufferSize / recordSize : 0;
}

std::optional<uint64_t> MemoryLayout::getFieldIndexFromName(const std::string& fieldName) const {
    auto nameFieldIt = nameFieldIndexMap.find(fieldName);
    if (!nameFieldIndexMap.contains(fieldName)) {
        return std::nullopt;
    }
    return {nameFieldIt->second};
}

std::optional<uint64_t> MemoryLayout::getFieldOffset(uint64_t tupleIndex, std::string_view fieldName) const {
    const auto fieldIndex = getFieldIndexFromName(std::string(fieldName));
    if (fieldIndex.has_value()) {
        const auto fieldIndexValue = fieldIndex.value();
        return getFieldOffset(tupleIndex, fieldIndexValue);
    }
    return {};
}

uint64_t MemoryLayout::getCapacity() const { return capacity; }

const SchemaPtr& MemoryLayout::getSchema() const { return schema; }

uint64_t MemoryLayout::getBufferSize() const { return bufferSize; }

const std::vector<PhysicalTypePtr>& MemoryLayout::getPhysicalTypes() const { return physicalTypes; }

bool MemoryLayout::operator==(const MemoryLayout& rhs) const {
    return bufferSize == rhs.bufferSize && schema->equals(rhs.schema) && recordSize == rhs.recordSize && capacity == rhs.capacity
        && physicalFieldSizes == rhs.physicalFieldSizes && physicalTypes == rhs.physicalTypes
        && nameFieldIndexMap == rhs.nameFieldIndexMap;
}

bool MemoryLayout::operator!=(const MemoryLayout& rhs) const { return !(rhs == *this); }
}// namespace NES::Runtime::MemoryLayouts
