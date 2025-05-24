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

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Memory::MemoryLayouts
{

std::string readVarSizedData(const Memory::TupleBuffer& buffer, const uint64_t childBufferIdx)
{
    auto childBuffer = buffer.loadChildBuffer(childBufferIdx);
    const auto stringSize = *childBuffer.getBuffer<uint32_t>();
    std::string varSizedData(stringSize, '\0');
    std::memcpy(varSizedData.data(), childBuffer.getBuffer<char>() + sizeof(uint32_t), stringSize);
    return varSizedData;
}

std::optional<uint32_t>
writeVarSizedData(const Memory::TupleBuffer& buffer, const std::string_view value, Memory::AbstractBufferProvider& bufferProvider)
{
    const auto valueLength = value.length();
    auto childBuffer = bufferProvider.getUnpooledBuffer(valueLength + sizeof(uint32_t));
    if (childBuffer.has_value())
    {
        auto& childBufferVal = childBuffer.value();
        *childBufferVal.getBuffer<uint32_t>() = valueLength;
        std::memcpy(childBufferVal.getBuffer<char>() + sizeof(uint32_t), value.data(), valueLength);
        return buffer.storeChildBuffer(childBufferVal);
    }
    return {};
}

uint64_t MemoryLayout::getTupleSize() const
{
    return recordSize;
}

uint64_t MemoryLayout::getFieldSize(const uint64_t fieldIndex) const
{
    return physicalFieldSizes[fieldIndex];
}

MemoryLayout::MemoryLayout(const uint64_t bufferSize, Schema schema) : bufferSize(bufferSize), schema(std::move(schema)), recordSize(0)
{
    for (size_t fieldIndex = 0; fieldIndex < this->schema.getNumberOfFields(); fieldIndex++)
    {
        const auto field = this->schema.getFieldAt(fieldIndex);
        auto physicalFieldSizeInBytes = field.dataType.getSizeInBytes();
        physicalFieldSizes.emplace_back(physicalFieldSizeInBytes);
        physicalTypes.emplace_back(field.dataType);
        recordSize += physicalFieldSizeInBytes;
        nameFieldIndexMap[field.name] = fieldIndex;
    }
    /// calculate the buffer capacity only if the record size is larger then zero
    capacity = recordSize > 0 ? bufferSize / recordSize : 0;
}

std::shared_ptr<MemoryLayout> MemoryLayout::createMemoryLayout(const Schema& schema, const uint64_t bufferSize)
{
    switch (schema.getLayoutType())
    {
        case Schema::MemoryLayoutType::ROW_LAYOUT:
            return std::make_shared<RowLayout>(schema, bufferSize);
        case Schema::MemoryLayoutType::COLUMNAR_LAYOUT:
            return std::make_shared<ColumnLayout>(schema, bufferSize);
        default: {
            NES_FATAL_ERROR("Unknown memory layout type");
            return nullptr;
        }
    }
}

std::optional<uint64_t> MemoryLayout::getFieldIndexFromName(const std::string& fieldName) const
{
    const auto nameFieldIt = nameFieldIndexMap.find(fieldName);
    if (!nameFieldIndexMap.contains(fieldName))
    {
        return std::nullopt;
    }
    return {nameFieldIt->second};
}

uint64_t MemoryLayout::getCapacity() const
{
    return capacity;
}

const Schema& MemoryLayout::getSchema() const
{
    return schema;
}

uint64_t MemoryLayout::getBufferSize() const
{
    return bufferSize;
}

void MemoryLayout::setBufferSize(const uint64_t bufferSize)
{
    MemoryLayout::bufferSize = bufferSize;
    /// As we have changed the bufferSize, we need to re-calculate the capacity
    capacity = recordSize > 0 ? bufferSize / recordSize : 0;
}

DataType MemoryLayout::getPhysicalType(const uint64_t fieldIndex) const
{
    return physicalTypes[fieldIndex];
}

std::vector<std::string> MemoryLayout::getKeyFieldNames() const
{
    return keyFieldNames;
}

void MemoryLayout::setKeyFieldNames(const std::vector<std::string>& keyFields)
{
    for (const auto& field : keyFields)
    {
        keyFieldNames.emplace_back(field);
    }
}

std::vector<std::tuple<MemoryLayout::FieldType, uint64_t>> MemoryLayout::getGroupedFieldTypeSizes() const
{
    std::vector<std::tuple<FieldType, uint64_t>> fieldTypeSizes;
    for (auto fieldIdx = 0UL; fieldIdx < schema.getFieldCount(); ++fieldIdx)
    {
        auto fieldType = FieldType::PAYLOAD;
        if (std::ranges::find(keyFieldNames, schema.getFieldNames()[fieldIdx]) != keyFieldNames.end())
        {
            fieldType = FieldType::KEY;
        }

        uint64_t fieldSize = getFieldSize(fieldIdx);
        if (!fieldTypeSizes.empty())
        {
            if (const auto [lastFieldType, lastFieldSize] = fieldTypeSizes.back(); lastFieldType == fieldType)
            {
                fieldSize += lastFieldSize;
                fieldTypeSizes.pop_back();
            }
        }

        fieldTypeSizes.emplace_back(std::make_tuple(fieldType, fieldSize));
    }

    return fieldTypeSizes;
}

Schema MemoryLayout::createKeyFieldsOnlySchema() const
{
    auto keyFieldsOnlySchema = Schema(schema.getLayoutType());
    for (const auto& keyFieldName : keyFieldNames)
    {
        keyFieldsOnlySchema.addField(keyFieldName, schema.getFieldByName(keyFieldName).value().getDataType());
    }
    return keyFieldsOnlySchema;
}

}
