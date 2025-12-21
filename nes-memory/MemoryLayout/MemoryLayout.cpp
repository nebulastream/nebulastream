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
#include <MemoryLayout/MemoryLayout.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <MemoryLayout/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

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
}
