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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
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
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
TupleBuffer getNewBuffer(AbstractBufferProvider& tupleBufferProvider, const uint32_t newBufferSize)
{
    /// If the fixed size buffers are not large enough, we get an unpooled buffer
    if (tupleBufferProvider.getBufferSize() > newBufferSize)
    {
        if (auto newBuffer = tupleBufferProvider.getBufferNoBlocking(); newBuffer.has_value())
        {
            return newBuffer.value();
        }
    }
    const auto unpooledBuffer = tupleBufferProvider.getUnpooledBuffer(newBufferSize);
    INVARIANT(unpooledBuffer.has_value(), "Cannot allocate unpooled buffer of size {}", newBufferSize);
    return unpooledBuffer.value();
}

/// @brief Copies the varsized to the specified location and then increments the number of tuples and used memory
/// @return the new childBufferOffset
void copyVarSizedAndIncrementMetaData(
    TupleBuffer& childBuffer, const uint64_t childBufferOffset, const std::span<const std::byte> varSizedValue)
{
    PRECONDITION(
        childBufferOffset < childBuffer.getBufferSize(),
        "Offset {} must be smaller than the buffer size {}",
        childBufferOffset,
        childBuffer.getBufferSize());
    const auto dstAddress = childBuffer.getAvailableMemoryArea().subspan(childBufferOffset, varSizedValue.size());
    const auto srcAddress = varSizedValue;
    std::ranges::copy(srcAddress, dstAddress.begin());
    childBuffer.setUsedMemorySize(childBuffer.getUsedMemorySize() + varSizedValue.size());
}
}

VariableSizedAccess MemoryLayout::storeAssociatedVarSizedValue(
    TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const std::span<const std::byte> varSizedValue)
{
    /// If there are no child buffers, we get a new buffer and copy the var sized into the newly acquired
    const auto numberOfChildBuffers = tupleBuffer.getNumberOfChildBuffers();
    VariableSizedAccess variableSizedAccess;
    if (numberOfChildBuffers == 0)
    {
        auto newChildBuffer = getNewBuffer(bufferProvider, varSizedValue.size());
        copyVarSizedAndIncrementMetaData(newChildBuffer, 0, varSizedValue);
        const auto childBufferIndex = tupleBuffer.storeChildBuffer(newChildBuffer);
        return variableSizedAccess.setIndex(childBufferIndex);
    }

    /// If there is no space in the lastChildBuffer, we get a new buffer and copy the var sized into the newly acquired
    auto lastChildBuffer = tupleBuffer.loadChildBuffer(numberOfChildBuffers - 1);
    if (lastChildBuffer.getUsedMemorySize() + varSizedValue.size() > lastChildBuffer.getBufferSize())
    {
        auto newChildBuffer = getNewBuffer(bufferProvider, varSizedValue.size());
        copyVarSizedAndIncrementMetaData(newChildBuffer, 0, varSizedValue);
        const auto childBufferIndex = tupleBuffer.storeChildBuffer(newChildBuffer);
        return variableSizedAccess.setIndex(childBufferIndex);
    }

    /// There is enough space in the lastChildBuffer, thus, we copy the var sized into it
    const auto childOffset = lastChildBuffer.getUsedMemorySize();
    copyVarSizedAndIncrementMetaData(lastChildBuffer, childOffset, varSizedValue);
    const auto childBufferIndex = numberOfChildBuffers - 1;
    variableSizedAccess.setIndex(childBufferIndex).setOffset(childOffset);
    return variableSizedAccess;
}

std::span<const std::byte>
MemoryLayout::loadAssociatedVarSizedValue(const TupleBuffer& tupleBuffer, const VariableSizedAccess variableSizedAccess)
{
    /// Loading the childbuffer containing the variable sized data
    auto childBuffer = tupleBuffer.loadChildBuffer(variableSizedAccess.getIndex());
    /// We need to jump childOffset bytes to go to the correct pointer for the var sized
    const auto offset = variableSizedAccess.getOffset();
    const auto varSizedSpan = childBuffer.getAvailableMemoryArea();
    const auto varSizedLength = *reinterpret_cast<uint32_t*>(varSizedSpan.subspan(offset).data());
    return varSizedSpan.subspan(offset, varSizedLength + sizeof(uint32_t));
}

VariableSizedAccess
MemoryLayout::writeVarSizedData(TupleBuffer& buffer, const std::span<const std::byte> varSizedValue, AbstractBufferProvider& bufferProvider)
{
    std::vector<std::byte> varSizedValuePlusItsSize{varSizedValue.size() + sizeof(uint32_t)};
    *reinterpret_cast<uint32_t*>(varSizedValuePlusItsSize.data()) = varSizedValue.size();
    std::ranges::copy(varSizedValue, varSizedValuePlusItsSize.begin() + sizeof(uint32_t));
    return MemoryLayout::storeAssociatedVarSizedValue(buffer, bufferProvider, varSizedValuePlusItsSize);
}

std::span<const std::byte> MemoryLayout::readVarSizedValue(const TupleBuffer& tupleBuffer, const VariableSizedAccess variableSizedAccess)
{
    const auto spanWithSize = MemoryLayout::loadAssociatedVarSizedValue(tupleBuffer, variableSizedAccess);
    return spanWithSize.subspan(sizeof(uint32_t), spanWithSize.size() - sizeof(uint32_t));
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

uint64_t MemoryLayout::getNumberOfTuples(const uint64_t usedMemorySize) const
{
    return usedMemorySize / getTupleSize();
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
