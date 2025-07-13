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

#include <cstdint>
#include <cstring>
#include <optional>
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
    TupleBuffer& childBuffer, const uint64_t childBufferOffset, const int8_t* varSizedValue, const uint32_t varSizedValueLength)
{
    PRECONDITION(
        childBufferOffset < childBuffer.getBufferSize(),
        "Offset {} must be smaller than the buffer size {}",
        childBufferOffset,
        childBuffer.getBufferSize());
    const auto dstAddress = childBuffer.getBuffer<int8_t>() + childBufferOffset;
    const auto srcAddress = varSizedValue;
    std::memcpy(dstAddress, srcAddress, varSizedValueLength);
    childBuffer.setUsedMemorySize(childBuffer.getUsedMemorySize() + varSizedValueLength);
}
}

VariableSizedAccess MemoryLayout::storeAssociatedVarSizedValue(
    TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const int8_t* varSizedValue, const uint32_t varSizedValueLength)
{
    /// If there are no child buffers, we get a new buffer and copy the var sized into the newly acquired
    const auto numberOfChildBuffers = tupleBuffer.getNumberOfChildBuffers();
    VariableSizedAccess variableSizedAccess;
    if (numberOfChildBuffers == 0)
    {
        auto newChildBuffer = getNewBuffer(bufferProvider, varSizedValueLength);
        copyVarSizedAndIncrementMetaData(newChildBuffer, 0, varSizedValue, varSizedValueLength);
        const auto childBufferIndex = tupleBuffer.storeChildBuffer(newChildBuffer);
        return variableSizedAccess.setIndex(childBufferIndex);
    }

    /// If there is no space in the lastChildBuffer, we get a new buffer and copy the var sized into the newly acquired
    auto lastChildBuffer = tupleBuffer.loadChildBuffer(numberOfChildBuffers - 1);
    if (lastChildBuffer.getUsedMemorySize() + varSizedValueLength > lastChildBuffer.getBufferSize())
    {
        auto newChildBuffer = getNewBuffer(bufferProvider, varSizedValueLength);
        copyVarSizedAndIncrementMetaData(newChildBuffer, 0, varSizedValue, varSizedValueLength);
        const auto childBufferIndex = tupleBuffer.storeChildBuffer(newChildBuffer);
        return variableSizedAccess.setIndex(childBufferIndex);
    }

    /// There is enough space in the lastChildBuffer, thus, we copy the var sized into it
    const auto childOffset = lastChildBuffer.getUsedMemorySize();
    copyVarSizedAndIncrementMetaData(lastChildBuffer, childOffset, varSizedValue, varSizedValueLength);
    const auto childBufferIndex = numberOfChildBuffers - 1;
    variableSizedAccess.setIndex(childBufferIndex).setOffset(childOffset);
    return variableSizedAccess;
}

const int8_t* MemoryLayout::loadAssociatedVarSizedValue(const TupleBuffer& tupleBuffer, const VariableSizedAccess variableSizedAccess)
{
    /// Loading the childbuffer containing the variable sized data
    auto childBuffer = tupleBuffer.loadChildBuffer(variableSizedAccess.getIndex());
    /// We need to jump childOffset bytes to go to the correct pointer for the var sized
    const auto varSizedPointer = childBuffer.getBuffer<int8_t>() + variableSizedAccess.getOffset();
    return varSizedPointer;
}

std::string MemoryLayout::readVarSizedDataAsString(const TupleBuffer& tupleBuffer, const VariableSizedAccess combinedIdxOffset)
{
    /// Getting the pointer to the @class VariableSizedData with the first 32-bit storing the size.
    const auto strWithSizePtr = loadAssociatedVarSizedValue(tupleBuffer, combinedIdxOffset);
    const auto stringSize = *reinterpret_cast<const uint32_t*>(strWithSizePtr);
    const auto strPtrContent = reinterpret_cast<const char*>(strWithSizePtr) + sizeof(uint32_t);
    return std::string{strPtrContent, stringSize};
}

VariableSizedAccess
MemoryLayout::writeVarSizedData(TupleBuffer& buffer, const std::string_view value, AbstractBufferProvider& bufferProvider)
{
    std::string str(value.length() + sizeof(uint32_t), '\0');
    *reinterpret_cast<uint32_t*>(str.data()) = value.length();
    std::memcpy(str.data() + sizeof(uint32_t), value.data(), value.length());
    return MemoryLayout::storeAssociatedVarSizedValue(buffer, bufferProvider, reinterpret_cast<const int8_t*>(str.c_str()), str.length());
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
