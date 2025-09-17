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
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
TupleBuffer getNewBufferForVarSized(AbstractBufferProvider& tupleBufferProvider, const uint32_t newBufferSize)
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
    if (not unpooledBuffer.has_value())
    {
        throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size {}", newBufferSize);
    }

    return unpooledBuffer.value();
}

enum PrependMode
{
    PREPEND_NONE,
    PREPEND_LENGTH_AS_UINT32
};

/// @brief Copies the varsized to the specified location and then increments the number of tuples and used memory
/// @return the new childBufferOffset
template <PrependMode prependMode>
void copyVarSizedAndIncrementMetaData(
    TupleBuffer& childBuffer,
    const VariableSizedAccess::Offset childBufferOffset,
    const char* varSizedValue,
    const uint32_t varSizedValueLength)
{
    constexpr uint32_t prependSize = (prependMode == PREPEND_LENGTH_AS_UINT32) ? sizeof(uint32_t) : 0;
    auto* const dstAddress
        = childBuffer.getMemArea<int8_t>() + childBufferOffset; /// NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    PRECONDITION(
        dstAddress + varSizedValueLength + prependSize < childBuffer.getMemArea() + childBuffer.getBufferSize(),
        "Address {} must stay in the memory are of the tuple buffer {}",
        fmt::ptr(dstAddress + varSizedValueLength),
        fmt::ptr(childBuffer.getMemArea() + childBuffer.getBufferSize()));
    const auto* const srcAddress = varSizedValue;
    std::memcpy(dstAddress, &varSizedValueLength, prependSize);
    std::memcpy(dstAddress + prependSize, srcAddress, varSizedValueLength);
    childBuffer.setNumberOfTuples(childBuffer.getNumberOfTuples() + varSizedValueLength + prependSize);
}

template <PrependMode prependMode>
VariableSizedAccess writeVarSized(TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const std::string_view varSizedValue)
{
    constexpr uint32_t prependSize = (prependMode == PREPEND_LENGTH_AS_UINT32) ? sizeof(uint32_t) : 0;
    const auto totalVarSizedLength = varSizedValue.length() + prependSize;


    /// If there are no child buffers, we get a new buffer and copy the var sized into the newly acquired
    const auto numberOfChildBuffers = tupleBuffer.getNumberOfChildBuffers();
    if (numberOfChildBuffers == 0)
    {
        auto newChildBuffer = getNewBufferForVarSized(bufferProvider, totalVarSizedLength);
        copyVarSizedAndIncrementMetaData<prependMode>(
            newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue.data(), varSizedValue.length());
        const auto childBufferIndex = tupleBuffer.storeChildBuffer(newChildBuffer);
        return VariableSizedAccess{childBufferIndex};
    }

    /// If there is no space in the lastChildBuffer, we get a new buffer and copy the var sized into the newly acquired
    const VariableSizedAccess::Index childIndex{numberOfChildBuffers - 1};
    auto lastChildBuffer = tupleBuffer.loadChildBuffer(childIndex);
    const auto usedMemorySize = lastChildBuffer.getNumberOfTuples();
    if (usedMemorySize + totalVarSizedLength >= lastChildBuffer.getBufferSize())
    {
        auto newChildBuffer = getNewBufferForVarSized(bufferProvider, totalVarSizedLength);
        copyVarSizedAndIncrementMetaData<prependMode>(
            newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue.data(), varSizedValue.length());
        const VariableSizedAccess::Index childBufferIndex{tupleBuffer.storeChildBuffer(newChildBuffer)};
        return VariableSizedAccess{childBufferIndex};
    }

    /// There is enough space in the lastChildBuffer, thus, we copy the var sized into it
    const VariableSizedAccess::Offset childOffset{usedMemorySize};
    copyVarSizedAndIncrementMetaData<prependMode>(lastChildBuffer, childOffset, varSizedValue.data(), varSizedValue.length());
    return VariableSizedAccess{childIndex, childOffset};
}
}

VariableSizedAccess MemoryLayout::writeVarSizedDataAndPrependLength(
    TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const std::string_view varSizedValue)
{
    return writeVarSized<PREPEND_LENGTH_AS_UINT32>(tupleBuffer, bufferProvider, varSizedValue);
}

VariableSizedAccess MemoryLayout::writeVarSizedData(
    TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const char* varSizedValue, const uint32_t varSizedValueLength)
{
    return writeVarSized<PREPEND_NONE>(tupleBuffer, bufferProvider, std::string_view(varSizedValue, varSizedValueLength));
}

const int8_t* MemoryLayout::loadAssociatedVarSizedValue(const TupleBuffer& tupleBuffer, const VariableSizedAccess variableSizedAccess)
{
    /// Loading the childbuffer containing the variable sized data
    auto childBuffer = tupleBuffer.loadChildBuffer(variableSizedAccess.getIndex());
    /// We need to jump childOffset bytes to go to the correct pointer for the var sized
    auto* const varSizedPointer = childBuffer.getMemArea<int8_t>() + variableSizedAccess.getOffset();
    return varSizedPointer;
}

std::string MemoryLayout::readVarSizedDataAsString(const TupleBuffer& tupleBuffer, const VariableSizedAccess combinedIdxOffset)
{
    /// Getting the pointer to the @class VariableSizedData with the first 32-bit storing the size.
    const auto* const strWithSizePtr = loadAssociatedVarSizedValue(tupleBuffer, combinedIdxOffset);
    const auto stringSize = *reinterpret_cast<const uint32_t*>(strWithSizePtr);
    const auto* const strPtrContent = reinterpret_cast<const char*>(strWithSizePtr) + sizeof(uint32_t);
    return std::string{strPtrContent, stringSize};
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
