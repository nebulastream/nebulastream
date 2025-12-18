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

/// @brief Copies the varSizedValue to the specified location and then increments the number of tuples
/// @return the new childBufferOffset
template <MemoryLayout::PrependMode PrependMode>
void copyVarSizedAndIncrementMetaData(
    TupleBuffer& childBuffer, const VariableSizedAccess::Offset childBufferOffset, const std::span<const std::byte> varSizedValue)
{
    const uint32_t prependSize = (PrependMode == MemoryLayout::PREPEND_LENGTH_AS_UINT32) ? sizeof(uint32_t) : 0;
    const auto spaceInChildBuffer = childBuffer.getAvailableMemoryArea().subspan(childBufferOffset.getRawOffset());
    PRECONDITION(spaceInChildBuffer.size() >= varSizedValue.size(), "SpaceInChildBuffer must be larger than varSizedValue");
    if constexpr (PrependMode == MemoryLayout::PREPEND_LENGTH_AS_UINT32)
    {
        uint32_t varSizedLength = varSizedValue.size();
        const auto varSizedLengthBytes = std::as_bytes<uint32_t>(std::span{&varSizedLength, 1});
        std::ranges::copy(varSizedLengthBytes, spaceInChildBuffer.begin());
        std::ranges::copy(varSizedValue, spaceInChildBuffer.begin() + varSizedLengthBytes.size());
    }
    else if constexpr (PrependMode == MemoryLayout::PREPEND_NONE)
    {
        std::ranges::copy(varSizedValue, spaceInChildBuffer.begin());
    }
    else
    {
        throw NotImplemented("prependMode {} is not implemented", magic_enum::enum_name(PrependMode));
    }

    childBuffer.setNumberOfTuples(childBuffer.getNumberOfTuples() + varSizedValue.size() + prependSize);
}
}

template <MemoryLayout::PrependMode PrependMode>
VariableSizedAccess MemoryLayout::writeVarSized(
    TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const std::span<const std::byte> varSizedValue)
{
    constexpr uint32_t prependSize = (PrependMode == PREPEND_LENGTH_AS_UINT32) ? sizeof(uint32_t) : 0;
    const auto totalVarSizedLength = varSizedValue.size() + prependSize;


    /// If there are no child buffers, we get a new buffer and copy the var sized into the newly acquired
    const auto numberOfChildBuffers = tupleBuffer.getNumberOfChildBuffers();
    if (numberOfChildBuffers == 0)
    {
        auto newChildBuffer = getNewBufferForVarSized(bufferProvider, totalVarSizedLength);
        copyVarSizedAndIncrementMetaData<PrependMode>(newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue);
        const auto childBufferIndex = tupleBuffer.storeChildBuffer(newChildBuffer);
        return VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size(totalVarSizedLength)};
    }

    /// If there is no space in the lastChildBuffer, we get a new buffer and copy the var sized into the newly acquired
    const VariableSizedAccess::Index childIndex{numberOfChildBuffers - 1};
    auto lastChildBuffer = tupleBuffer.loadChildBuffer(childIndex);
    const auto usedMemorySize = lastChildBuffer.getNumberOfTuples();
    if (usedMemorySize + totalVarSizedLength >= lastChildBuffer.getBufferSize())
    {
        auto newChildBuffer = getNewBufferForVarSized(bufferProvider, totalVarSizedLength);
        copyVarSizedAndIncrementMetaData<PrependMode>(newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue);
        const VariableSizedAccess::Index childBufferIndex{tupleBuffer.storeChildBuffer(newChildBuffer)};
        return VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size(totalVarSizedLength)};
    }

    /// There is enough space in the lastChildBuffer, thus, we copy the var sized into it
    const VariableSizedAccess::Offset childOffset{usedMemorySize};
    copyVarSizedAndIncrementMetaData<PrependMode>(lastChildBuffer, childOffset, varSizedValue);
    return VariableSizedAccess{childIndex, childOffset, VariableSizedAccess::Size(totalVarSizedLength)};
}

/// Explicit instantiations for writeVarSized()
template VariableSizedAccess
MemoryLayout::writeVarSized<MemoryLayout::PrependMode::PREPEND_NONE>(TupleBuffer&, AbstractBufferProvider&, std::span<const std::byte>);
template VariableSizedAccess MemoryLayout::writeVarSized<MemoryLayout::PrependMode::PREPEND_LENGTH_AS_UINT32>(
    TupleBuffer&, AbstractBufferProvider&, std::span<const std::byte>);

std::span<std::byte>
MemoryLayout::loadAssociatedVarSizedValue(const TupleBuffer& tupleBuffer, const VariableSizedAccess variableSizedAccess)
{
    /// Loading the childbuffer containing the variable sized data.
    auto childBuffer = tupleBuffer.loadChildBuffer(variableSizedAccess.getIndex());

    /// Creating a subspan that starts at the required offset. It still can contain multiple other var sized, as we have solely offset the
    /// lower bound but not the upper bound.
    const auto varSized = childBuffer.getAvailableMemoryArea().subspan(variableSizedAccess.getOffset().getRawOffset());

    return varSized.subspan(0, variableSizedAccess.getSize().getRawSize());
}

std::string MemoryLayout::readVarSizedDataAsString(const TupleBuffer& tupleBuffer, const VariableSizedAccess variableSizedAccess)
{
    const auto str = loadAssociatedVarSizedValue(tupleBuffer, variableSizedAccess);
    const auto stringSize = variableSizedAccess.getSize().getRawSize();
    const auto* const strPtrContent = reinterpret_cast<const char*>(str.subspan(0, stringSize).data());
    INVARIANT(str.size() >= stringSize, "Parsed varSized {} must NOT be larger than the span size {} ", stringSize, str.size());
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
