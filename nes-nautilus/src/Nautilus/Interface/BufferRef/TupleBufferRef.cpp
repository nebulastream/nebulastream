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

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/VariableSizedAccessRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

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
void copyVarSizedAndIncrementMetaData(
    TupleBuffer& childBuffer, const VariableSizedAccess::Offset childBufferOffset, const std::span<const std::byte> varSizedValue)
{
    const auto spaceInChildBuffer = childBuffer.getAvailableMemoryArea().subspan(childBufferOffset.getRawOffset());
    PRECONDITION(spaceInChildBuffer.size() >= varSizedValue.size(), "SpaceInChildBuffer must be larger than varSizedValue");
    std::ranges::copy(varSizedValue, spaceInChildBuffer.begin());

    /// We increment the number of tuples by the size of the newly added varsized to store the used no. bytes in the tuple buffer.
    /// We plan on getting rid of this "mis"-use in the near future.
    childBuffer.setNumberOfTuples(childBuffer.getNumberOfTuples() + varSizedValue.size());
}
}

VariableSizedAccess TupleBufferRef::writeVarSized(
    TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const std::span<const std::byte> varSizedValue)
{
    const auto totalVarSizedLength = varSizedValue.size();


    /// If there are no child buffers, we get a new buffer and copy the var sized into the newly acquired
    const auto numberOfChildBuffers = tupleBuffer.getNumberOfChildBuffers();
    if (numberOfChildBuffers == 0)
    {
        auto newChildBuffer = getNewBufferForVarSized(bufferProvider, totalVarSizedLength);
        copyVarSizedAndIncrementMetaData(newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue);
        const auto childBufferIndex = tupleBuffer.storeChildBuffer(newChildBuffer);
        return VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{totalVarSizedLength}};
    }

    /// If there is no space in the lastChildBuffer, we get a new buffer and copy the var sized into the newly acquired
    /// We store the number of used bytes in the no. tuples field.  We plan on getting rid of this "mis"-use in the near future.
    const VariableSizedAccess::Index childIndex{numberOfChildBuffers - 1};
    auto lastChildBuffer = tupleBuffer.loadChildBuffer(childIndex);
    const auto usedMemorySize = lastChildBuffer.getNumberOfTuples();
    if (usedMemorySize + totalVarSizedLength >= lastChildBuffer.getBufferSize())
    {
        auto newChildBuffer = getNewBufferForVarSized(bufferProvider, totalVarSizedLength);
        copyVarSizedAndIncrementMetaData(newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue);
        const VariableSizedAccess::Index childBufferIndex{tupleBuffer.storeChildBuffer(newChildBuffer)};
        return VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{totalVarSizedLength}};
    }

    /// There is enough space in the lastChildBuffer, thus, we copy the var sized into it
    const VariableSizedAccess::Offset childOffset{usedMemorySize};
    copyVarSizedAndIncrementMetaData(lastChildBuffer, childOffset, varSizedValue);
    return VariableSizedAccess{childIndex, childOffset, VariableSizedAccess::Size{totalVarSizedLength}};
}

std::span<std::byte>
TupleBufferRef::loadAssociatedVarSizedValue(const TupleBuffer& tupleBuffer, const VariableSizedAccess variableSizedAccess)
{
    /// Loading the childbuffer containing the variable sized data.
    auto childBuffer = tupleBuffer.loadChildBuffer(variableSizedAccess.getIndex());

    /// Creating a subspan that starts at the required offset. It still can contain multiple other var sized, as we have solely offset the
    /// lower bound but not the upper bound.
    const auto varSized = childBuffer.getAvailableMemoryArea().subspan(variableSizedAccess.getOffset().getRawOffset());

    /// Reading the first 32-bit (size of var sized) and then cutting the span to only contain the required var sized
    alignas(uint32_t) std::array<std::byte, sizeof(uint32_t)> varSizedLengthBuffer{};
    std::ranges::copy(varSized.first<sizeof(uint32_t)>(), varSizedLengthBuffer.begin());
    const auto varSizedLength = std::bit_cast<uint32_t>(varSizedLengthBuffer);
    return varSized.subspan(0, varSizedLength + sizeof(uint32_t));
}

VarVal
TupleBufferRef::loadValue(const DataType& physicalType, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    if (physicalType.type != DataType::Type::VARSIZED)
    {
        return VarVal::readVarValFromMemory(fieldReference, physicalType.type);
    }

    auto combinedIndexOffset = readValueFromMemRef<uint64_t>(fieldReference);
    const nautilus::val<uint64_t> size{getMemberWithOffset<uint64_t>(fieldReference, offsetof(VariableSizedAccess::CombinedIndex, size))};
    const auto varSizedPtr = invoke(
        +[](const TupleBuffer* tupleBuffer, uint64_t combinedIndexOffset, uint64_t size)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            const VariableSizedAccess access(VariableSizedAccess::CombinedIndex{combinedIndexOffset, size});
            return loadAssociatedVarSizedValue(*tupleBuffer, access).data();
        },
        recordBuffer.getReference(),
        combinedIndexOffset,
        size);
    return VariableSizedData(varSizedPtr, size);
}

VarVal TupleBufferRef::storeValue(
    const DataType& physicalType,
    const RecordBuffer& recordBuffer,
    const nautilus::val<int8_t*>& fieldReference,
    VarVal value,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    if (physicalType.type != DataType::Type::VARSIZED)
    {
        /// We might have to cast the value to the correct type, e.g. VarVal could be a INT8 but the type we have to write is of type INT16
        /// We get the correct function to call via a unordered_map
        if (const auto storeFunction = storeValueFunctionMap.find(physicalType.type); storeFunction != storeValueFunctionMap.end())
        {
            return storeFunction->second(value, fieldReference);
        }
        throw UnknownDataType("Physical Type: {} is currently not supported", physicalType);
    }

    const auto varSizedValue = value.cast<VariableSizedData>();
    auto refToIndex = static_cast<nautilus::val<uint64_t*>>(fieldReference);
    auto refToSize = refToIndex + offsetof(VariableSizedAccess::CombinedIndex, size) / 8;

    invoke(
        +[](TupleBuffer* tupleBuffer,
            AbstractBufferProvider* bufferProvider,
            const int8_t* varSizedPtr,
            const uint32_t varSizedValueLength,
            uint64_t* refToIndex,
            uint64_t* refToSize)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            INVARIANT(bufferProvider != nullptr, "BufferProvider MUST NOT be null at this point");
            const std::span varSizedValueSpan{varSizedPtr, varSizedPtr + varSizedValueLength};
            const VariableSizedAccess writtenAccess
                = writeVarSized(*tupleBuffer, *bufferProvider, std::as_bytes(varSizedValueSpan));
            *refToIndex = writtenAccess.getCombinedIdxOffset().index;
            *refToSize = writtenAccess.getCombinedIdxOffset().size;
        },
        recordBuffer.getReference(),
        bufferProvider,
        varSizedValue.getReference(),
        varSizedValue.getTotalSize(),
        nautilus::val(refToIndex),
        nautilus::val(refToSize));

    return value;
}

bool TupleBufferRef::includesField(
    const std::vector<Record::RecordFieldIdentifier>& projections, const Record::RecordFieldIdentifier& fieldIndex)
{
    return std::ranges::find(projections, fieldIndex) != projections.end();
}

uint64_t TupleBufferRef::getCapacity() const
{
    return capacity;
}

uint64_t TupleBufferRef::getBufferSize() const
{
    return bufferSize;
}

uint64_t TupleBufferRef::getTupleSize() const
{
    return tupleSize;
}

TupleBufferRef::TupleBufferRef(const uint64_t capacity, const uint64_t bufferSize, const uint64_t tupleSize)
    : capacity(capacity), bufferSize(bufferSize), tupleSize(tupleSize)
{
}

TupleBufferRef::~TupleBufferRef() = default;
}
