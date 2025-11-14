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
template <TupleBufferRef::PrependMode PrependMode>
void copyVarSizedAndIncrementMetaData(
    TupleBuffer& childBuffer, const VariableSizedAccess::Offset childBufferOffset, const std::span<const std::byte> varSizedValue)
{
    const uint32_t prependSize = (PrependMode == TupleBufferRef::PREPEND_LENGTH_AS_UINT32) ? sizeof(uint32_t) : 0;
    const auto spaceInChildBuffer = childBuffer.getAvailableMemoryArea().subspan(childBufferOffset.getRawOffset());
    PRECONDITION(spaceInChildBuffer.size() >= varSizedValue.size(), "SpaceInChildBuffer must be larger than varSizedValue");
    if constexpr (PrependMode == TupleBufferRef::PREPEND_LENGTH_AS_UINT32)
    {
        uint32_t varSizedLength = varSizedValue.size();
        const auto varSizedLengthBytes = std::as_bytes<uint32_t>(std::span{&varSizedLength, 1});
        std::ranges::copy(varSizedLengthBytes, spaceInChildBuffer.begin());
        std::ranges::copy(varSizedValue, spaceInChildBuffer.begin() + varSizedLengthBytes.size());
    }
    else if constexpr (PrependMode == TupleBufferRef::PREPEND_NONE)
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

template <TupleBufferRef::PrependMode PrependMode>
VariableSizedAccess TupleBufferRef::writeVarSized(
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
        return VariableSizedAccess{childBufferIndex};
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
        return VariableSizedAccess{childBufferIndex};
    }

    /// There is enough space in the lastChildBuffer, thus, we copy the var sized into it
    const VariableSizedAccess::Offset childOffset{usedMemorySize};
    copyVarSizedAndIncrementMetaData<PrependMode>(lastChildBuffer, childOffset, varSizedValue);
    return VariableSizedAccess{childIndex, childOffset};
}

/// Explicit instantiations for writeVarSized()
template VariableSizedAccess
TupleBufferRef::writeVarSized<TupleBufferRef::PrependMode::PREPEND_NONE>(TupleBuffer&, AbstractBufferProvider&, std::span<const std::byte>);
template VariableSizedAccess TupleBufferRef::writeVarSized<TupleBufferRef::PrependMode::PREPEND_LENGTH_AS_UINT32>(
    TupleBuffer&, AbstractBufferProvider&, std::span<const std::byte>);

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

std::string TupleBufferRef::readVarSizedDataAsString(const TupleBuffer& tupleBuffer, const VariableSizedAccess variableSizedAccess)
{
    /// Getting the pointer to the @class VariableSizedData with the first 32-bit storing the size.
    const auto strWithSize = loadAssociatedVarSizedValue(tupleBuffer, variableSizedAccess);
    const auto stringSize = strWithSize.size() - sizeof(uint32_t);
    const auto* const strPtrContent = reinterpret_cast<const char*>(strWithSize.subspan(sizeof(uint32_t), stringSize).data());
    INVARIANT(
        strWithSize.size() >= stringSize, "Parsed varSized {} must NOT be larger than the span size {} ", stringSize, strWithSize.size());
    return std::string{strPtrContent, stringSize};
}

VarVal
TupleBufferRef::loadValue(const DataType& physicalType, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    /// For now, we store the null byte before the actual VarVal
    nautilus::val<bool> null = false;
    nautilus::val<int8_t*> varValRef = fieldReference;
    if (physicalType.isNullable)
    {
        /// Reading the first byte (null) and then incrementing the memref by 1 byte to read the actual value
        null = readValueFromMemRef<bool>(fieldReference);
        varValRef += 1;
    }

    if (physicalType.type != DataType::Type::VARSIZED)
    {
        return VarVal::readVarValFromMemory(varValRef, physicalType, null);
    }
    const nautilus::val<VariableSizedAccess> combinedIdxOffset{readValueFromMemRef<VariableSizedAccess::CombinedIndex>(varValRef)};
    const auto varSizedPtr = invoke(
        +[](const TupleBuffer* tupleBuffer, const VariableSizedAccess variableSizedAccess)
        {
            INVARIANT(tupleBuffer != nullptr, "TupleBuffer MUST NOT be null at this point");
            return loadAssociatedVarSizedValue(*tupleBuffer, variableSizedAccess).data();
        },
        recordBuffer.getReference(),
        combinedIdxOffset);
    const auto isNullable = physicalType.isNullable ? VarVal::NULLABLE_ENUM::NULLABLE : VarVal::NULLABLE_ENUM::NOT_NULLABLE;
    return VarVal{VariableSizedData(varSizedPtr), isNullable, null};
}

VarVal TupleBufferRef::storeValue(
    const DataType& physicalType,
    const RecordBuffer& recordBuffer,
    const nautilus::val<int8_t*>& fieldReference,
    VarVal value,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    /// For now, we store the null byte before the actual VarVal
    nautilus::val<int8_t*> varValRef = fieldReference;
    if (physicalType.isNullable)
    {
        /// Writing the null value to the first byte and then incrementing the memref by 1 byte to store the actual value
        VarVal{value.isNull()}.writeToMemory(varValRef);
        varValRef += 1;
    }
    if (physicalType.type != DataType::Type::VARSIZED)
    {
        /// We might have to cast the value to the correct type, e.g. VarVal could be a INT8 but the type we have to write is of type INT16
        /// We get the correct function to call via a unordered_map
        if (const auto storeFunction = storeValueFunctionMap.find(physicalType.type); storeFunction != storeValueFunctionMap.end())
        {
            return storeFunction->second(value, varValRef);
        }
        throw UnknownDataType("Physical Type: {} is currently not supported", physicalType);
    }

    const auto varSizedValue = value.cast<VariableSizedData>();
    const auto variableSizedAccess = invoke(
        +[](TupleBuffer* tupleBuffer, AbstractBufferProvider* bufferProvider, const int8_t* varSizedPtr, const uint32_t varSizedValueLength)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            INVARIANT(bufferProvider != nullptr, "BufferProvider MUST NOT be null at this point");
            const std::span varSizedValueSpan{varSizedPtr, varSizedPtr + varSizedValueLength};
            return writeVarSized<PREPEND_NONE>(*tupleBuffer, *bufferProvider, std::as_bytes(varSizedValueSpan));
        },
        recordBuffer.getReference(),
        bufferProvider,
        varSizedValue.getReference(),
        varSizedValue.getTotalSize());
    auto fieldReferenceCastedU64 = static_cast<nautilus::val<uint64_t*>>(varValRef);
    *fieldReferenceCastedU64 = variableSizedAccess.convertToValue();
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
