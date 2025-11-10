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

VariableSizedAccess TupleBufferRef::writeVarSizedDataAndPrependLength(
    TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const std::string_view varSizedValue)
{
    return writeVarSized<PREPEND_LENGTH_AS_UINT32>(tupleBuffer, bufferProvider, varSizedValue);
}

VariableSizedAccess TupleBufferRef::writeVarSizedData(
    TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const char* varSizedValue, const uint32_t varSizedValueLength)
{
    return writeVarSized<PREPEND_NONE>(tupleBuffer, bufferProvider, std::string_view(varSizedValue, varSizedValueLength));
}

const int8_t* TupleBufferRef::loadAssociatedVarSizedValue(const TupleBuffer& tupleBuffer, const VariableSizedAccess variableSizedAccess)
{
    /// Loading the childbuffer containing the variable sized data
    auto childBuffer = tupleBuffer.loadChildBuffer(variableSizedAccess.getIndex());
    /// We need to jump childOffset bytes to go to the correct pointer for the var sized
    auto* const varSizedPointer = childBuffer.getMemArea<int8_t>() + variableSizedAccess.getOffset();
    return varSizedPointer;
}

std::string TupleBufferRef::readVarSizedDataAsString(const TupleBuffer& tupleBuffer, const VariableSizedAccess combinedIdxOffset)
{
    /// Getting the pointer to the @class VariableSizedData with the first 32-bit storing the size.
    const auto* const strWithSizePtr = loadAssociatedVarSizedValue(tupleBuffer, combinedIdxOffset);
    const auto stringSize = *reinterpret_cast<const uint32_t*>(strWithSizePtr);
    const auto* const strPtrContent = reinterpret_cast<const char*>(strWithSizePtr) + sizeof(uint32_t);
    return std::string{strPtrContent, stringSize};
}

VarVal
TupleBufferRef::loadValue(const DataType& physicalType, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    if (physicalType.type != DataType::Type::VARSIZED)
    {
        return VarVal::readVarValFromMemory(fieldReference, physicalType.type);
    }
    nautilus::val<VariableSizedAccess> combinedIdxOffset{readValueFromMemRef<VariableSizedAccess::CombinedIndex>(fieldReference)};
    const auto varSizedPtr = invoke(
        +[](const TupleBuffer* tupleBuffer, const VariableSizedAccess variableSizedAccess)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            return TupleBufferRef::loadAssociatedVarSizedValue(*tupleBuffer, variableSizedAccess);
        },
        recordBuffer.getReference(),
        combinedIdxOffset);
    return VariableSizedData(varSizedPtr);
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
    const auto variableSizedAccess = invoke(
        +[](TupleBuffer* tupleBuffer, AbstractBufferProvider* bufferProvider, const int8_t* varSizedPtr, const uint32_t varSizedValueLength)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            INVARIANT(bufferProvider != nullptr, "BufferProvider MUST NOT be null at this point");
            return TupleBufferRef::writeVarSizedData(*tupleBuffer, *bufferProvider, varSizedPtr, varSizedValueLength);
        },
        recordBuffer.getReference(),
        bufferProvider,
        varSizedValue.getReference(),
        varSizedValue.getTotalSize());
    auto fieldReferenceCastedU64 = static_cast<nautilus::val<uint64_t*>>(fieldReference);
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
