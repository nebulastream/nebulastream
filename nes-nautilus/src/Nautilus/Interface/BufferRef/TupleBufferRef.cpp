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
#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/VariableSizedAccessRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/std/cstring.h>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

namespace NES
{

namespace
{
TupleBuffer getNewBufferForVarSized(AbstractBufferProvider& tupleBufferProvider, const uint64_t newBufferSize)
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

namespace
{
/// Reserve `totalSize` contiguous bytes for a var-sized value in a child buffer of `tupleBuffer`, record the
/// resulting access in `*refToIndex`, and return a pointer to the (still uninitialised) reserved region for the
/// caller to fill. This is writeVarSized's child-buffer bookkeeping WITHOUT the payload copy: a rope store uses
/// it to write its segments straight into the child buffer, skipping the intermediate arena materialisation.
/// storeChildBuffer stores a refcounted handle to the same underlying memory the returned pointer addresses, so
/// filling the region after this call is safe.
int8_t* reserveVarSizedRegion(
    TupleBuffer* tupleBuffer, AbstractBufferProvider* bufferProvider, const uint64_t totalSize, VariableSizedAccess* refToIndex)
{
    INVARIANT(tupleBuffer != nullptr, "TupleBuffer MUST NOT be null at this point");
    INVARIANT(bufferProvider != nullptr, "BufferProvider MUST NOT be null at this point");

    /// Append to the last child buffer if it has room (mirrors writeVarSized's `>=`-full check).
    const auto numberOfChildBuffers = tupleBuffer->getNumberOfChildBuffers();
    if (numberOfChildBuffers != 0)
    {
        const VariableSizedAccess::Index childIndex{numberOfChildBuffers - 1};
        auto lastChildBuffer = tupleBuffer->loadChildBuffer(childIndex);
        const auto usedMemorySize = lastChildBuffer.getNumberOfTuples();
        if (usedMemorySize + totalSize < lastChildBuffer.getBufferSize())
        {
            const VariableSizedAccess::Offset childOffset{usedMemorySize};
            auto* const destination
                = reinterpret_cast<int8_t*>(lastChildBuffer.getAvailableMemoryArea().subspan(childOffset.getRawOffset()).data());
            lastChildBuffer.setNumberOfTuples(usedMemorySize + totalSize);
            *refToIndex = VariableSizedAccess{childIndex, childOffset, VariableSizedAccess::Size{totalSize}};
            return destination;
        }
    }

    /// No children yet, or the last one is full: acquire a fresh child buffer.
    auto newChildBuffer = getNewBufferForVarSized(*bufferProvider, totalSize);
    auto* const destination = reinterpret_cast<int8_t*>(newChildBuffer.getAvailableMemoryArea().data());
    newChildBuffer.setNumberOfTuples(totalSize);
    const VariableSizedAccess::Index childBufferIndex{tupleBuffer->storeChildBuffer(newChildBuffer)};
    *refToIndex = VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{totalSize}};
    return destination;
}
}

std::span<std::byte>
TupleBufferRef::loadAssociatedVarSizedValue(const TupleBuffer& tupleBuffer, const VariableSizedAccess variableSizedAccess) noexcept
{
    /// Loading the childbuffer containing the variable sized data.
    auto childBuffer = tupleBuffer.loadChildBuffer(variableSizedAccess.getIndex());

    /// Creating a subspan that starts at the required offset. It still can contain multiple other var sized, as we have solely offset the
    /// lower bound but not the upper bound.
    const auto varSized = childBuffer.getAvailableMemoryArea().subspan(variableSizedAccess.getOffset().getRawOffset());

    return varSized.subspan(0, variableSizedAccess.getSize().getRawSize());
}

VarVal
TupleBufferRef::loadValue(const DataType& physicalType, const RecordBuffer& recordBuffer, const nautilus::val<int8_t*>& fieldReference)
{
    /// For now, we store the null byte before the actual VarVal
    nautilus::val<bool> null = false;
    nautilus::val<int8_t*> varValRef = fieldReference;
    if (physicalType.nullable)
    {
        /// Reading the first byte (null) and then incrementing the memref by 1 byte to read the actual value
        null = readValueFromMemRef<bool>(fieldReference);
        varValRef += 1;
    }

    if (physicalType.type != DataType::Type::VARSIZED)
    {
        return VarVal::readVarValFromMemory(varValRef, physicalType, null);
    }

    auto variableSizedAccess = static_cast<nautilus::val<VariableSizedAccess*>>(varValRef);
    const auto varSizedPtr = invoke(
        {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
        +[](const TupleBuffer* tupleBuffer, const VariableSizedAccess* variableSizedAccessPtr)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            INVARIANT(variableSizedAccessPtr != nullptr, "VariableSizedAccess MUST NOT be null at this point");
            return loadAssociatedVarSizedValue(*tupleBuffer, *variableSizedAccessPtr).data();
        },
        recordBuffer.getReference(),
        variableSizedAccess);

    const nautilus::val<uint64_t> size = *getMemberWithOffset<uint64_t>(variableSizedAccess, offsetof(VariableSizedAccess, size));
    return VarVal{VariableSizedData(varSizedPtr, size), physicalType.nullable, null};
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
    if (physicalType.nullable)
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

    auto refToIndex = static_cast<nautilus::val<VariableSizedAccess*>>(varValRef);

    /// A lazy VARSIZED value writes itself straight into the child buffer, run by run, instead of first being
    /// materialised into an arena buffer and then copied in. This removes the redundant arena round-trip at this
    /// pipeline boundary: the payload is copied once (into the child buffer here), then once more by the
    /// downstream formatter -- not three times. writeEachChunk drives the walk and hides whether the value is a
    /// single passthrough span or a many-segment concat rope; a passthrough is simply the one-run case.
    if (value.isLazyValue())
    {
        const auto lazyValue = value.getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
        const auto destination
            = invoke(reserveVarSizedRegion, recordBuffer.getReference(), bufferProvider, lazyValue->getSize(), refToIndex);
        nautilus::val<uint64_t> offset{0};
        lazyValue->writeEachChunk(
            [&](const nautilus::val<int8_t*>& chunkPtr, const nautilus::val<uint64_t>& chunkLen, const bool, const bool)
            {
                nautilus::memcpy(destination + offset, chunkPtr, chunkLen);
                offset += chunkLen;
            });
        return value;
    }

    const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
    invoke(
        +[](TupleBuffer* tupleBuffer,
            AbstractBufferProvider* bufferProvider,
            const int8_t* varSizedPtr,
            const uint64_t varSizedValueLength,
            VariableSizedAccess* refToIndex)
        {
            INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
            INVARIANT(bufferProvider != nullptr, "BufferProvider MUST NOT be null at this point");
            const std::span varSizedValueSpan{varSizedPtr, varSizedPtr + varSizedValueLength};
            const VariableSizedAccess writtenAccess = writeVarSized(*tupleBuffer, *bufferProvider, std::as_bytes(varSizedValueSpan));
            *refToIndex = writtenAccess;
        },
        recordBuffer.getReference(),
        bufferProvider,
        varSizedValue.getContent(),
        varSizedValue.getSize(),
        refToIndex);

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
