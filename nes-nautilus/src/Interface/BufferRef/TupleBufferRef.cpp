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

#include <Interface/BufferRef/TupleBufferRef.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/RecordLayoutUtil.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Interface/VariableSizedAccessRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/inline.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
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
    if (tupleBufferProvider.getBufferSize() >= newBufferSize)
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
/// @tparam ChildBuffer a TupleBuffer or a TupleBufferView
template <typename ChildBuffer>
void copyVarSizedAndIncrementMetaData(
    ChildBuffer& childBuffer, const VariableSizedAccess::Offset childBufferOffset, const std::span<const std::byte> varSizedValue)
{
    const auto spaceInChildBuffer = childBuffer.getAvailableMemoryArea().subspan(childBufferOffset.getRawOffset());
    PRECONDITION(spaceInChildBuffer.size() >= varSizedValue.size(), "SpaceInChildBuffer must be larger than varSizedValue");
    std::ranges::copy(varSizedValue, spaceInChildBuffer.begin());

    /// We increment the number of tuples by the size of the newly added varsized to store the used no. bytes in the tuple buffer.
    /// We plan on getting rid of this "mis"-use in the near future.
    childBuffer.setNumberOfTuples(childBuffer.getNumberOfTuples() + varSizedValue.size());
}

/// Stays out of line, as it runs only once per child buffer.
[[gnu::noinline]] VariableSizedAccess
writeVarSizedToNewChild(TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const std::span<const std::byte> varSizedValue)
{
    auto newChildBuffer = getNewBufferForVarSized(bufferProvider, varSizedValue.size());
    copyVarSizedAndIncrementMetaData(newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue);
    const auto childBufferIndex = tupleBuffer.storeChildBuffer(newChildBuffer);
    return VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{varSizedValue.size()}};
}
}

NAUTILUS_INLINE VariableSizedAccess TupleBufferRef::writeVarSized(
    TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const std::span<const std::byte> varSizedValue)
{
    const auto totalVarSizedLength = varSizedValue.size();

    /// Usually the last child buffer has space, so we copy the var sized into it.
    /// We store the number of used bytes in the no. tuples field.
    /// We plan on getting rid of this "mis"-use in the near future.
    /// We only view the last child buffer, as taking a reference to it would cost two atomic operations per call.
    if (const auto numberOfChildBuffers = tupleBuffer.getNumberOfChildBuffers(); numberOfChildBuffers > 0)
    {
        const ChildBufferIndex lastChildIndex{numberOfChildBuffers - 1};
        const auto childOffset = tupleBuffer.withChildBuffer(
            lastChildIndex,
            [&](TupleBufferView lastChildBuffer) -> std::optional<VariableSizedAccess::Offset>
            {
                const auto usedMemorySize = lastChildBuffer.getNumberOfTuples();
                if (usedMemorySize + totalVarSizedLength > lastChildBuffer.getBufferSize())
                {
                    return std::nullopt;
                }
                const VariableSizedAccess::Offset offset{usedMemorySize};
                copyVarSizedAndIncrementMetaData(lastChildBuffer, offset, varSizedValue);
                return offset;
            });
        if (childOffset.has_value())
        {
            return VariableSizedAccess{lastChildIndex, *childOffset, VariableSizedAccess::Size{totalVarSizedLength}};
        }
    }

    /// No child buffer, or no space in the last one
    return writeVarSizedToNewChild(tupleBuffer, bufferProvider, varSizedValue);
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

VarSizedLoadFn TupleBufferRef::getRecordBufferLoad(const RecordBuffer& recordBuffer)
{
    /// Capturing the RecordBuffer by value would copy an owned buffer, which costs a retain and a release per call at runtime.
    /// The returned function holds a pointer into the record buffer and must not outlive it.
    return [tupleBuffer
            = recordBuffer.getReference()](const nautilus::val<int8_t*>& slot) -> std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>
    {
        auto variableSizedAccess = static_cast<nautilus::val<VariableSizedAccess*>>(slot);
        const auto varSizedPtr = invoke(
            {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
            +[](const TupleBuffer* tupleBuffer, const VariableSizedAccess* variableSizedAccessPtr)
            {
                INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
                INVARIANT(variableSizedAccessPtr != nullptr, "VariableSizedAccess MUST NOT be null at this point");
                return loadAssociatedVarSizedValue(*tupleBuffer, *variableSizedAccessPtr).data();
            },
            tupleBuffer,
            variableSizedAccess);

        const nautilus::val<uint64_t> size = *getMemberWithOffset<uint64_t>(variableSizedAccess, offsetof(VariableSizedAccess, size));
        return {static_cast<nautilus::val<int8_t*>>(varSizedPtr), size};
    };
}

namespace
{
NAUTILUS_INLINE void writeVarSizedProxy(
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider,
    const int8_t* varSizedPtr,
    const uint64_t varSizedValueLength,
    VariableSizedAccess* refToIndex)
{
    INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
    INVARIANT(bufferProvider != nullptr, "BufferProvider MUST NOT be null at this point");
    const std::span varSizedValueSpan{varSizedPtr, varSizedPtr + varSizedValueLength};
    *refToIndex = TupleBufferRef::writeVarSized(*tupleBuffer, *bufferProvider, std::as_bytes(varSizedValueSpan));
}
}

VarSizedStoreFn
TupleBufferRef::getRecordBufferStore(const RecordBuffer& recordBuffer, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    /// Capturing the RecordBuffer by value would copy an owned buffer, which costs a retain and a release per call at runtime.
    /// The returned function holds a pointer into the record buffer and must not outlive it.
    return [tupleBuffer = recordBuffer.getReference(), bufferProvider](const nautilus::val<int8_t*>& slot, const VarVal& value)
    {
        const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
        auto refToIndex = static_cast<nautilus::val<VariableSizedAccess*>>(slot);

        invoke(writeVarSizedProxy, tupleBuffer, bufferProvider, varSizedValue.getContent(), varSizedValue.getSize(), refToIndex);
    };
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
