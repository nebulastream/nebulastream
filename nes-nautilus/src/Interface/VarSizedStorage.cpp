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
#include <Interface/VarSizedStorage.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <utility>
#include <DataTypes/DataTypesUtil.hpp>
#include <Interface/BTree/BTree.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// Copies the varSizedValue to the specified location and then increments the number of tuples
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

VariableSizedAccess
writeVarSized(TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const std::span<const std::byte> varSizedValue)
{
    const auto totalVarSizedLength = varSizedValue.size();

    /// If there are no child buffers, we get a new buffer and copy the var sized into the newly acquired
    const auto numberOfChildBuffers = tupleBuffer.getNumberOfChildBuffers();
    if (numberOfChildBuffers == 0)
    {
        auto newChildBuffer = getBufferOfAtLeast(totalVarSizedLength, bufferProvider);
        copyVarSizedAndIncrementMetaData(newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue);
        const auto childBufferIndex = tupleBuffer.storeChildBuffer(newChildBuffer);
        return VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{totalVarSizedLength}};
    }

    /// Otherwise append to the last child buffer, falling back to a new one once it cannot hold the value
    const ChildBufferIndex childIndex{numberOfChildBuffers - 1};
    auto lastChildBuffer = tupleBuffer.loadChildBuffer(childIndex);
    const auto usedMemorySize = lastChildBuffer.getNumberOfTuples();
    if (usedMemorySize + totalVarSizedLength >= lastChildBuffer.getBufferSize())
    {
        auto newChildBuffer = getBufferOfAtLeast(totalVarSizedLength, bufferProvider);
        copyVarSizedAndIncrementMetaData(newChildBuffer, VariableSizedAccess::Offset{0}, varSizedValue);
        const ChildBufferIndex childBufferIndex{tupleBuffer.storeChildBuffer(newChildBuffer)};
        return VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{totalVarSizedLength}};
    }

    /// There is enough space in the lastChildBuffer, thus, we copy the var sized into it
    const VariableSizedAccess::Offset childOffset{usedMemorySize};
    copyVarSizedAndIncrementMetaData(lastChildBuffer, childOffset, varSizedValue);
    return VariableSizedAccess{childIndex, childOffset, VariableSizedAccess::Size{totalVarSizedLength}};
}

std::span<std::byte> loadAssociatedVarSizedValue(const TupleBuffer& tupleBuffer, const VariableSizedAccess variableSizedAccess) noexcept
{
    /// Loading the childbuffer containing the variable sized data.
    auto childBuffer = tupleBuffer.loadChildBuffer(variableSizedAccess.getIndex());

    /// Creating a subspan that starts at the required offset. It still can contain multiple other var sized, as we have solely offset the
    /// lower bound but not the upper bound.
    const auto varSized = childBuffer.getAvailableMemoryArea().subspan(variableSizedAccess.getOffset().getRawOffset());

    return varSized.subspan(0, variableSizedAccess.getSize().getRawSize());
}

LoadVarSized makeVarSizedLoadFunction(const NautilusBuffer& buffer)
{
    /// NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks,bugprone-exception-escape)
    return [buffer](const nautilus::val<int8_t*>& fieldSlot) -> std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>
    {
        const auto variableSizedAccess = static_cast<nautilus::val<VariableSizedAccess*>>(fieldSlot);
        const auto varSizedPtr = invoke(
            {.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true},
            +[](const TupleBuffer* tupleBuffer, const VariableSizedAccess* variableSizedAccessPtr) -> int8_t*
            {
                INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
                INVARIANT(variableSizedAccessPtr != nullptr, "VariableSizedAccess MUST NOT be null at this point");
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): TupleBuffer hands out byte spans by design.
                return reinterpret_cast<int8_t*>(loadAssociatedVarSizedValue(*tupleBuffer, *variableSizedAccessPtr).data());
            },
            buffer.asArg(),
            variableSizedAccess);

        /// The size sits in the VariableSizedAccess itself, so it is a plain traced load rather than another invoke
        const nautilus::val<uint64_t> size = *getMemberWithOffset<uint64_t>(variableSizedAccess, offsetof(VariableSizedAccess, size));
        return {varSizedPtr, size}; /// NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
    };
}

StoreVarSized
makeChildBufferVarSizedStoreFunction(const NautilusBuffer& buffer, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    return /// NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
        /// NOLINTNEXTLINE(bugprone-exception-escape): nautilus::invoke ignores the lambda's exception spec; INVARIANT may throw on bad input.
        [buffer,
         bufferProvider](const nautilus::val<int8_t*>& fieldSlot, const nautilus::val<int8_t*>& data, const nautilus::val<uint64_t>& size)
    {
        invoke( /// NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
            +[](TupleBuffer* tupleBuffer, AbstractBufferProvider* provider, int8_t* slot, const int8_t* payload, const uint64_t length)
            {
                INVARIANT(tupleBuffer != nullptr, "Tuplebuffer MUST NOT be null at this point");
                INVARIANT(provider != nullptr, "BufferProvider MUST NOT be null at this point");
                const std::span payloadSpan{payload, payload + length};
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): slot is the typed VariableSizedAccess slot.
                *reinterpret_cast<VariableSizedAccess*>(slot) = writeVarSized(*tupleBuffer, *provider, std::as_bytes(payloadSpan));
            },
            buffer.asArg(),
            bufferProvider,
            fieldSlot,
            data,
            size);
    };
}

StoreVarSized makeBTreeVarSizedStoreFunction(const NautilusBuffer& buffer, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    return /// NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)
        /// NOLINTNEXTLINE(bugprone-exception-escape): nautilus::invoke ignores the lambda's exception spec; INVARIANT may throw on bad input.
        [buffer,
         bufferProvider](const nautilus::val<int8_t*>& fieldSlot, const nautilus::val<int8_t*>& data, const nautilus::val<uint64_t>& size)
    {
        nautilus::invoke(
            +[](TupleBuffer* treeBuffer, AbstractBufferProvider* provider, int8_t* slot, const int8_t* payload, const uint64_t length)
            {
                INVARIANT(treeBuffer != nullptr, "BTree buffer must not be null");
                INVARIANT(provider != nullptr, "BTree buffer provider must not be null");
                auto allocation = BTree::load(*treeBuffer).allocateSpaceForVarSized(provider, length);
                *reinterpret_cast<VariableSizedAccess*>(slot) = VariableSizedAccess{/// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                                                                                    allocation.childIndex,
                                                                                    VariableSizedAccess::Offset{allocation.offset},
                                                                                    VariableSizedAccess::Size{length}};
                INVARIANT(allocation.memory.data() != nullptr, "Memory address MUST NOT be null at this point");
                std::memcpy(allocation.memory.data(), payload, length);
            },
            buffer.asArg(),
            bufferProvider,
            fieldSlot,
            data,
            size);
    };
}

}
