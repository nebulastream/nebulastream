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

#include <Arena.hpp>

#include <DataTypes/VariableSizedData.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

namespace NES
{

namespace
{
/// Arena allocations are advanced in multiples of 8 bytes so that every allocation starts on an 8-byte
/// boundary. The arena previously handed out tightly packed, unaligned pointers, which is unsafe for typed or
/// atomic access (e.g. ARM SIGBUSes on unaligned atomic access).
constexpr size_t ARENA_ALLOCATION_ALIGNMENT = 8;
constexpr size_t alignToArenaGranularity(const size_t size)
{
    return (size + (ARENA_ALLOCATION_ALIGNMENT - 1)) & ~(ARENA_ALLOCATION_ALIGNMENT - 1);
}
}

std::span<std::byte> Arena::allocateMemory(const size_t sizeInBytes)
{
    /// Case 1: larger than a pooled buffer -> dedicated unpooled buffer.
    if (bufferProvider->getBufferSize() < sizeInBytes)
    {
        const auto unpooledBufferOpt = bufferProvider->getUnpooledBuffer(sizeInBytes);
        if (not unpooledBufferOpt.has_value())
        {
            throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size " + std::to_string(sizeInBytes));
        }
        unpooledBuffers.emplace_back(unpooledBufferOpt.value());
        return unpooledBuffers.back().getAvailableMemoryArea().subspan(0, sizeInBytes);
    }

    /// Case 2: no current buffer, or the request does not fit -> pull a fresh fixed-size buffer.
    if (fixedSizeBuffers.empty() || lastAllocationSize < currentOffset + sizeInBytes)
    {
        fixedSizeBuffers.emplace_back(bufferProvider->getBufferBlocking());
        currentOffset = 0;
    }

    /// Case 3: bump-allocate from the current buffer, advancing the cursor by the aligned size.
    auto& lastBuffer = fixedSizeBuffers.back();
    lastAllocationSize = lastBuffer.getBufferSize();
    const auto result = lastBuffer.getAvailableMemoryArea().subspan(currentOffset, sizeInBytes);
    currentOffset += alignToArenaGranularity(sizeInBytes);
    return result;
}

nautilus::val<int8_t*> ArenaRef::allocateMemory(const nautilus::val<size_t>& sizeInBytes) const
{
    /// If the available space for the pointer is smaller than the required size, we allocate a new buffer from the arena.
    /// We use the arena's allocateMemory function to allocate a new buffer and set the available space for the pointer to the last allocation size.
    /// Further, we set the space pointer to the beginning of the new buffer.
    const auto currentArenaPtr = nautilus::invoke(
        +[](Arena* arena, const size_t sizeInBytesVal) -> int8_t*
        {
            return reinterpret_cast<int8_t*>( ///NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                arena->allocateMemory(sizeInBytesVal).data());
        },
        arenaRef,
        sizeInBytes);
    return currentArenaPtr;
}

VariableSizedData ArenaRef::allocateVariableSizedData(const nautilus::val<uint64_t>& sizeInBytes) const
{
    const auto basePtr = allocateMemory(sizeInBytes);
    return VariableSizedData(basePtr, sizeInBytes);
}

nautilus::val<Arena*> ArenaRef::getArena() const
{
    return this->arenaRef;
}
}
