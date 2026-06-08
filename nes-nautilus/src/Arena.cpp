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
#include <Util/Sanitizer.hpp>
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
/// ASan shadow memory has 8-byte granularity, so the arena advances its cursor in multiples of 8. This keeps
/// every allocation's start 8-byte aligned (required for precise poisoning) and leaves the rounding padding
/// between allocations poisoned, where it acts as a redzone. The alignment is unconditional, so the layout is
/// identical with and without ASan (and the arena no longer hands out unaligned pointers).
constexpr size_t ARENA_ALLOCATION_ALIGNMENT = 8;

constexpr size_t alignToArenaGranularity(const size_t size)
{
    return (size + (ARENA_ALLOCATION_ALIGNMENT - 1)) & ~(ARENA_ALLOCATION_ALIGNMENT - 1);
}

/// A poisoned redzone inserted after every fixed-buffer allocation so that even a perfectly 8-byte-sized
/// allocation is separated from its neighbour (the alignment padding alone can be zero). It is a pure
/// detection aid, so it costs nothing in production (0 without ASan); under ASan it traps an overflow from one
/// allocation into the next. A multiple of the allocation alignment, so the next allocation stays 8-aligned.
constexpr size_t ARENA_REDZONE_SIZE = NES_ASAN_ENABLED ? ARENA_ALLOCATION_ALIGNMENT : 0;
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
        lastAllocationSize = sizeInBytes;
        const auto area = unpooledBuffers.back().getAvailableMemoryArea();
        /// The whole data region was unpoisoned on hand-out; re-poison the unused tail past the request so an
        /// overflow beyond the allocation is trapped under ASan.
        ASAN_POISON_MEMORY_REGION(area.data() + sizeInBytes, area.size() - sizeInBytes);
        return area.subspan(0, sizeInBytes);
    }

    /// Case 2: no current buffer, or the request does not fit -> pull a fresh fixed-size buffer and poison it
    /// whole. Allocations then carve unpoisoned regions out of it; the unused tail stays poisoned.
    if (fixedSizeBuffers.empty() || lastAllocationSize < currentOffset + sizeInBytes)
    {
        fixedSizeBuffers.emplace_back(bufferProvider->getBufferBlocking());
        currentOffset = 0;
        const auto area = fixedSizeBuffers.back().getAvailableMemoryArea();
        ASAN_POISON_MEMORY_REGION(area.data(), area.size());
    }

    /// Case 3: bump-allocate from the current buffer.
    auto& lastBuffer = fixedSizeBuffers.back();
    lastAllocationSize = lastBuffer.getBufferSize();
    const auto result = lastBuffer.getAvailableMemoryArea().subspan(currentOffset, sizeInBytes);
    /// Unpoison exactly the requested bytes; the alignment padding and the trailing redzone stay poisoned (the
    /// whole buffer was poisoned on acquire and is never unpoisoned outside of returned allocations), so they
    /// separate this allocation from the next.
    ASAN_UNPOISON_MEMORY_REGION(result.data(), result.size());
    currentOffset += alignToArenaGranularity(sizeInBytes) + ARENA_REDZONE_SIZE;
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
