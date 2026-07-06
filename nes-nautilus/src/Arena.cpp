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

static constexpr size_t DEFAULT_ARENA_PAGE_SIZE = 4096;

std::span<std::byte> Arena::allocateMemory(const size_t sizeInBytes)
{
    /// Case 1: the request does not fit into a default-size buffer. Serve it from the smallest
    /// fitting pooled size class (falling back to an unpooled buffer for very large requests).
    if (DEFAULT_ARENA_PAGE_SIZE < sizeInBytes)
    {
        if (bufferProvider->getMaxBufferSize() < sizeInBytes)
        {
            auto oversizedBuffer = bufferProvider->getUnpooledBuffer(sizeInBytes);
            if (!oversizedBuffer)
            {
                throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size {}", sizeInBytes);
            }
            unpooledBuffers.emplace_back(std::move(*oversizedBuffer));
            lastAllocationSize = sizeInBytes;
            return unpooledBuffers.back().getAvailableMemoryArea().subspan(0, sizeInBytes);
        }
        auto oversizedBuffer = bufferProvider->getBuffer(sizeInBytes);
        lastAllocationSize = oversizedBuffer.getBufferSize();
        currentOffset = sizeInBytes;
        fixedSizeBuffers.emplace_back(std::move(oversizedBuffer));
        return fixedSizeBuffers.back().getAvailableMemoryArea().subspan(0, sizeInBytes);
    }

    if (fixedSizeBuffers.empty())
    {
        fixedSizeBuffers.emplace_back(bufferProvider->getBuffer(DEFAULT_ARENA_PAGE_SIZE));
        lastAllocationSize = DEFAULT_ARENA_PAGE_SIZE;
        currentOffset += sizeInBytes;
        return fixedSizeBuffers.back().getAvailableMemoryArea().subspan(0, sizeInBytes);
    }

    /// Case 2
    if (lastAllocationSize < currentOffset + sizeInBytes)
    {
        fixedSizeBuffers.emplace_back(bufferProvider->getBuffer(DEFAULT_ARENA_PAGE_SIZE));
        this->currentOffset = 0;
    }

    /// Case 3
    auto& lastBuffer = fixedSizeBuffers.back();
    lastAllocationSize = lastBuffer.getBufferSize();
    const auto result = lastBuffer.getAvailableMemoryArea().subspan(currentOffset, sizeInBytes);
    currentOffset += sizeInBytes;
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
