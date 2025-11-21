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

#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

namespace NES
{

std::span<std::byte> Arena::allocateMemory(const size_t sizeInBytes)
{
    /// Case 1
    if (bufferProvider->getBufferSize() < sizeInBytes)
    {
        const auto unpooledBufferOpt = bufferProvider->getUnpooledBuffer(sizeInBytes);
        if (not unpooledBufferOpt.has_value())
        {
            throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size " + std::to_string(sizeInBytes));
        }
        unpooledBuffers.emplace_back(unpooledBufferOpt.value());
        lastAllocationSize = sizeInBytes;
        return unpooledBuffers.back().getAvailableMemoryArea().subspan(0, sizeInBytes);
    }

    if (fixedSizeBuffers.empty())
    {
        fixedSizeBuffers.emplace_back(bufferProvider->getBufferBlocking());
        lastAllocationSize = bufferProvider->getBufferSize();
        currentOffset += sizeInBytes;
        return fixedSizeBuffers.back().getAvailableMemoryArea().subspan(0, sizeInBytes);
    }

    /// Case 2
    if (lastAllocationSize < currentOffset + sizeInBytes)
    {
        fixedSizeBuffers.emplace_back(bufferProvider->getBufferBlocking());
        this->currentOffset = 0;
    }

    /// Case 3
    auto& lastBuffer = fixedSizeBuffers.back();
    lastAllocationSize = lastBuffer.getBufferSize();
    const auto result = lastBuffer.getAvailableMemoryArea().subspan(currentOffset, sizeInBytes);
    currentOffset += sizeInBytes;
    return result;
}

nautilus::val<int8_t*> ArenaRef::allocateMemory(const nautilus::val<size_t>& sizeInBytes)
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

Nautilus::VariableSizedData ArenaRef::allocateVariableSizedData(const nautilus::val<uint32_t>& sizeInBytes)
{
    auto basePtr = allocateMemory(sizeInBytes + nautilus::val<size_t>(4));
    *(static_cast<nautilus::val<uint32_t*>>(basePtr)) = sizeInBytes;
    return Nautilus::VariableSizedData(basePtr, sizeInBytes);
}

nautilus::val<Arena*> ArenaRef::getArena() const
{
    return this->arenaRef;
}
}
