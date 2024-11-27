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

#include <Runtime/FloatingBuffer.hpp>
#include <Runtime/PinnedBuffer.hpp>
#include <TupleBufferImpl.hpp>

namespace NES::Memory
{
FloatingBuffer::FloatingBuffer() noexcept : controlBlock(nullptr), childOrMainData(detail::ChildOrMainDataKey::UNKNOWN())
{
}

FloatingBuffer::FloatingBuffer(const FloatingBuffer& other) noexcept
    : controlBlock(other.controlBlock), childOrMainData(other.childOrMainData)
{
    if (controlBlock != nullptr)
    {
        controlBlock->dataRetain();
    }
}
FloatingBuffer::FloatingBuffer(FloatingBuffer&& other) noexcept : controlBlock(other.controlBlock), childOrMainData(other.childOrMainData)
{
    other.controlBlock = nullptr;
    other.childOrMainData = detail::ChildOrMainDataKey::UNKNOWN();
}
FloatingBuffer::FloatingBuffer(PinnedBuffer&& buffer) noexcept
    : controlBlock(buffer.getControlBlock()), childOrMainData(buffer.childOrMainData)
{
    if (controlBlock != nullptr)
    {
        controlBlock->pinnedRelease();
    }
    buffer.controlBlock = nullptr;
    buffer.dataSegment = detail::DataSegment<detail::InMemoryLocation>{};
    buffer.childOrMainData = detail::ChildOrMainDataKey::UNKNOWN();
    //TODO Put into a persistable queue in buffer manager or just mark buffer als persistable?
    //Queue is more expensive, but allows to use LRU or similar policies easily.
}
FloatingBuffer& FloatingBuffer::operator=(const FloatingBuffer& other) noexcept
{
    if PLACEHOLDER_UNLIKELY (this == std::addressof(other))
    {
        return *this;
    }

    /// Override the content of this with those of `other`
    auto* const oldControlBlock = std::exchange(controlBlock, other.controlBlock);
    childOrMainData = other.childOrMainData;

    /// Update reference counts: If the new and old controlBlocks differ, retain the new one and release the old one.
    if (oldControlBlock != controlBlock)
    {
        controlBlock->dataRetain();
        if (oldControlBlock != nullptr)
        {
            oldControlBlock->dataRelease();
        }
    }
    return *this;
}

FloatingBuffer& FloatingBuffer::operator=(FloatingBuffer&& other) noexcept
{
    /// Especially for rvalues, the following branch should most likely never be taken if the caller writes
    /// reasonable code. Therefore, this branch is considered unlikely.
    if PLACEHOLDER_UNLIKELY (this == std::addressof(other))
    {
        return *this;
    }

    /// Swap content of this with those of `other` to let the other's destructor take care of releasing the overwritten
    /// resource.
    using std::swap;
    swap(*this, other);

    return *this;
}
void swap(FloatingBuffer& lhs, FloatingBuffer& rhs) noexcept
{
    /// Enable ADL to spell out to onlookers how swap should be used.
    using std::swap;

    swap(lhs.controlBlock, rhs.controlBlock);
    swap(lhs.childOrMainData, rhs.childOrMainData);
}

FloatingBuffer::~FloatingBuffer() noexcept
{
    if (controlBlock != nullptr)
    {
        controlBlock->dataRelease();
    }
}
detail::DataSegment<detail::DataLocation> FloatingBuffer::getSegment() const noexcept
{
    if (childOrMainData == detail::ChildOrMainDataKey::MAIN())
    {
        return controlBlock->getData();
    }
    //TODO handle unknown case
    return *controlBlock->getChild(*childOrMainData.asChildKey());
}

bool FloatingBuffer::isSpilled() const
{
    return getSegment().isSpilled();
}

}
