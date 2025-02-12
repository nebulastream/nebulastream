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

namespace NES::Memory
{

FloatingBuffer::FloatingBuffer(FloatingBuffer const& other) noexcept : controlBlock(other.controlBlock), childOrMainData(other.childOrMainData)
{
    controlBlock->dataRetain();
}
 FloatingBuffer::FloatingBuffer(FloatingBuffer&& other) noexcept : controlBlock(other.controlBlock), childOrMainData(other.childOrMainData)
{
    other.controlBlock = nullptr;
    other.childOrMainData = detail::ChildOrMainDataKey::UNKNOWN();
}
FloatingBuffer::FloatingBuffer(PinnedBuffer&& buffer) noexcept
    : controlBlock(buffer.getControlBlock()), childOrMainData(buffer.childOrMainData)
{
    controlBlock->pinnedRelease();
    buffer.controlBlock = nullptr;
    buffer.dataSegment = detail::DataSegment<detail::InMemoryLocation>{};
    buffer.childOrMainData = detail::ChildOrMainDataKey::UNKNOWN();
    //TODO Put into a persistable queue in buffer manager or just mark buffer als persistable?
    //Queue is more expensive, but allows to use LRU or similar policies easily.
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
