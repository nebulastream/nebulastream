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

#include <cstddef>
#include <functional>
#include <optional>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/PinnedBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/asio/detail/descriptor_ops.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include "TupleBufferImpl.hpp"
namespace NES::Memory
{


// PinnedBuffer PinnedBuffer::reinterpretAsTupleBuffer(void* bufferPointer)
// {
//     auto controlBlockSize = alignBufferSize(sizeof(detail::BufferControlBlock), 64);
//     auto buffer = reinterpret_cast<uint8_t*>(bufferPointer);
//     auto block = reinterpret_cast<detail::BufferControlBlock*>(buffer - controlBlockSize);
//     auto pb = PinnedBuffer{block};
//     //auto tb = PinnedBuffer(block, block->data.getLocation().inMemory, block->data.getSize());
//     return pb;
// }

// PinnedBuffer::PinnedBuffer(detail::BufferControlBlock* controlBlock) noexcept : controlBlock(controlBlock), dataSegment(controlBlock->getData())
// {
//     retain();
// }

PinnedBuffer::PinnedBuffer(
    detail::BufferControlBlock* controlBlock,
    detail::DataSegment<detail::InMemoryLocation> segment,
    detail::ChildOrMainDataKey childOrMainData) noexcept
    : controlBlock(controlBlock), dataSegment(segment), childOrMainData(childOrMainData)
{
    controlBlock->pinnedRetain();
    controlBlock->dataRetain();
}

PinnedBuffer::PinnedBuffer(const PinnedBuffer& other) noexcept
    : controlBlock(other.controlBlock), dataSegment(other.dataSegment), childOrMainData(other.childOrMainData)
{
    controlBlock->pinnedRetain();
    controlBlock->dataRetain();
}


PinnedBuffer& PinnedBuffer::operator=(const PinnedBuffer& other) noexcept
{
    if PLACEHOLDER_UNLIKELY (this == std::addressof(other))
    {
        return *this;
    }

    /// Override the content of this with those of `other`
    auto* const oldControlBlock = std::exchange(controlBlock, other.controlBlock);
    dataSegment = other.dataSegment;
    childOrMainData = other.childOrMainData;

    /// Update reference counts: If the new and old controlBlocks differ, retain the new one and release the old one.
    if (oldControlBlock != controlBlock)
    {
        controlBlock->pinnedRetain();
        controlBlock->dataRetain();
        if (oldControlBlock != nullptr)
        {
            oldControlBlock->pinnedRelease();
            oldControlBlock->dataRelease();
        }
    }
    return *this;
}
PinnedBuffer& PinnedBuffer::operator=(PinnedBuffer&& other) noexcept
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
PinnedBuffer::~PinnedBuffer() noexcept
{
    if (controlBlock != nullptr)
    {
        controlBlock->pinnedRelease();
        controlBlock->dataRelease();
    }
}

int8_t* PinnedBuffer::getBuffer() noexcept
{
    return getBuffer<int8_t>();
}
uint32_t PinnedBuffer::getReferenceCounter() const noexcept
{
    return (controlBlock != nullptr) ? controlBlock->getPinnedReferenceCount() : 0;
}

bool PinnedBuffer::isValid() const noexcept
{
    return controlBlock != nullptr && childOrMainData != detail::ChildOrMainDataKey::UNKNOWN()
        && dataSegment.getLocation().getPtr() != nullptr;
}

uint64_t PinnedBuffer::getBufferSize() const noexcept
{
    return dataSegment.getSize();
}
void PinnedBuffer::setNumberOfTuples(const uint64_t numberOfTuples) const noexcept
{
    controlBlock->setNumberOfTuples(numberOfTuples);
}
Runtime::Timestamp PinnedBuffer::getWatermark() const noexcept
{
    return controlBlock->getWatermark();
}
void PinnedBuffer::setWatermark(const Runtime::Timestamp value) noexcept
{
    controlBlock->setWatermark(value);
}
Runtime::Timestamp PinnedBuffer::getCreationTimestampInMS() const noexcept
{
    return controlBlock->getCreationTimestamp();
}
void PinnedBuffer::setSequenceNumber(const SequenceNumber sequenceNumber) noexcept
{
    controlBlock->setSequenceNumber(sequenceNumber);
}

std::string PinnedBuffer::getSequenceDataAsString() const noexcept
{
    return fmt::format("SeqNumber: {}, ChunkNumber: {}, LastChunk: {}", getSequenceNumber(), getChunkNumber(), isLastChunk());
}

SequenceNumber PinnedBuffer::getSequenceNumber() const noexcept
{
    return controlBlock->getSequenceNumber();
}
void PinnedBuffer::setChunkNumber(const ChunkNumber chunkNumber) noexcept
{
    controlBlock->setChunkNumber(chunkNumber);
}
void PinnedBuffer::setLastChunk(const bool isLastChunk) noexcept
{
    controlBlock->setLastChunk(isLastChunk);
}
bool PinnedBuffer::isLastChunk() const noexcept
{
    return controlBlock->isLastChunk();
}
void PinnedBuffer::setCreationTimestampInMS(const Runtime::Timestamp value) noexcept
{
    controlBlock->setCreationTimestamp(value);
}
void PinnedBuffer::setOriginId(const OriginId id) noexcept
{
    controlBlock->setOriginId(id);
}


std::optional<ChildKey> PinnedBuffer::storeReturnChildIndex(PinnedBuffer&& other) const noexcept
{
    PRECONDITION(controlBlock != other.controlBlock, "Cannot attach other to self");
    other.controlBlock->pinnedRelease();
    if (auto childKey = other.childOrMainData.asChildKey())
    {
        const detail::DataSegment<detail::InMemoryLocation> childSegment = other.dataSegment;
        //other is a child buffer
        if (other.controlBlock->unregisterChild(*childKey))
        {
            //The passed other was also part of the reference count, we effectively destroyed the old one
            other.controlBlock->dataRelease();
            other.dataSegment = detail::DataSegment<detail::InMemoryLocation>{};
            other.controlBlock = nullptr;
            other.childOrMainData = detail::ChildOrMainDataKey::UNKNOWN();
            return controlBlock->registerChild(childSegment);
        }
    }
    else
    {
        //Destroy other buffers BCB and steal its data segment
        if (const auto childSegment
            = other.controlBlock->stealDataSegment().and_then(&detail::DataSegment<detail::DataLocation>::get<detail::InMemoryLocation>))
        {
            other.controlBlock = nullptr;
            other.dataSegment = detail::DataSegment<detail::InMemoryLocation>{};
            other.childOrMainData = detail::ChildOrMainDataKey::UNKNOWN();
            return controlBlock->registerChild(*childSegment);
        }
    }
    other.controlBlock->pinnedRetain();
    return std::nullopt;
}

std::optional<PinnedBuffer> PinnedBuffer::loadChildBuffer(const int8_t* ptr, uint32_t size) const noexcept
{
    //We use uint8_t internally
    auto nonConstPtr = reinterpret_cast<const uint8_t*>(ptr);
    const auto segment = detail::DataSegment{detail::InMemoryLocation{nonConstPtr}, size};
    auto childKey = controlBlock->findChild(segment);
    if (childKey)
    {
        return PinnedBuffer{controlBlock, segment, *childKey};
    }
    return std::nullopt;
}

std::optional<PinnedBuffer> PinnedBuffer::storeReturnAsChildBuffer(PinnedBuffer&& buffer) const noexcept
{
    const detail::DataSegment<detail::InMemoryLocation> childSegment = buffer.dataSegment;
    if (auto index = storeReturnChildIndex(std::move(buffer)))
    {
        std::optional<PinnedBuffer>::value_type pinnedBuffer{controlBlock, childSegment, *index};
        return pinnedBuffer;
    }
    return std::nullopt;
}
// bool PinnedBuffer::deleteChild(PinnedBuffer&& child) noexcept
// {
// }
// bool PinnedBuffer::deleteChild(const int8_t* oldbuffer, uint32_t size) const noexcept
// {
// }
bool PinnedBuffer::stealChild(const PinnedBuffer* oldParent, const int8_t* child, uint32_t size) const
{
    //Create dummy temp pinned buffer for the old parent
    //Marks the child buffer index as unknown, whoever tackles varsize data needs to redo this anyway
    PinnedBuffer temp{
        oldParent->controlBlock,
        detail::DataSegment{detail::InMemoryLocation{reinterpret_cast<uint8_t*>(const_cast<int8_t*>(child))}, size},
        detail::ChildOrMainDataKey::UNKNOWN()};
    return storeReturnChildIndex(std::move(temp)).has_value();
}
std::optional<PinnedBuffer> PinnedBuffer::loadChildBuffer(ChildKey bufferIndex) const
{
    if (const auto childSegment = controlBlock->getChild(bufferIndex))
    {
        if (const auto inMemorySegment = childSegment->get<detail::InMemoryLocation>())
        {
            return PinnedBuffer{controlBlock, *inMemorySegment, bufferIndex};
        }
        INVARIANT(!childSegment->isSpilled(), "Child of a pinned buffer was still spilled");
    }
    return std::nullopt;
}

bool recycleTupleBuffer(void* bufferPointer)
{
    PRECONDITION(bufferPointer, "invalid bufferPointer");
    auto buffer = reinterpret_cast<uint8_t*>(bufferPointer);
    auto block = reinterpret_cast<detail::BufferControlBlock*>(buffer - sizeof(detail::BufferControlBlock));
    return block->pinnedRelease();
}

void swap(PinnedBuffer& lhs, PinnedBuffer& rhs) noexcept
{
    /// Enable ADL to spell out to onlookers how swap should be used.
    using std::swap;

    swap(lhs.dataSegment, rhs.dataSegment);
    swap(lhs.controlBlock, rhs.controlBlock);
    swap(lhs.childOrMainData, rhs.childOrMainData);
}
std::ostream& operator<<(std::ostream& os, const PinnedBuffer& buff) noexcept
{
    return os << reinterpret_cast<std::uintptr_t>(buff.dataSegment.getLocation().getPtr());
}

uint64_t PinnedBuffer::getNumberOfTuples() const noexcept
{
    return controlBlock->getNumberOfTuples();
}
OriginId PinnedBuffer::getOriginId() const noexcept
{
    return controlBlock->getOriginId();
}
uint32_t PinnedBuffer::getNumberOfChildrenBuffer() const noexcept
{
    return controlBlock->getNumberOfChildrenBuffers();
}
ChunkNumber PinnedBuffer::getChunkNumber() const noexcept
{
    return controlBlock->getChunkNumber();
}


}
