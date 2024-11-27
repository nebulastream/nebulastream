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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/asio/detail/descriptor_ops.hpp>
#include <fmt/format.h>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include "TupleBufferImpl.hpp"
namespace NES::Memory
{


// TupleBuffer TupleBuffer::reinterpretAsTupleBuffer(void* bufferPointer)
// {
//     auto controlBlockSize = alignBufferSize(sizeof(detail::BufferControlBlock), 64);
//     auto buffer = reinterpret_cast<uint8_t*>(bufferPointer);
//     auto block = reinterpret_cast<detail::BufferControlBlock*>(buffer - controlBlockSize);
//     auto pb = TupleBuffer{block};
//     //auto tb = TupleBuffer(block, block->data.getLocation().inMemory, block->data.getSize());
//     return pb;
// }

// TupleBuffer::TupleBuffer(detail::BufferControlBlock* controlBlock) noexcept : controlBlock(controlBlock), dataSegment(controlBlock->getData())
// {
//     retain();
// }

TupleBuffer::TupleBuffer(
    detail::BufferControlBlock* controlBlock,
    detail::DataSegment<detail::InMemoryLocation> segment,
    detail::ChildOrMainDataKey childOrMainData) noexcept
    : controlBlock(controlBlock), dataSegment(segment), childOrMainData(childOrMainData)
{
    controlBlock->pinnedRetain();
    controlBlock->dataRetain();
}

TupleBuffer::TupleBuffer(const TupleBuffer& other) noexcept
    : controlBlock(other.controlBlock), dataSegment(other.dataSegment), childOrMainData(other.childOrMainData)
{
    controlBlock->pinnedRetain();
    controlBlock->dataRetain();
}


TupleBuffer& TupleBuffer::operator=(const TupleBuffer& other) noexcept
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
TupleBuffer& TupleBuffer::operator=(TupleBuffer&& other) noexcept
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
TupleBuffer::~TupleBuffer() noexcept
{
    if (controlBlock != nullptr)
    {
        controlBlock->pinnedRelease();
        controlBlock->dataRelease();
    }
}

int8_t* TupleBuffer::getBuffer() noexcept
{
    return getBuffer<int8_t>();
}
uint32_t TupleBuffer::getReferenceCounter() const noexcept
{
    return (controlBlock != nullptr) ? controlBlock->getPinnedReferenceCount() : 0;
}

bool TupleBuffer::isValid() const noexcept
{
    return controlBlock != nullptr && childOrMainData != detail::ChildOrMainDataKey::UNKNOWN()
        && dataSegment.getLocation().getPtr() != nullptr;
}

uint64_t TupleBuffer::getBufferSize() const noexcept
{
    return dataSegment.getSize();
}
void TupleBuffer::setNumberOfTuples(const uint64_t numberOfTuples) const noexcept
{
    controlBlock->setNumberOfTuples(numberOfTuples);
}
Timestamp TupleBuffer::getWatermark() const noexcept
{
    return controlBlock->getWatermark();
}
void TupleBuffer::setWatermark(const Timestamp value) noexcept
{
    controlBlock->setWatermark(value);
}
Timestamp TupleBuffer::getCreationTimestampInMS() const noexcept
{
    return controlBlock->getCreationTimestamp();
}
void TupleBuffer::setSequenceNumber(const SequenceNumber sequenceNumber) noexcept
{
    controlBlock->setSequenceNumber(sequenceNumber);
}

std::string TupleBuffer::getSequenceDataAsString() const noexcept
{
    return fmt::format("SeqNumber: {}, ChunkNumber: {}, LastChunk: {}", getSequenceNumber(), getChunkNumber(), isLastChunk());
}

SequenceNumber TupleBuffer::getSequenceNumber() const noexcept
{
    return controlBlock->getSequenceNumber();
}
void TupleBuffer::setChunkNumber(const ChunkNumber chunkNumber) noexcept
{
    controlBlock->setChunkNumber(chunkNumber);
}
void TupleBuffer::setLastChunk(const bool isLastChunk) noexcept
{
    controlBlock->setLastChunk(isLastChunk);
}
bool TupleBuffer::isLastChunk() const noexcept
{
    return controlBlock->isLastChunk();
}
void TupleBuffer::setCreationTimestampInMS(const Timestamp value) noexcept
{
    controlBlock->setCreationTimestamp(value);
}
void TupleBuffer::setOriginId(const OriginId id) noexcept
{
    controlBlock->setOriginId(id);
}


std::optional<ChildKey> TupleBuffer::storeReturnChildIndex(TupleBuffer&& other) const noexcept
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

std::optional<TupleBuffer> TupleBuffer::loadChildBuffer(const int8_t* ptr, uint32_t size) const noexcept
{
    //We use uint8_t internally
    auto nonConstPtr = reinterpret_cast<const uint8_t*>(ptr);
    const auto segment = detail::DataSegment{detail::InMemoryLocation{nonConstPtr}, size};
    auto childKey = controlBlock->findChild(segment);
    if (childKey)
    {
        return TupleBuffer{controlBlock, segment, *childKey};
    }
    return std::nullopt;
}

std::optional<TupleBuffer> TupleBuffer::storeReturnAsChildBuffer(TupleBuffer&& buffer) const noexcept
{
    const detail::DataSegment<detail::InMemoryLocation> childSegment = buffer.dataSegment;
    if (auto index = storeReturnChildIndex(std::move(buffer)))
    {
        std::optional<TupleBuffer>::value_type pinnedBuffer{controlBlock, childSegment, *index};
        return pinnedBuffer;
    }
    return std::nullopt;
}
// bool TupleBuffer::deleteChild(TupleBuffer&& child) noexcept
// {
// }
// bool TupleBuffer::deleteChild(const int8_t* oldbuffer, uint32_t size) const noexcept
// {
// }
bool TupleBuffer::stealChild(const TupleBuffer* oldParent, const int8_t* child, uint32_t size) const
{
    //Create dummy temp pinned buffer for the old parent
    //Marks the child buffer index as unknown, whoever tackles varsize data needs to redo this anyway
    TupleBuffer temp{
        oldParent->controlBlock,
        detail::DataSegment{detail::InMemoryLocation{reinterpret_cast<uint8_t*>(const_cast<int8_t*>(child))}, size},
        detail::ChildOrMainDataKey::UNKNOWN()};
    return storeReturnChildIndex(std::move(temp)).has_value();
}
std::optional<TupleBuffer> TupleBuffer::loadChildBuffer(ChildKey bufferIndex) const
{
    if (const auto childSegment = controlBlock->getChild(bufferIndex))
    {
        if (const auto inMemorySegment = childSegment->get<detail::InMemoryLocation>())
        {
            return TupleBuffer{controlBlock, *inMemorySegment, bufferIndex};
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

void swap(TupleBuffer& lhs, TupleBuffer& rhs) noexcept
{
    /// Enable ADL to spell out to onlookers how swap should be used.
    using std::swap;

    swap(lhs.dataSegment, rhs.dataSegment);
    swap(lhs.controlBlock, rhs.controlBlock);
    swap(lhs.childOrMainData, rhs.childOrMainData);
}
std::ostream& operator<<(std::ostream& os, const TupleBuffer& buff) noexcept
{
    return os << reinterpret_cast<std::uintptr_t>(buff.dataSegment.getLocation().getPtr());
}

uint64_t TupleBuffer::getNumberOfTuples() const noexcept
{
    return controlBlock->getNumberOfTuples();
}
OriginId TupleBuffer::getOriginId() const noexcept
{
    return controlBlock->getOriginId();
}
uint32_t TupleBuffer::getNumberOfChildrenBuffer() const noexcept
{
    return controlBlock->getNumberOfChildrenBuffers();
}
ChunkNumber TupleBuffer::getChunkNumber() const noexcept
{
    return controlBlock->getChunkNumber();
}


}
