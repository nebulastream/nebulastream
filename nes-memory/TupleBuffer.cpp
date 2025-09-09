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

#include <Runtime/TupleBuffer.hpp>

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp> ///NOLINT: required for fmt
#include <MemoryLayout/VariableSizedAccess.hpp>
#include <Time/Timestamp.hpp>
#include <fmt/format.h>
#include <nautilus/inline.hpp>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

namespace NES
{

TupleBuffer::TupleBuffer(const TupleBuffer& other) noexcept : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size)
{
    if (controlBlock != nullptr)
    {
        controlBlock->retain();
    }
}

TupleBuffer& TupleBuffer::operator=(const TupleBuffer& other) noexcept
{
    if PLACEHOLDER_UNLIKELY (this == std::addressof(other))
    {
        return *this;
    }

    /// Override the content of this with those of `other`
    auto* const oldControlBlock = std::exchange(controlBlock, other.controlBlock);
    ptr = other.ptr;
    size = other.size;

    /// Update reference counts: If the new and old controlBlocks differ, retain the new one and release the old one.
    if (oldControlBlock != controlBlock)
    {
        retain();
        if (oldControlBlock)
        {
            oldControlBlock->release();
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
    release();
}

NAUTILUS_INLINE TupleBuffer& TupleBuffer::retain() noexcept
{
    if (controlBlock)
    {
        controlBlock->retain();
    }
    return *this;
}

NAUTILUS_INLINE void TupleBuffer::release() noexcept
{
    if (controlBlock)
    {
        controlBlock->release();
    }
    controlBlock = nullptr;
    ptr = nullptr;
    size = 0;
}

NAUTILUS_INLINE uint32_t TupleBuffer::getReferenceCounter() const noexcept
{
    return controlBlock ? controlBlock->getReferenceCount() : 0;
}

NAUTILUS_INLINE uint64_t TupleBuffer::getBufferSize() const noexcept
{
    return size;
}

NAUTILUS_INLINE void TupleBuffer::setNumberOfTuples(const uint64_t numberOfTuples) const noexcept
{
    controlBlock->setNumberOfTuples(numberOfTuples);
}

NAUTILUS_INLINE Timestamp TupleBuffer::getWatermark() const noexcept
{
    return controlBlock->getWatermark();
}

NAUTILUS_INLINE void TupleBuffer::setWatermark(const Timestamp value) noexcept
{
    controlBlock->setWatermark(value);
}

NAUTILUS_INLINE Timestamp TupleBuffer::getCreationTimestampInMS() const noexcept
{
    return controlBlock->getCreationTimestamp();
}

NAUTILUS_INLINE void TupleBuffer::setSequenceNumber(const SequenceNumber sequenceNumber) noexcept
{
    controlBlock->setSequenceNumber(sequenceNumber);
}

std::string TupleBuffer::getSequenceDataAsString() const noexcept
{
    return fmt::format("SeqNumber: {}, ChunkNumber: {}, LastChunk: {}", getSequenceNumber(), getChunkNumber(), isLastChunk());
}

NAUTILUS_INLINE SequenceNumber TupleBuffer::getSequenceNumber() const noexcept
{
    return controlBlock->getSequenceNumber();
}

NAUTILUS_INLINE void TupleBuffer::setChunkNumber(const ChunkNumber chunkNumber) noexcept
{
    controlBlock->setChunkNumber(chunkNumber);
}

NAUTILUS_INLINE void TupleBuffer::setLastChunk(const bool isLastChunk) noexcept
{
    controlBlock->setLastChunk(isLastChunk);
}

NAUTILUS_INLINE bool TupleBuffer::isLastChunk() const noexcept
{
    return controlBlock->isLastChunk();
}

NAUTILUS_INLINE void TupleBuffer::setCreationTimestampInMS(const Timestamp value) noexcept
{
    controlBlock->setCreationTimestamp(value);
}

NAUTILUS_INLINE void TupleBuffer::setOriginId(const OriginId id) noexcept
{
    controlBlock->setOriginId(id);
}

VariableSizedAccess::Index TupleBuffer::storeChildBuffer(TupleBuffer& buffer) noexcept
{
    TupleBuffer empty;
    auto* control = buffer.controlBlock;
    INVARIANT(controlBlock != control, "Cannot attach buffer to self");
    const auto index = controlBlock->storeChildBuffer(control);
    std::swap(empty, buffer);
    return index;
}

TupleBuffer TupleBuffer::loadChildBuffer(VariableSizedAccess::Index bufferIndex) const noexcept
{
    TupleBuffer childBuffer;
    const auto ret = controlBlock->loadChildBuffer(bufferIndex, childBuffer.controlBlock, childBuffer.ptr, childBuffer.size);
    INVARIANT(ret, "Cannot load tuple buffer with index={}", bufferIndex);
    return childBuffer;
}

bool recycleTupleBuffer(void* bufferPointer)
{
    PRECONDITION(bufferPointer, "invalid bufferPointer");
    auto buffer = reinterpret_cast<uint8_t*>(bufferPointer);
    auto block = reinterpret_cast<detail::BufferControlBlock*>(buffer - sizeof(detail::BufferControlBlock));
    return block->release();
}

NAUTILUS_INLINE void swap(TupleBuffer& lhs, TupleBuffer& rhs) noexcept
{
    /// Enable ADL to spell out to onlookers how swap should be used.
    using std::swap;

    swap(lhs.ptr, rhs.ptr);
    swap(lhs.size, rhs.size);
    swap(lhs.controlBlock, rhs.controlBlock);
}

std::ostream& operator<<(std::ostream& os, const TupleBuffer& buff) noexcept
{
    return os << reinterpret_cast<std::uintptr_t>(buff.ptr);
}

NAUTILUS_INLINE uint64_t TupleBuffer::getNumberOfTuples() const noexcept
{
    return controlBlock->getNumberOfTuples();
}

NAUTILUS_INLINE OriginId TupleBuffer::getOriginId() const noexcept
{
    return controlBlock->getOriginId();
}

NAUTILUS_INLINE uint32_t TupleBuffer::getNumberOfChildBuffers() const noexcept
{
    return controlBlock->getNumberOfChildBuffers();
}

NAUTILUS_INLINE ChunkNumber TupleBuffer::getChunkNumber() const noexcept
{
    return controlBlock->getChunkNumber();
}


}
