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

#include <Runtime/Buffer.hpp>

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp> ///NOLINT: required for fmt
#include <Time/Timestamp.hpp>
#include <fmt/format.h>
#include <BufferImpl.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

Buffer::Buffer(const Buffer& other) noexcept : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size)
{
    if (controlBlock != nullptr)
    {
        controlBlock->retain();
    }
}

Buffer& Buffer::operator=(const Buffer& other) noexcept
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

Buffer& Buffer::operator=(Buffer&& other) noexcept
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

Buffer::~Buffer() noexcept
{
    release();
}

Buffer& Buffer::retain() noexcept
{
    if (controlBlock)
    {
        controlBlock->retain();
    }
    return *this;
}

void Buffer::release() noexcept
{
    if (controlBlock)
    {
        controlBlock->release();
    }
    controlBlock = nullptr;
    ptr = nullptr;
    size = 0;
}

uint32_t Buffer::getReferenceCounter() const noexcept
{
    return controlBlock ? controlBlock->getReferenceCount() : 0;
}

uint64_t Buffer::getBufferSize() const noexcept
{
    return size;
}

void Buffer::setNumberOfTuples(const uint64_t numberOfTuples) const noexcept
{
    controlBlock->setNumberOfTuples(numberOfTuples);
}

Timestamp Buffer::getWatermark() const noexcept
{
    return controlBlock->getWatermark();
}

void Buffer::setWatermark(const Timestamp value) noexcept
{
    controlBlock->setWatermark(value);
}

Timestamp Buffer::getCreationTimestampInMS() const noexcept
{
    return controlBlock->getCreationTimestamp();
}

void Buffer::setSequenceNumber(const SequenceNumber sequenceNumber) noexcept
{
    controlBlock->setSequenceNumber(sequenceNumber);
}

std::string Buffer::getSequenceDataAsString() const noexcept
{
    return fmt::format("SeqNumber: {}, ChunkNumber: {}, LastChunk: {}", getSequenceNumber(), getChunkNumber(), isLastChunk());
}

SequenceNumber Buffer::getSequenceNumber() const noexcept
{
    return controlBlock->getSequenceNumber();
}

void Buffer::setChunkNumber(const ChunkNumber chunkNumber) noexcept
{
    controlBlock->setChunkNumber(chunkNumber);
}

void Buffer::setLastChunk(const bool isLastChunk) noexcept
{
    controlBlock->setLastChunk(isLastChunk);
}

bool Buffer::isLastChunk() const noexcept
{
    return controlBlock->isLastChunk();
}

void Buffer::setCreationTimestampInMS(const Timestamp value) noexcept
{
    controlBlock->setCreationTimestamp(value);
}

void Buffer::setOriginId(const OriginId id) noexcept
{
    controlBlock->setOriginId(id);
}

ChildBufferIndex Buffer::storeChildBuffer(Buffer& buffer) noexcept
{
    Buffer empty;
    auto* control = buffer.controlBlock;
    INVARIANT(controlBlock != control, "Cannot attach buffer to self");
    const auto index = controlBlock->storeChildBuffer(control);
    std::swap(empty, buffer);
    return index;
}

Buffer Buffer::loadChildBuffer(ChildBufferIndex bufferIndex) const noexcept
{
    Buffer childBuffer;
    const auto ret = controlBlock->loadChildBuffer(bufferIndex, childBuffer.controlBlock, childBuffer.ptr, childBuffer.size);
    INVARIANT(ret, "Cannot load tuple buffer with index={}", bufferIndex);
    return childBuffer;
}

bool recycleBuffer(void* bufferPointer)
{
    PRECONDITION(bufferPointer, "invalid bufferPointer");
    auto buffer = reinterpret_cast<uint8_t*>(bufferPointer);
    auto block = reinterpret_cast<detail::BufferControlBlock*>(buffer - sizeof(detail::BufferControlBlock));
    return block->release();
}

void swap(Buffer& lhs, Buffer& rhs) noexcept
{
    /// Enable ADL to spell out to onlookers how swap should be used.
    using std::swap;

    swap(lhs.ptr, rhs.ptr);
    swap(lhs.size, rhs.size);
    swap(lhs.controlBlock, rhs.controlBlock);
}

std::ostream& operator<<(std::ostream& os, const Buffer& buff) noexcept
{
    return os << reinterpret_cast<std::uintptr_t>(buff.ptr);
}

uint64_t Buffer::getNumberOfTuples() const noexcept
{
    return controlBlock->getNumberOfTuples();
}

OriginId Buffer::getOriginId() const noexcept
{
    return controlBlock->getOriginId();
}

uint32_t Buffer::getNumberOfChildBuffers() const noexcept
{
    return controlBlock->getNumberOfChildBuffers();
}

ChunkNumber Buffer::getChunkNumber() const noexcept
{
    return controlBlock->getChunkNumber();
}

}
