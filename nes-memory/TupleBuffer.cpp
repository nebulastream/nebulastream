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

#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include "TupleBufferImpl.hpp"
namespace NES::Memory
{

TupleBuffer TupleBuffer::reinterpretAsTupleBuffer(void* bufferPointer)
{
    auto controlBlockSize = alignBufferSize(sizeof(detail::BufferControlBlock), 64);
    auto buffer = reinterpret_cast<uint8_t*>(bufferPointer);
    auto block = reinterpret_cast<detail::BufferControlBlock*>(buffer - controlBlockSize);
    auto memorySegment = block->getOwner();
    auto tb = TupleBuffer(memorySegment->controlBlock.get(), memorySegment->ptr, memorySegment->size);
    tb.retain();
    return tb;
}

TupleBuffer TupleBuffer::wrapMemory(uint8_t* ptr, size_t length, BufferRecycler* parent)
{
    auto callback = [](detail::MemorySegment* segment, BufferRecycler* recycler)
    {
        recycler->recyclePooledBuffer(segment);
        delete segment;
    };
    auto* memSegment = new detail::MemorySegment(ptr, length, parent, std::move(callback), true);
    return TupleBuffer(memSegment->controlBlock.get(), ptr, length);
}

TupleBuffer TupleBuffer::wrapMemory(uint8_t* ptr, size_t length, std::function<void(detail::MemorySegment*, BufferRecycler*)>&& callback)
{
    auto* memSegment = new detail::MemorySegment(ptr, length, nullptr, std::move(callback), true);
    return TupleBuffer(memSegment->controlBlock.get(), ptr, length);
}
TupleBuffer::TupleBuffer(TupleBuffer const& other) noexcept : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size)
{
    if (controlBlock)
    {
        controlBlock->retain();
    }
}
TupleBuffer& TupleBuffer::operator=(TupleBuffer const& other) noexcept
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
TupleBuffer& TupleBuffer::retain() noexcept
{
    if (controlBlock)
    {
        controlBlock->retain();
    }
    return *this;
}
void TupleBuffer::release() noexcept
{
    if (controlBlock)
    {
        controlBlock->release();
    }
    controlBlock = nullptr;
    ptr = nullptr;
    size = 0;
}

int8_t* TupleBuffer::getBuffer() noexcept
{
    return getBuffer<int8_t>();
}
uint32_t TupleBuffer::getReferenceCounter() const noexcept
{
    return controlBlock ? controlBlock->getReferenceCount() : 0;
}
uint64_t TupleBuffer::getBufferSize() const noexcept
{
    return size;
}
void TupleBuffer::setNumberOfTuples(uint64_t numberOfTuples) noexcept
{
    controlBlock->setNumberOfTuples(numberOfTuples);
}
Runtime::Timestamp TupleBuffer::getWatermark() const noexcept
{
    return controlBlock->getWatermark();
}
void TupleBuffer::setWatermark(Runtime::Timestamp value) noexcept
{
    controlBlock->setWatermark(value);
}
Runtime::Timestamp TupleBuffer::getCreationTimestampInMS() const noexcept
{
    return controlBlock->getCreationTimestamp();
}
void TupleBuffer::setSequenceNumber(SequenceNumber sequenceNumber) noexcept
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
void TupleBuffer::setChunkNumber(ChunkNumber chunkNumber) noexcept
{
    controlBlock->setChunkNumber(chunkNumber);
}
void TupleBuffer::setLastChunk(bool isLastChunk) noexcept
{
    controlBlock->setLastChunk(isLastChunk);
}
bool TupleBuffer::isLastChunk() const noexcept
{
    return controlBlock->isLastChunk();
}
void TupleBuffer::setCreationTimestampInMS(Runtime::Timestamp value) noexcept
{
    controlBlock->setCreationTimestamp(value);
}
void TupleBuffer::setOriginId(OriginId id) noexcept
{
    controlBlock->setOriginId(id);
}
void TupleBuffer::addRecycleCallback(std::function<void(detail::MemorySegment*, BufferRecycler*)> newCallback) noexcept
{
    controlBlock->addRecycleCallback(std::move(newCallback));
}

uint32_t TupleBuffer::storeChildBuffer(TupleBuffer& buffer) const noexcept
{
    TupleBuffer empty;
    auto* control = buffer.controlBlock;
    INVARIANT(controlBlock != control, "Cannot attach buffer to self");
    auto index = controlBlock->storeChildBuffer(control);
    std::swap(empty, buffer);
    return index;
}

TupleBuffer TupleBuffer::loadChildBuffer(NestedTupleBufferKey bufferIndex) const noexcept
{
    TupleBuffer childBuffer;
    INVARIANT(
        controlBlock->loadChildBuffer(bufferIndex, childBuffer.controlBlock, childBuffer.ptr, childBuffer.size),
        "Cannot load tuple buffer");
    return childBuffer;
}

bool recycleTupleBuffer(void* bufferPointer)
{
    PRECONDITION(bufferPointer, "invalid bufferPointer");
    auto buffer = reinterpret_cast<uint8_t*>(bufferPointer);
    auto block = reinterpret_cast<detail::BufferControlBlock*>(buffer - sizeof(detail::BufferControlBlock));
    return block->release();
}

bool TupleBuffer::hasSpaceLeft(uint64_t used, uint64_t needed) const
{
    if (used + needed <= this->size)
    {
        return true;
    }
    return false;
}
void swap(TupleBuffer& lhs, TupleBuffer& rhs) noexcept
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
    return controlBlock->getNumberOfChildrenBuffer();
}
ChunkNumber TupleBuffer::getChunkNumber() const noexcept
{
    return controlBlock->getChunkNumber();
}


}
