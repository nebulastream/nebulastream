/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <NodeEngine/BufferRecycler.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/detail/TupleBufferImpl.hpp>
#include <Util/Logger.hpp>
namespace NES::NodeEngine {

TupleBuffer TupleBuffer::wrapMemory(uint8_t* ptr, size_t length, BufferRecycler* parent) {
    auto callback = [](detail::MemorySegment* segment, BufferRecycler* recycler) {
        recycler->recyclePooledBuffer(segment);
    };
    auto memSegment = new detail::MemorySegment(ptr, length, parent, callback, true);
    return TupleBuffer(memSegment->controlBlock, ptr, length);
}

TupleBuffer::TupleBuffer(detail::BufferControlBlock* controlBlock, uint8_t* ptr, uint32_t size) noexcept
    : controlBlock(controlBlock), ptr(ptr), size(size) {
    // nop
}

TupleBuffer::TupleBuffer(const TupleBuffer& other) noexcept : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size) {
    if (controlBlock) {
        controlBlock->retain();
    }
}

TupleBuffer::TupleBuffer(TupleBuffer&& other) noexcept : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size) {
    other.controlBlock = nullptr;
    other.ptr = nullptr;
    other.size = 0;
}

TupleBuffer& TupleBuffer::operator=(const TupleBuffer& other) noexcept {
    if PLACEHOLDER_UNLIKELY (this == std::addressof(other)) {
        return *this;
    }

    // Override the content of this with those of `other`
    auto const oldControlBlock = std::exchange(controlBlock, other.controlBlock);
    ptr = other.ptr;
    size = other.size;

    // Update reference counts: If the new and old controlBlocks differ, retain the new one and release the old one.
    if (oldControlBlock != controlBlock) {
        retain();

        if (oldControlBlock) {
            oldControlBlock->release();
        }
    }
    return *this;
}

TupleBuffer& TupleBuffer::operator=(TupleBuffer&& other) noexcept {

    // Especially for rvalues, the following branch should most likely never be taken if the caller writes
    // reasonable code. Therefore, this branch is considered unlikely.
    if PLACEHOLDER_UNLIKELY (this == std::addressof(other)) {
        return *this;
    }

    // Swap content of this with those of `other` to let the other's destructor take care of releasing the overwritten
    // resource.
    using std::swap;
    swap(*this, other);

    return *this;
}

TupleBuffer::~TupleBuffer() noexcept { release(); }

bool TupleBuffer::isValid() const noexcept { return ptr != nullptr; }

TupleBuffer& TupleBuffer::retain() noexcept {
    controlBlock->retain();
    return *this;
}

void TupleBuffer::release() noexcept {
    if (controlBlock) {
        controlBlock->release();
    }
    controlBlock = nullptr;
    ptr = nullptr;
    size = 0;
}

uint64_t TupleBuffer::getBufferSize() const noexcept { return size; }

uint64_t TupleBuffer::getNumberOfTuples() const noexcept { return controlBlock->getNumberOfTuples(); }

void TupleBuffer::setNumberOfTuples(uint64_t numberOfTuples) noexcept { controlBlock->setNumberOfTuples(numberOfTuples); }

uint64_t TupleBuffer::getWatermark() const noexcept { return controlBlock->getWatermark(); }

uint64_t TupleBuffer::getOriginId() const noexcept { return controlBlock->getOriginId(); }

void TupleBuffer::setOriginId(uint64_t id) noexcept { controlBlock->setOriginId(id); }

void TupleBuffer::setWatermark(uint64_t value) noexcept { controlBlock->setWatermark(value); }

void TupleBuffer::setCreationTimestamp(uint64_t value) noexcept { controlBlock->setCreationTimestamp(value); }

uint64_t TupleBuffer::getCreationTimestamp() const noexcept { return controlBlock->getCreationTimestamp(); }

void swap(TupleBuffer& lhs, TupleBuffer& rhs) noexcept {
    // Enable ADL to spell out to onlookers how swap should be used.
    using std::swap;

    swap(lhs.ptr, rhs.ptr);
    swap(lhs.size, rhs.size);
    swap(lhs.controlBlock, rhs.controlBlock);
}

}// namespace NES::NodeEngine