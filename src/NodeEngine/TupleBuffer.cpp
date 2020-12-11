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

#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/detail/TupleBufferImpl.hpp>
#include <Util/Logger.hpp>

namespace NES::NodeEngine {

TupleBuffer::TupleBuffer() noexcept : ptr(nullptr), size(0), controlBlock(nullptr) {
    //nop
}

TupleBuffer::TupleBuffer(detail::BufferControlBlock* controlBlock, uint8_t* ptr, uint32_t size)
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

TupleBuffer& TupleBuffer::operator=(const TupleBuffer& other) {
    if (this == &other) {
        return *this;
    }
    controlBlock = other.controlBlock;
    ptr = other.ptr;
    size = other.size;
    if (controlBlock) {
        controlBlock->retain();
    }
    return *this;
}

TupleBuffer& TupleBuffer::operator=(TupleBuffer&& other) {
    if (this == std::addressof(other)) {
        return *this;
    }
    if (controlBlock) {
        controlBlock->release();
    }
    controlBlock = other.controlBlock;
    ptr = other.ptr;
    size = other.size;
    other.controlBlock = nullptr;
    other.ptr = nullptr;
    other.size = 0;
    return *this;
}

TupleBuffer::~TupleBuffer() { release(); }

bool TupleBuffer::isValid() const { return ptr != nullptr; }

TupleBuffer& TupleBuffer::retain() {
    controlBlock->retain();
    return *this;
}

void TupleBuffer::release() {
    if (controlBlock && controlBlock->release()) {
        controlBlock = nullptr;
        ptr = nullptr;
        size = 0;
    }
}

uint64_t TupleBuffer::getBufferSize() const { return size; }

uint64_t TupleBuffer::getNumberOfTuples() const { return controlBlock->getNumberOfTuples(); }

void TupleBuffer::setNumberOfTuples(uint64_t numberOfTuples) { controlBlock->setNumberOfTuples(numberOfTuples); }

void TupleBuffer::revertEndianness(SchemaPtr schema) { detail::revertEndianness(*this, schema); }

int64_t TupleBuffer::getWatermark() const { return controlBlock->getWatermark(); }

uint64_t TupleBuffer::getOriginId() const { return controlBlock->getOriginId(); }

void TupleBuffer::setOriginId(uint64_t id) { controlBlock->setOriginId(id); }

void TupleBuffer::setWatermark(int64_t value) { controlBlock->setWatermark(value); }

void swap(TupleBuffer& lhs, TupleBuffer& rhs) {
    std::swap(lhs.ptr, rhs.ptr);
    std::swap(lhs.size, rhs.size);
    std::swap(lhs.controlBlock, rhs.controlBlock);
}

}// namespace NES::NodeEngine