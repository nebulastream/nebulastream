#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/detail/TupleBufferImpl.hpp>
#include <Util/Logger.hpp>

namespace NES {

TupleBuffer::TupleBuffer() noexcept : ptr(nullptr), size(0), controlBlock(nullptr) {
    //nop
}

TupleBuffer::TupleBuffer(detail::BufferControlBlock* controlBlock, uint8_t* ptr, uint32_t size)
    : controlBlock(controlBlock), ptr(ptr), size(size) {
    // nop
}

TupleBuffer::TupleBuffer(const TupleBuffer& other) noexcept : controlBlock(other.controlBlock), ptr(other.ptr),
                                                              size(other.size) {
    if (controlBlock) {
        controlBlock->retain();
    }
}

TupleBuffer::TupleBuffer(TupleBuffer&& other) noexcept
    : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size) {
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
    controlBlock = other.controlBlock;
    ptr = other.ptr;
    size = other.size;
    other.controlBlock = nullptr;
    other.ptr = nullptr;
    other.size = 0;
    return *this;
}

TupleBuffer::~TupleBuffer() {
    release();
}

bool TupleBuffer::isValid() const {
    return ptr != nullptr;
}

TupleBuffer& TupleBuffer::retain() {
    controlBlock->retain();
    return *this;
}

void TupleBuffer::swap(TupleBuffer& other) noexcept {
    std::swap(ptr, other.ptr);
    std::swap(size, other.size);
    std::swap(controlBlock, other.controlBlock);
}

void TupleBuffer::release() {
    if (controlBlock && controlBlock->release()) {
        controlBlock = nullptr;
        ptr = nullptr;
        size = 0;
    }
}

size_t TupleBuffer::getBufferSize() const {
    return size;
}

size_t TupleBuffer::getNumberOfTuples() const {
    return controlBlock->getNumberOfTuples();
}

void TupleBuffer::setNumberOfTuples(size_t numberOfTuples) {
    controlBlock->setNumberOfTuples(numberOfTuples);
}

void TupleBuffer::revertEndianness(SchemaPtr schema) {
    detail::revertEndianness(*this, schema);
}

int64_t TupleBuffer::getWatermark() {
    return controlBlock->getWatermark();
}

void TupleBuffer::setWatermark(int64_t value) {
    controlBlock->setWatermark(value);
}

}// namespace NES