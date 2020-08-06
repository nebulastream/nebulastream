#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/detail/TupleBufferImpl.hpp>
#include <Util/Logger.hpp>

namespace NES {

TupleBuffer::TupleBuffer() noexcept : ptr(nullptr), size(0), controlBlock(nullptr), watermark(0) {
    //nop
}

TupleBuffer::TupleBuffer(detail::BufferControlBlock* controlBlock, uint8_t* ptr, uint32_t size)
    : controlBlock(controlBlock), ptr(ptr), size(size), watermark(0) {
    // nop
}

TupleBuffer::TupleBuffer(const TupleBuffer& other) noexcept : controlBlock(other.controlBlock), ptr(other.ptr),
                                                              size(other.size), watermark(other.watermark) {
    if (controlBlock) {
        controlBlock->retain();
    }
}

TupleBuffer::TupleBuffer(TupleBuffer&& other) noexcept
    : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size) {
    other.controlBlock = nullptr;
    other.ptr = nullptr;
    other.size = 0;
    other.watermark = 0;
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

size_t TupleBuffer::getWatermark()
{
    return watermark;
}

void TupleBuffer::setWatermark(size_t value)
{
    watermark = value;
}


}// namespace NES