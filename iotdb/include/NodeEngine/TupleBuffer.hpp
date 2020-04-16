#ifndef INCLUDE_TUPLEBUFFER_H_
#define INCLUDE_TUPLEBUFFER_H_
#include <cstdint>
#include <memory>
#include <functional>
#include <atomic>
#include <API/Schema.hpp>

namespace NES {
class BufferManager;
class TupleBuffer;

namespace detail {
std::string printTupleBuffer(TupleBuffer& buffer, SchemaPtr schema);
void revertEndianness(TupleBuffer& buffer, SchemaPtr schema);
}

namespace detail {
class MemorySegment;
class BufferControlBlock {
  public:
    explicit BufferControlBlock(MemorySegment* owner, std::function<void(MemorySegment*)>&& recycleCallback) :
        referenceCounter(0), tupleSizeInBytes(0), numberOfTuples(0), owner(owner), recycleCallback(recycleCallback) {
    }

    BufferControlBlock(const BufferControlBlock& that) {
        referenceCounter.store(that.referenceCounter.load());
        tupleSizeInBytes.store(that.tupleSizeInBytes.load());
        numberOfTuples.store(that.numberOfTuples.load());
        recycleCallback = that.recycleCallback;
        owner = that.owner;
    }

    BufferControlBlock& operator=(const BufferControlBlock& that) {
        referenceCounter.store(that.referenceCounter.load());
        tupleSizeInBytes.store(that.tupleSizeInBytes.load());
        numberOfTuples.store(that.numberOfTuples.load());
        recycleCallback = that.recycleCallback;
        owner = that.owner;
        return *this;
    }

    MemorySegment* getOwner() {
        return owner;
    }

    bool prepare() {
        uint32_t expected = 0;
        return referenceCounter.compare_exchange_strong(expected, 1);
    }

    BufferControlBlock* retain() {
        referenceCounter++;
        return this;
    }

    uint32_t getReferenceCount() {
        return referenceCounter.load();
    }

    bool release() {
        uint32_t prev;
        if ((prev = referenceCounter.fetch_sub(1)) == 1) {
            tupleSizeInBytes.store(0);
            numberOfTuples.store(0);
            recycleCallback(owner);
            return true;
        }
        if (prev == 0) {
            assert(false);
        }
        return false;
    }

    size_t getTupleSizeInBytes() const {
        return tupleSizeInBytes;
    }

    size_t getNumberOfTuples() const {
        return numberOfTuples;
    }

    void setNumberOfTuples(size_t numberOfTuples) {
        this->numberOfTuples = numberOfTuples;
    }

    void setTupleSizeInBytes(size_t tupleSizeInBytes) {
        this->tupleSizeInBytes = tupleSizeInBytes;
    }

  private:
    std::atomic<uint32_t> referenceCounter;
    // TODO check whether to remove them
    std::atomic<uint32_t> tupleSizeInBytes;
    std::atomic<uint32_t> numberOfTuples;
    MemorySegment* owner;
    std::function<void(MemorySegment*)> recycleCallback;
};
}

class TupleBuffer {
    friend class NES::BufferManager;
    friend class detail::MemorySegment;
  private:
    TupleBuffer() noexcept : ptr(nullptr), size(0), controlBlock(nullptr) {
        // nop
    }
  public:
    TupleBuffer(const TupleBuffer& other) noexcept : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size) {
        if (controlBlock) {
            controlBlock->retain();
        }
    }

    TupleBuffer(TupleBuffer&& other) noexcept : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size) {
        other.controlBlock = nullptr;
        other.ptr = nullptr;
        other.size = 0;
    }

    TupleBuffer& operator=(const TupleBuffer& other) {
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

    TupleBuffer& operator=(TupleBuffer&& other) {
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
  private:
    explicit TupleBuffer(detail::BufferControlBlock* controlBlock, uint8_t* ptr, uint32_t size) :
        controlBlock(controlBlock), ptr(ptr), size(size) {
    }
  public:
    ~TupleBuffer() {
        release();
    }

    /**
     * @return the content of the buffer as pointer to unsigned char
     */
    uint8_t* getBuffer() {
        return ptr;
    }

    /**
     * @return true if the interal pointer is not null
     */
    bool isValid() const {
        return ptr != nullptr;
    }

    TupleBuffer* operator&() = delete;

    /**
     * @tparam T
     * @return the content of the buffer as pointer to T
     */
    template<typename T>
    T* getBufferAs() {
        return reinterpret_cast<T*>(ptr);
    }

    /**
     * @brief Increases the internal reference counter by one
     * @return the same TupleBuffer object
     */
    TupleBuffer& retain() {
        controlBlock->retain();
        return *this;
    }

    /**
     * @brief Swaps this TupleBuffers with the content of other
     * @param other
     */
    void swap(TupleBuffer& other) noexcept {
        std::swap(ptr, other.ptr);
        std::swap(size, other.size);
        std::swap(controlBlock, other.controlBlock);
    }

    /**
    * @brief Decreases the internal reference counter by one. Note that if the buffer's counter reaches 0
    * then the TupleBuffer is not usable any longer.
    */
    void release() {
        if (controlBlock && controlBlock->release()) {
            controlBlock = nullptr;
            ptr = nullptr;
            size = 0;
        }
    }

    size_t getBufferSize() const {
        return size;
    }

    size_t getTupleSizeInBytes() const {
        return controlBlock->getTupleSizeInBytes();
    }

    size_t getNumberOfTuples() const {
        return controlBlock->getNumberOfTuples();
    }

    void setNumberOfTuples(size_t numberOfTuples) {
        controlBlock->setNumberOfTuples(numberOfTuples);
    }

    void setTupleSizeInBytes(size_t tupleSizeInBytes) {
        controlBlock->setTupleSizeInBytes(tupleSizeInBytes);
    }

    friend std::ostream& operator<<(std::ostream& os, const TupleBuffer& buff) {
        return os << buff.ptr;
    }

    /**
     * @brief this method creates a string from the content of a tuple buffer
     * @return string of the buffer content
     */
    std::string printTupleBuffer(Schema schema) {
        return detail::printTupleBuffer(*this, schema);
    }

    /**
      * @brief revert the endianess of the tuple buffer
      * @schema of the buffer
      */
    void revertEndianness(Schema schema) {
        detail::revertEndianness(*this, schema);
    }

  private:
    detail::BufferControlBlock* controlBlock;
    uint8_t* ptr;
    uint32_t size;
};

namespace detail {
class MemorySegment {
    friend class NES::TupleBuffer;
    friend class NES::BufferManager;
  public:
    MemorySegment(const MemorySegment& other) : ptr(other.ptr), size(other.size), controlBlock(other.controlBlock) {
    }

    MemorySegment& operator=(const MemorySegment& other) {
        ptr = other.ptr;
        size = other.size;
        controlBlock = other.controlBlock;
        return *this;
    }

    MemorySegment(MemorySegment&& other) = delete;

    MemorySegment& operator=(MemorySegment&& other) = delete;

    MemorySegment() : ptr(nullptr), size(0), controlBlock(nullptr, [](MemorySegment*) {}) {}

    explicit MemorySegment(uint8_t* ptr, uint32_t size, std::function<void(MemorySegment*)>&& recycleFunction)
        : ptr(ptr), size(size), controlBlock(this, std::move(recycleFunction)) {
        assert(this->ptr != nullptr);
        assert(this->size > 0);
    }

    ~MemorySegment() {
        if (ptr) {
            auto refCnt = controlBlock.getReferenceCount();
            if (refCnt != 0) {
                assert(false);
            }
            free(ptr);
            ptr = nullptr;
        }
    }
  private:
    TupleBuffer toTupleBuffer() {
        if (controlBlock.prepare()) {
            return TupleBuffer(&controlBlock, ptr, size);
        } else {
            assert(false);
        }
    }

    bool isAvailable() {
        return controlBlock.getReferenceCount() == 0;
    }

    uint32_t getSize() const {
        return size;
    }

  private:
    uint8_t* ptr;
    uint32_t size;
    detail::BufferControlBlock controlBlock;
};


std::string toString(TupleBuffer& buffer, SchemaPtr schema);

}  // namespace NES
#endif /* INCLUDE_TUPLEBUFFER_H_ */
