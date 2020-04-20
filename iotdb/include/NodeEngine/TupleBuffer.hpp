#ifndef INCLUDE_TUPLEBUFFER_H_
#define INCLUDE_TUPLEBUFFER_H_

#include <cstdint>
#include <memory>
#include <functional>
#include <atomic>
#include <API/Schema.hpp>
#include <NodeEngine/detail/TupleBufferImpl.hpp>

namespace NES {
class BufferManager;
class TupleBuffer;

/**
 * @brief The TupleBuffer is the NES API that allows runtime components to access memory to store records
 * The purpose of the TupleBuffer is to zero memory allocations and enable batching. To zero the memory allocation
 * a BufferManager keeps a fixed set of fixed-size buffers that can be handed to components.
 * When a component is done with a buffer, it can be returned to the BufferManager.
 * However, the multi-threaded nature of NES execution engine introduces the challenge of reference counting: i.e.,
 * if a tuple buffer is shared among threads, we need to safely determine when the TupleBuffer can be returned to the
 * BufferManager. Our solution is as follows. We follow the same principle behind std::shared_ptr.
 * We use the TupleBuffer as a normal lvalue. Note that TupleBuffer* is forbidden and operator& of TupleBuffer is deleted.
 * We increase the reference counter of the TupleBuffer every time it gets passed via a copy ctor or a copy assignment
 * operator. When the dtor of a TupleBuffer is called then the reference counter is decreased.
 * To pass the TupleBuffer within a thread, a TupleBuffer& must be used.
 * To pass the TupleBuffer among thread, use TupleBuffer. Ensure the copy semantics is used. The move semantics is enabled
 * but that does not increase the internal counter.
 * Important note: when a component is done with a TupleBuffer, it must be released. Not returning a TupleBuffer will
 * result in a runtime error that the BufferManager will raise by the termination of the NES program.
 *
 * Reminder: this class should be header-only to help inlining
 */
class TupleBuffer {
    friend class NES::BufferManager;

    friend class detail::MemorySegment;

  private:
    TupleBuffer() noexcept;

  public:
    TupleBuffer(const TupleBuffer& other) noexcept;

    TupleBuffer(TupleBuffer&& other) noexcept;

    TupleBuffer& operator=(const TupleBuffer& other);

    TupleBuffer& operator=(TupleBuffer&& other);

  private:
    explicit TupleBuffer(detail::BufferControlBlock* controlBlock, uint8_t* ptr, uint32_t size);

  public:
    ~TupleBuffer();

    /**
    * @return the content of the buffer as pointer to unsigned char
    */
    template <typename T = uint8_t>
    T* getBuffer() {
        return reinterpret_cast<T*>(ptr);
    }

    /**
    * @return true if the interal pointer is not null
    */
    bool isValid() const;

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
    TupleBuffer& retain();

    /**
    * @brief Swaps this TupleBuffers with the content of other
    * @param other
    */
    void swap(TupleBuffer& other) noexcept;

    /**
    * @brief Decreases the internal reference counter by one. Note that if the buffer's counter reaches 0
    * then the TupleBuffer is not usable any longer.
    */
    void release();

    size_t getBufferSize() const;

    size_t getTupleSizeInBytes() const;

    size_t getNumberOfTuples() const;

    void setNumberOfTuples(size_t numberOfTuples);


    void setTupleSizeInBytes(size_t tupleSizeInBytes);

    friend std::ostream& operator<<(std::ostream& os, const TupleBuffer& buff) {
        return os << buff.ptr;
    }

    /**
    * @brief this method creates a string from the content of a tuple buffer
    * @return string of the buffer content
    */
    std::string printTupleBuffer(SchemaPtr schema);

    /**
    * @brief revert the endianess of the tuple buffer
    * @schema of the buffer
    */
    void revertEndianness(SchemaPtr schema);

  private:
    detail::BufferControlBlock* controlBlock;
    uint8_t* ptr;
    uint32_t size;
};

std::string toString(TupleBuffer& buffer, SchemaPtr schema);

}  // namespace NES
#endif /* INCLUDE_TUPLEBUFFER_H_ */
