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

#ifndef INCLUDE_TUPLEBUFFER_H_
#define INCLUDE_TUPLEBUFFER_H_

#include <NodeEngine/detail/TupleBufferImpl.hpp>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

/// Check: not zero and `v` has got no 1 in common with `v - 1`.
/// Making use of short-circuit evaluation here because otherwise v-1 might be an underflow.
/// TODO: switch to std::ispow2 when we use C++2a.
template<std::size_t v>
static constexpr bool ispow2 = (!!v) && !(v & (v - 1));

namespace NES::Network {
class OutputChannel;
}

namespace NES::NodeEngine {
/**
 * @brief The TupleBuffer allows runtime components to access memory to store records in a reference-counted and
 * thread-safe manner.
 *
 * The purpose of the TupleBuffer is to zero memory allocations and enable batching.
 * In order to zero the memory allocation, a BufferManager keeps a fixed set of fixed-size buffers that it hands out to
 * components.
 * A TupleBuffer's content is automatically recycled or deleted once its reference count reaches zero.
 *
 * Prefer passing the TupleBuffer by reference whenever possible, pass the TupleBuffer to another thread by value.
 *
 * Important note: when a component is done with a TupleBuffer, it must be released. Not returning a TupleBuffer will
 * result in a runtime error that the BufferManager will raise by the termination of the NES program.
 *
 * Reminder: this class should be header-only to help inlining
 */

class [[nodiscard]] TupleBuffer {
    /// Utilize the wrapped-memory constructor
    friend class BufferManager;
    friend class FixedSizeBufferPool;
    friend class LocalBufferPool;
    friend class detail::MemorySegment;

    /// Utilize the wrapped-memory constructor and requires direct access to the control block for the ZMQ sink.
    friend class Network::OutputChannel;

  public:
    /**
     * @brief Creates a TupleBuffer of length bytes starting at ptr address. The parent will be notified of the buffer release. Only at that point, the ptr memory area can be freed, which must be done by the user.
     * @param ptr
     * @param length
     * @param parent
     * @return
     */
    static TupleBuffer wrapMemory(uint8_t* ptr, size_t length, BufferRecycler* parent);

    TupleBuffer(const TupleBuffer& other) noexcept;

    TupleBuffer(TupleBuffer&& other) noexcept;

    TupleBuffer& operator=(const TupleBuffer& other) noexcept;

    TupleBuffer& operator=(TupleBuffer&& other) noexcept;

    TupleBuffer() noexcept;

    friend void swap(TupleBuffer& lhs, TupleBuffer& rhs) noexcept;

  private:
    explicit TupleBuffer(detail::BufferControlBlock* controlBlock, uint8_t* ptr, uint32_t size) noexcept;

  public:
    ~TupleBuffer() noexcept;

    /**
    * @return the content of the buffer as pointer to unsigned char
    */
    template<typename T = uint8_t> inline T* getBuffer() noexcept {
        static_assert(alignof(T) <= alignof(std::max_align_t), "Alignment of type T is stricter than allowed.");
        static_assert(ispow2<alignof(T)>);
        return reinterpret_cast<T*>(ptr);
    }

    /**
    * @return true if the interal pointer is not null
    */
    bool isValid() const noexcept;

    TupleBuffer* operator&() = delete;

    uint64_t getBufferSize() const noexcept;

    uint64_t getNumberOfTuples() const noexcept;

    void setNumberOfTuples(uint64_t numberOfTuples) noexcept;

    /// @brief Print the buffer's address.
    friend std::ostream& operator<<(std::ostream& os, const TupleBuffer& buff) noexcept{
        // TODO: C++2a: change to std::bit_cast
        return os << reinterpret_cast<std::uintptr_t>(buff.ptr);
    }

    /**
    * @brief Increases the internal reference counter by one
    * @return the same TupleBuffer object
    */
    TupleBuffer& retain() noexcept;

    /**
    * @brief Decreases the internal reference counter by one. Note that if the buffer's counter reaches 0
    * then the TupleBuffer is not usable any longer.
    */
    void release() noexcept;

    /**
     * @brief method to get the watermark as a timestamp
     * @return watermark
     */
    uint64_t getWatermark() const noexcept;

    /**
     * @brief method to set the watermark with a timestamp
     * @param value timestamp
     */
    void setWatermark(uint64_t value) noexcept;

    /**
     * @brief method to set the watermark with a timestamp
     * @param value timestamp
     */
    void setCreationTimestamp(uint64_t value) noexcept;

    /**
     * @brief method to get the creation timestamp
     * @return ts
     */
    uint64_t getCreationTimestamp() const noexcept;

    /**
     * @brief set the origin id for the buffer (the operator id that creates this buffer)
     * @param origin id
     */
    void setOriginId(uint64_t id) noexcept;

    /**
     * @brief returns the origin id of the buffer
     * @return origin id
     */
    uint64_t getOriginId() const noexcept;

  private:
    /**
     * @brief returns the control block of the buffer USE THIS WITH CAUTION!
     */
    detail::BufferControlBlock* getControlBlock() const { return controlBlock; }

  private:
    detail::BufferControlBlock* controlBlock = nullptr;
    uint8_t* ptr = nullptr;
    uint32_t size = 0;
};

}// namespace NES::NodeEngine
#endif /* INCLUDE_TUPLEBUFFER_H_ */
