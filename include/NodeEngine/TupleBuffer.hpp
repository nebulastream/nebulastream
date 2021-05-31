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
    friend class BufferManager;
    friend class detail::MemorySegment;
    friend class LocalBufferPool;
    friend class FixedSizeBufferPool;
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

    TupleBuffer& operator=(const TupleBuffer& other);

    TupleBuffer& operator=(TupleBuffer&& other);

    TupleBuffer() noexcept;

    friend void swap(TupleBuffer& lhs, TupleBuffer& rhs);

  private:
    explicit TupleBuffer(detail::BufferControlBlock* controlBlock, uint8_t* ptr, uint32_t size);

  public:
    ~TupleBuffer();

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
    bool isValid() const;

    TupleBuffer* operator&() = delete;

    /**
    * @brief Increases the internal reference counter by one
    * @return the same TupleBuffer object
    */
    TupleBuffer& retain();

    /**
    * @brief Decreases the internal reference counter by one. Note that if the buffer's counter reaches 0
    * then the TupleBuffer is not usable any longer.
    */
    void release();

    uint64_t getBufferSize() const;

    uint64_t getNumberOfTuples() const;

    void setNumberOfTuples(uint64_t numberOfTuples);

    /// @brief Print the buffer's address.
    friend std::ostream& operator<<(std::ostream& os, const TupleBuffer& buff) {
        return os << reinterpret_cast<std::uintptr_t>(buff.ptr);
    }

    /**
     * @brief method to get the watermark as a timestamp
     * @return watermark
     */
    uint64_t getWatermark() const;

    /**
     * @brief method to set the watermark with a timestamp
     * @param value timestamp
     */
    void setWatermark(uint64_t value);

    /**
     * @brief method to set the watermark with a timestamp
     * @param value timestamp
     */
    void setCreationTimestamp(uint64_t value);

    /**
     * @brief method to get the creation timestamp
     * @return ts
     */
    uint64_t getCreationTimestamp() const noexcept;

    /**
     * @brief set the origin id for the buffer (the operator id that creates this buffer)
     * @param origin id
     */
    void setOriginId(uint64_t id);

    /**
     * @brief returns the origin id of the buffer
     * @return origin id
     */
    uint64_t getOriginId() const;

  private:
    /**
     * @brief returns the control block of the buffer USE THIS WITH CAUTION!
     * @return
     */
    detail::BufferControlBlock* getControlBlock() const { return controlBlock; }

  private:
    detail::BufferControlBlock* controlBlock;
    uint8_t* ptr;
    uint32_t size;
};

}// namespace NES::NodeEngine
#endif /* INCLUDE_TUPLEBUFFER_H_ */
