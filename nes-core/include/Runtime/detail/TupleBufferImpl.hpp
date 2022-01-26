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

#ifndef NES_INCLUDE_RUNTIME_DETAIL_TUPLEBUFFERIMPL_HPP_
#define NES_INCLUDE_RUNTIME_DETAIL_TUPLEBUFFERIMPL_HPP_

#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <atomic>
#include <functional>

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#endif

namespace NES {
using WatermarkTs = uint64_t;
using OriginId = uint64_t;
using SequenceNumber = uint64_t;

namespace Runtime {
class BufferManager;
class LocalBufferPool;
class TupleBuffer;
class FixedSizeBufferPool;
class BufferRecycler;

namespace detail {

class MemorySegment;

#define PLACEHOLDER_LIKELY   /* XXX: replace with [[likely]] when using C++ 2a */
#define PLACEHOLDER_UNLIKELY /* XXX: replace with [[unlikely]] when using C++ 2a */

/**
 * @brief This class provides a convenient way to track the reference counter as well metadata for its owning
 * MemorySegment/TupleBuffer. In particular, it stores the atomic reference counter that tracks how many
 * live reference exists of the owning MemorySegment/TupleBuffer and it also stores the callback to execute
 * when the reference counter reaches 0.
 *
 * Reminder: this class should be header-only to help inlining
 */
class BufferControlBlock {
  public:
    explicit BufferControlBlock(MemorySegment* owner,
                                BufferRecycler* recycler,
                                std::function<void(MemorySegment*, BufferRecycler*)>&& recycleCallback);

    BufferControlBlock(const BufferControlBlock&);

    BufferControlBlock& operator=(const BufferControlBlock&);

    /**
     * @brief Returns the underlying owning segment
     * @return the underlying owning segment
     */
    MemorySegment* getOwner() const;

    /**
     * @brief Resets the recycler
     * @param recycler the new recycler
     */
    void resetBufferRecycler(BufferRecycler* recycler);

    /**
     * @brief Add recycle callback to be called upon reaching 0 as ref cnt
     * @param func the callbac to call
     */
    void addRecycleCallback(std::function<void(MemorySegment*, BufferRecycler*)>&& func) noexcept;

    /**
     * @brief This method must be called before the BufferManager hands out a TupleBuffer. It ensures that the internal
     * reference counter is zero. If that's not the case, an exception is thrown.
     * @return true if the mem segment can be used to create a TupleBuffer.
     */
    bool prepare();

    /**
     * @brief Increase the reference counter by one.
     * @return this
     */
    BufferControlBlock* retain();

    /**
     * @return get the reference counter
     */
    [[nodiscard]] int32_t getReferenceCount() const noexcept;

    /**
     * @brief Decrease the reference counter by one.
     * @return true if 0 is reached and the buffer is recycled
     */
    bool release();

    /**
    * @brief returns the number of tuples stored in the companion buffer
    * Note that this is going to be deprecated in future NES versions
    * @return the tuple size stored in the companion buffer
    */
    [[nodiscard]] uint64_t getNumberOfTuples() const noexcept;

    /**
     * @brief set the tuple size stored in the companion buffer
     */
    void setNumberOfTuples(uint64_t);

    /**
     * @brief method to get the watermark as a timestamp
     * @return watermark
     */
    [[nodiscard]] uint64_t getWatermark() const noexcept;

    /**
   * @brief method to set the watermark with a timestamp
   * @param value timestamp
   */
    void setWatermark(uint64_t watermark);

    /**
    * @brief method to get the sequence number
    * @return sequence number
    */
    [[nodiscard]] uint64_t getSequenceNumber() const noexcept;

    /**
     * @brief method to set the sequenceNumber
     * @param value
     */
    void setSequenceNumber(uint64_t sequenceNumber);

    /**
     * @brief get id where this buffer was created
     * @return origin id
     */
    [[nodiscard]] uint64_t getOriginId() const noexcept;

    /**
     * @brief set originId
     * @param originId
     */
    void setOriginId(uint64_t originId);

    /**
    * @brief method to set the watermark with a timestamp
    * @param value timestamp
    */
    void setCreationTimestamp(uint64_t ts);

    /**
     * @brief method to get the creation timestamp
     * @return ts
     */
    [[nodiscard]] uint64_t getCreationTimestamp() const noexcept;

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    void dumpOwningThreadInfo();
#endif

  private:
    std::atomic<int32_t> referenceCounter = 0;
    uint32_t numberOfTuples = 0;
    WatermarkTs watermark = 0;
    SequenceNumber sequenceNumber = 0;
    int64_t creationTimestamp{};
    OriginId originId = 0;

  public:
    MemorySegment* owner;
    std::atomic<BufferRecycler*> owningBufferRecycler{};
    std::function<void(MemorySegment*, BufferRecycler*)> recycleCallback;

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
  private:
    class ThreadOwnershipInfo {
        friend class BufferControlBlock;

      private:
        std::string threadName;
        std::string callstack;

      public:
        ThreadOwnershipInfo();

        ThreadOwnershipInfo(std::string&& threadName, std::string&& callstack);

        ThreadOwnershipInfo(const ThreadOwnershipInfo&) = default;

        ThreadOwnershipInfo& operator=(const ThreadOwnershipInfo&) = default;

        friend std::ostream& operator<<(std::ostream& os, const ThreadOwnershipInfo& info) {
            os << info.threadName << " buffer is used in " << info.callstack;
            return os;
        }
    };
    std::mutex owningThreadsMutex;
    std::unordered_map<std::thread::id, std::deque<ThreadOwnershipInfo>> owningThreads;
#endif
};

/**
 * @brief The MemorySegment is a wrapper around a pointer to allocated memory of size bytes and a control block
 * (@see class BufferControlBlock). The MemorySegment is intended to be used **only** in the BufferManager.
 * The BufferManager is the only class that can store MemorySegments. A MemorySegment has no clue of what it's stored
 * inside its allocated memory and has no way to expose the pointer to outside world.
 * The public companion of a MemorySegment is the TupleBuffer, which can "leak" the pointer to the outside world.
 *
 *
 * Reminder: this class should be header-only to help inlining
 *
 *
 *
 *
 */
class MemorySegment {
    friend class NES::Runtime::TupleBuffer;
    friend class NES::Runtime::LocalBufferPool;
    friend class NES::Runtime::FixedSizeBufferPool;
    friend class NES::Runtime::BufferManager;

  public:
    MemorySegment(const MemorySegment& other);

    MemorySegment& operator=(const MemorySegment& other);

    MemorySegment() noexcept = default;

    explicit MemorySegment(uint8_t* ptr,
                           uint32_t size,
                           BufferRecycler* recycler,
                           std::function<void(MemorySegment*, BufferRecycler*)>&& recycleFunction);

    ~MemorySegment();

    uint8_t* getPointer() const { return ptr; }

  private:
    /**
     * @brief Private constructor for the memory Segment
     * @param ptr
     * @param size of the segment
     * @param recycler
     * @param recycleFunction
     */
    explicit MemorySegment(uint8_t* ptr,
                           uint32_t size,
                           BufferRecycler* recycler,
                           std::function<void(MemorySegment*, BufferRecycler*)>&& recycleFunction,
                           bool);

    /**
     * @return true if the segment has a reference counter equals to zero
     */
    bool isAvailable() { return controlBlock->getReferenceCount() == 0; }

    /**
     * @brief The size of the memory segment
     * @return
     */
    [[nodiscard]] uint32_t getSize() const { return size; }

    /*

     Layout of the mem segment (padding might be added differently depending on the compiler in-use).
     +-----------------------+-----------+-------------------+-------------------------------+
     | pointer to data  (8b) | size (4b) | likely 4b padding | pointer to control block (8b) |
     +------------+----------+-----------+-------------------+---------+---------------------+
                  |                                                    |
     +------------+                                          +---------+
     |                                                       |
     v                                                       v
     +----------------------------+-------------------------+----------------------------+
     |    data region (variable size)                       | control block (fixed size) |
     +----------------------------+-------------------------+----------------------------+
     */

    uint8_t* ptr{nullptr};
    uint32_t size{0};
    detail::BufferControlBlock* controlBlock{nullptr};
};

/**
 * @brief This is the callback that is called when ZMQ is done with the sending of the buffer with payload in ptr.
 * The hint parameter is the size of the whole buffer (casted as void*)
 */
void zmqBufferRecyclingCallback(void* ptr, void* hint);

}// namespace detail
}// namespace Runtime
}// namespace NES

#endif  // NES_INCLUDE_RUNTIME_DETAIL_TUPLEBUFFERIMPL_HPP_
