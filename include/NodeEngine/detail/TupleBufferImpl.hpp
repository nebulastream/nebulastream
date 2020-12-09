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

#ifndef NES_INCLUDE_NODEENGINE_DETAIL_TUPLEBUFFERIMPL_HPP_
#define NES_INCLUDE_NODEENGINE_DETAIL_TUPLEBUFFERIMPL_HPP_

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
class TupleBuffer;

namespace NodeEngine{
class BufferManager;

}

namespace detail {

class MemorySegment;

void revertEndianness(TupleBuffer& buffer, SchemaPtr schema);

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
    explicit BufferControlBlock(MemorySegment* owner, std::function<void(MemorySegment*)>&& recycleCallback);

    BufferControlBlock(const BufferControlBlock&);

    BufferControlBlock& operator=(const BufferControlBlock&);

    MemorySegment* getOwner();

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
    uint32_t getReferenceCount();

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
    uint64_t getNumberOfTuples() const;

    /**
     * @brief set the tuple size stored in the companion buffer
     */
    void setNumberOfTuples(uint64_t);

    /**
     * @brief method to get the watermark as a timestamp
     * @return watermark
     */
    int64_t getWatermark() const;

    /**
   * @brief method to set the watermark with a timestamp
   * @param value timestamp
   */
    void setWatermark(int64_t value);

    /**
     * @brief get id where this buffer was created
     * @return origin id
     */
    const uint64_t getOriginId() const;

    /**
     * @brief set originId
     * @param originId
     */
    void setOriginId(uint64_t originId);

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    void dumpOwningThreadInfo();
#endif

  private:
    std::atomic<uint32_t> referenceCounter;
    std::atomic<uint32_t> numberOfTuples;
    std::atomic<int64_t> watermark;
    std::atomic<uint64_t> originId;

  private:
    MemorySegment* owner;
    std::function<void(MemorySegment*)> recycleCallback;

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
    friend class NES::TupleBuffer;

    friend class NES::NodeEngine::BufferManager;

  public:
    MemorySegment(const MemorySegment& other);

    MemorySegment& operator=(const MemorySegment& other);

    MemorySegment(MemorySegment&& other) = delete;

    MemorySegment& operator=(MemorySegment&& other) = delete;

    MemorySegment();

    explicit MemorySegment(uint8_t* ptr, uint32_t size, std::function<void(MemorySegment*)>&& recycleFunction);

    ~MemorySegment();

  private:
    /**
     * @return true if the segment has a reference counter equals to zero
     */
    bool isAvailable() { return controlBlock->getReferenceCount() == 0; }

    /**
     * @brief The size of the memory segment
     * @return
     */
    uint32_t getSize() const { return size; }

  private:
    /*

     Layout of the mem segment
     +----------------------------+----------------+-----------------------------------+
     |    pointer to data  (8b)   |  size (4b)     |  pointer to control block (8b)    |
     +------------+---------------+----------------+-----------------------------------+
                  |                                          |
     +------------+                                          |
     |                                                       |
     v                                                       v
     +----------------------------+-------------------------+----------------------------+
     |    data region (variable size)                       | control block (fixed size) |
     +----------------------------+-------------------------+----------------------------+

*/

    uint8_t* ptr;
    uint32_t size;
    detail::BufferControlBlock* controlBlock;
};

/**
 * @brief This is the callback that is called when ZMQ is done with the sending of the buffer with payload in ptr.
 * The hint parameter is the size of the whole buffer (casted as void*)
 */
void zmqBufferRecyclingCallback(void* ptr, void* hint);

}// namespace detail
}// namespace NES

#endif//NES_INCLUDE_NODEENGINE_DETAIL_TUPLEBUFFERIMPL_HPP_
