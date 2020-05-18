#ifndef NES_INCLUDE_NODEENGINE_DETAIL_TUPLEBUFFERIMPL_HPP_
#define NES_INCLUDE_NODEENGINE_DETAIL_TUPLEBUFFERIMPL_HPP_

#include <Util/Logger.hpp>

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#endif

namespace NES {
class BufferManager;
class TupleBuffer;
namespace detail {

class MemorySegment;

std::string printTupleBuffer(TupleBuffer& buffer, SchemaPtr schema);
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
     * @brief returns the tuple size stored in the companion buffer
     * Note that this is going to be deprecated in future NES versions
     * @return the tuple size stored in the companion buffer
     */
    size_t getTupleSizeInBytes() const;

    /**
    * @brief returns the number of tuples stored in the companion buffer
    * Note that this is going to be deprecated in future NES versions
    * @return the tuple size stored in the companion buffer
    */
    size_t getNumberOfTuples() const;

    /**
     * @brief set the tuple size stored in the companion buffer
     */
    void setNumberOfTuples(size_t);

    /**
     * @brief set the number of tuples stored in the companion buffer
     */
    void setTupleSizeInBytes(size_t);

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    void dumpOwningThreadInfo();
#endif

  private:
    std::atomic<uint32_t> referenceCounter;
    // TODO check whether to remove them
    std::atomic<uint32_t> tupleSizeInBytes;
    std::atomic<uint32_t> numberOfTuples;
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
 */
class MemorySegment {
    friend class NES::TupleBuffer;

    friend class NES::BufferManager;

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
