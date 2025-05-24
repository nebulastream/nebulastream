/*
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

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <TaggedPointer.hpp>
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    #include <deque>
    #include <mutex>
    #include <thread>
    #include <unordered_map>
    #include <cpptrace.hpp>
#endif

namespace NES
{
class UnpooledChunksManager;
}

namespace NES::Memory
{
class BufferManager;
class LocalBufferPool;
class TupleBuffer;
class FixedSizeBufferPool;
class BufferRecycler;

static constexpr auto GET_BUFFER_TIMEOUT = std::chrono::milliseconds(1000);


/**
 * @brief Computes aligned buffer size based on original buffer size and alignment
 */
constexpr uint32_t alignBufferSize(const uint32_t bufferSize, const uint32_t withAlignment)
{
    if (bufferSize % withAlignment)
    {
        /// make sure that each buffer is a multiple of the alignment
        return bufferSize + (withAlignment - bufferSize % withAlignment);
    }
    return bufferSize;
}

namespace detail
{

class MemorySegment;

#define PLACEHOLDER_LIKELY(cond) (cond) [[likely]]
#define PLACEHOLDER_UNLIKELY(cond) (cond) [[unlikely]]

/**
 * @brief This class provides a convenient way to track the reference counter as well metadata for its owning
 * MemorySegment/TupleBuffer. In particular, it stores the atomic reference counter that tracks how many
 * live reference exists of the owning MemorySegment/TupleBuffer and it also stores the callback to execute
 * when the reference counter reaches 0.
 *
 * Reminder: this class should be header-only to help inlining
 */
class alignas(64) BufferControlBlock
{
public:
    explicit BufferControlBlock(MemorySegment* owner, std::function<void(MemorySegment*, BufferRecycler*)>&& recycleCallback);

    [[nodiscard]] MemorySegment* getOwner() const;

    /// This method must be called before the BufferManager hands out a TupleBuffer. It ensures that the internal
    /// reference counter is zero. If that's not the case, an exception is thrown.
    /// Returns true if the mem segment can be used to create a TupleBuffer.
    bool prepare(const std::shared_ptr<BufferRecycler>& recycler);

    /// Increase the reference counter by one.
    BufferControlBlock* retain();

    [[nodiscard]] int32_t getReferenceCount() const noexcept;

    /// Decrease the reference counter by one
    /// Returns true if 0 is reached and the buffer is recycled
    bool release();
    [[nodiscard]] uint64_t getNumberOfTuples() const noexcept;
    void setNumberOfTuples(uint64_t);
    [[nodiscard]] uint64_t getUsedMemorySize() const noexcept;
    void setUsedMemorySize(uint64_t);
    [[nodiscard]] Timestamp getWatermark() const noexcept;
    void setWatermark(Timestamp watermark);
    [[nodiscard]] SequenceNumber getSequenceNumber() const noexcept;
    void setSequenceNumber(SequenceNumber sequenceNumber);
    [[nodiscard]] ChunkNumber getChunkNumber() const noexcept;
    void setChunkNumber(ChunkNumber chunkNumber);
    [[nodiscard]] bool isLastChunk() const noexcept;
    void setLastChunk(bool lastChunk);
    [[nodiscard]] OriginId getOriginId() const noexcept;
    void setOriginId(OriginId originId);
    void setCreationTimestamp(Timestamp timestamp);
    [[nodiscard]] Timestamp getCreationTimestamp() const noexcept;
    [[nodiscard]] uint32_t storeChildBuffer(BufferControlBlock* control);
    [[nodiscard]] bool loadChildBuffer(uint16_t index, BufferControlBlock*& control, uint8_t*& ptr, uint32_t& size) const;
    [[nodiscard]] uint32_t getNumberOfChildrenBuffer() const noexcept { return children.size(); }
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    void dumpOwningThreadInfo();
#endif

private:
    std::atomic<int32_t> referenceCounter = 0;
    uint32_t numberOfTuples = 0;
    uint32_t usedMemorySize = 0;
    Timestamp watermark = Timestamp(Timestamp::INITIAL_VALUE);
    SequenceNumber sequenceNumber = INVALID_SEQ_NUMBER;
    ChunkNumber chunkNumber = INVALID_CHUNK_NUMBER;
    bool lastChunk = true;
    Timestamp creationTimestamp = Timestamp(Timestamp::INITIAL_VALUE);
    OriginId originId = INVALID_ORIGIN_ID;
    std::vector<MemorySegment*> children;

public:
    MemorySegment* owner;
    std::shared_ptr<BufferRecycler> owningBufferRecycler = nullptr;
    std::function<void(MemorySegment*, BufferRecycler*)> recycleCallback;

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
private:
    class ThreadOwnershipInfo
    {
        friend class BufferControlBlock;

    private:
        std::string threadName;
        cpptrace::raw_trace callstack;

    public:
        ThreadOwnershipInfo();

        ThreadOwnershipInfo(std::string&& threadName, cpptrace::raw_trace&& callstack);

        ThreadOwnershipInfo(const ThreadOwnershipInfo&) = default;

        ThreadOwnershipInfo& operator=(const ThreadOwnershipInfo&) = default;

        friend std::ostream& operator<<(std::ostream& os, const ThreadOwnershipInfo& info)
        {
            os << info.threadName << " buffer is used in " << info.callstack.resolve();
            return os;
        }
    };
    std::mutex owningThreadsMutex;
    std::unordered_map<std::thread::id, std::deque<ThreadOwnershipInfo>> owningThreads;
#endif
};
static_assert(sizeof(BufferControlBlock) % 64 == 0);
static_assert(alignof(BufferControlBlock) % 64 == 0);
/**
 * @brief The MemorySegment is a wrapper around a pointer to allocated memory of size bytes and a control block
 * (@see class BufferControlBlock). The MemorySegment is intended to be used **only** in the BufferManager.
 * The BufferManager is the only class that can store MemorySegments. A MemorySegment has no clue of what it's stored
 * inside its allocated memory and has no way to expose the pointer to outside world.
 * The public companion of a MemorySegment is the TupleBuffer, which can "leak" the pointer to the outside world.
 *
 * Reminder: this class should be header-only to help inlining
 *
 */
class MemorySegment
{
    friend class NES::Memory::TupleBuffer;
    friend class NES::Memory::LocalBufferPool;
    friend class NES::Memory::FixedSizeBufferPool;
    friend class NES::Memory::BufferManager;
    friend class NES::UnpooledChunksManager;
    friend class NES::Memory::detail::BufferControlBlock;

    enum class MemorySegmentType : uint8_t
    {
        Native = 0,
        Wrapped = 1
    };

public:
    MemorySegment(const MemorySegment& other);

    MemorySegment& operator=(const MemorySegment& other);

    MemorySegment() noexcept = default;

    /// Constructor for the memory Segment that sets the tag pointer of the control block to the native type
    /// This constructor should be used if the memory of the buffer is managed by the BufferManager
    explicit MemorySegment(
        uint8_t* ptr, uint32_t size, std::function<void(MemorySegment*, BufferRecycler*)>&& recycleFunction, uint8_t* controlBlock);

    /// Constructor for the memory Segment that sets the tag pointer of the control block to the wrapped type
    /// This constructor should be used if the memory of the buffer is managed externally
    explicit MemorySegment(uint8_t* ptr, uint32_t size, std::function<void(MemorySegment*, BufferRecycler*)>&& recycleFunction);

    ~MemorySegment();

    uint8_t* getPointer() const { return ptr; }


private:
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
     +--------------------------------+-----------+-------------------+----------------------+
     | pointer to control block  (8b) | size (4b) | likely 4b padding | pointer to data (8b) |
     +------------+-------------------+-----------+-------------------+---------+------------+
                  |                                                    |
     +------------+                +-----------------------------------+
     |                             |
     v                             v
     +----------------------------+------------------------------------------------------+
     | control block (fixed size) |    data region (variable size)                       |
     +----------------------------+------------------------------------------------------+
     */

    uint8_t* ptr{nullptr};
    uint32_t size{0};
    TaggedPointer<BufferControlBlock> controlBlock{nullptr};
};

}
}
