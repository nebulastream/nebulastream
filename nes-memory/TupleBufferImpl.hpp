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
#include <optional>
#include <shared_mutex>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/DataSegment.hpp>
#include <Time/Timestamp.hpp>
#include <Runtime/BufferPrimitives.hpp>

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
#    include <deque>
#    include <mutex>
#    include <thread>
#    include <unordered_map>
#    include <cpptrace.hpp>
#endif


namespace NES::Memory
{

namespace detail
{
class ChildOrMainDataKey;
}
class BufferManager;
class LocalBufferPool;
class PinnedBuffer;
class FixedSizeBufferPool;
class BufferRecycler;

static constexpr auto GET_BUFFER_TIMEOUT = std::chrono::milliseconds(1000);


/**
 * @brief Computes aligned buffer size based on original buffer size and alignment
 */
constexpr uint32_t alignBufferSize(uint32_t bufferSize, uint32_t withAlignment)
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
    friend BufferManager;

public:
    ///Creates a BufferControlBlock around a raw memory address
    explicit BufferControlBlock(
        const DataSegment<InMemoryLocation>&,
        BufferRecycler* recycler); //, std::function<void(DataSegment<DataLocation>&&, BufferRecycler*)>&& recycleCallback);


    BufferControlBlock(const BufferControlBlock&) = delete;

    BufferControlBlock& operator=(const BufferControlBlock&) = delete;

    [[nodiscard]] DataSegment<DataLocation> getData() const;
    void resetBufferRecycler(BufferRecycler* recycler);

    /// Increase the pinned reference counter by one.
    BufferControlBlock* pinnedRetain();


    /// Increase the data reference counter by one.
    BufferControlBlock* dataRetain();

    [[nodiscard]] int32_t getPinnedReferenceCount() const noexcept;
    [[nodiscard]] int32_t getDataReferenceCount() const noexcept;

    /// Decrease the pinned reference counter by one
    /// Returns true if 0 is reached and the buffers memory is marked for recycling
    bool pinnedRelease();

    /// Decrease the data reference counter by one
    /// Returns true if 0 is reached and the buffer is recycled
    bool dataRelease();

    [[nodiscard]] uint64_t getNumberOfTuples() const noexcept;
    void setNumberOfTuples(uint64_t);
    [[nodiscard]] Runtime::Timestamp getWatermark() const noexcept;
    void setWatermark(Runtime::Timestamp watermark);
    [[nodiscard]] SequenceNumber getSequenceNumber() const noexcept;
    void setSequenceNumber(SequenceNumber sequenceNumber);
    [[nodiscard]] ChunkNumber getChunkNumber() const noexcept;
    void setChunkNumber(ChunkNumber chunkNumber);
    [[nodiscard]] bool isLastChunk() const noexcept;
    void setLastChunk(bool lastChunk);
    [[nodiscard]] OriginId getOriginId() const noexcept;
    void setOriginId(OriginId originId);
    void setCreationTimestamp(Runtime::Timestamp timestamp);
    [[nodiscard]] Runtime::Timestamp getCreationTimestamp() const noexcept;

    bool swapDataSegment(DataSegment<DataLocation>& segment) noexcept;
    bool swapChild(DataSegment<DataLocation>&, ChildKey) noexcept;

    ///@brief Unregisters a child data segment.
    ///@return whether the passed data segment was a child of this node
    bool unregisterChild(DataSegment<DataLocation>& child) noexcept;

    ///@brief Registers the data segment as a child of this BCB.
    ///@return true when the passed data segment is a new child, false if it was already registerd
    ChildKey registerChild(const DataSegment<DataLocation>& child) noexcept;
    std::optional<ChildKey> findChild(const DataSegment<DataLocation>& child) noexcept;

    ///@brief Deletes a child data segment, the data is not accessible afterward anymore
    ///@return whether the argument was a child. If false, child remains valid.
    bool deleteChild(DataSegment<DataLocation>&& child);

    ///@brief Self-destructs this BCB if there is only one owner left
    ///@return the owned data segment if self-destructed, nullopt otherwise
    std::optional<DataSegment<DataLocation>> stealDataSegment();
    [[nodiscard]] uint32_t getNumberOfChildrenBuffers() const noexcept { return children.size(); }

    std::optional<DataSegment<DataLocation>> getChild(ChildKey) const noexcept;

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    void dumpOwningThreadInfo();
#endif

private:
    //>= 1 buffer is pinned
    //== 0 buffer is unpinned and could be spilled at any point
    //== -1 buffers data segment is getting switched out, will be set to 0 afterward
    std::atomic<int32_t> pinnedCounter = 0;
    //Once the data counter has zero, it is not going to increase again
    std::atomic<int32_t> dataCounter = 0;
    uint32_t numberOfTuples = 0;
    Runtime::Timestamp watermark = Runtime::Timestamp(Runtime::Timestamp::INITIAL_VALUE);
    SequenceNumber sequenceNumber = INVALID_SEQ_NUMBER;
    ChunkNumber chunkNumber = INVALID_CHUNK_NUMBER;
    bool lastChunk = true;
    Runtime::Timestamp creationTimestamp = Runtime::Timestamp(Runtime::Timestamp::INITIAL_VALUE);
    OriginId originId = INVALID_ORIGIN_ID;

    mutable std::shared_mutex childMutex;
    ///Not thread safe
    ///Max size is max uint32 - 2.
    ///When accessing, make sure to use the the index stored in the buffers - 2.
    ///See ChildKey and ChildOrMainDataKey
    std::vector<DataSegment<DataLocation>> children{};
    ///Not thread safe, to be used in second chance to avoid initiating spilling for the same segment multiple times
    ///Unknown indicates that no spilling was attempted yet, >= 2 indicates that for up to (inclusive) that child spilling was initiated,
    ///Main indicates spilling was initiated for all children and the main data segment.
    ChildOrMainDataKey spillingInitiatedUpTo = ChildOrMainDataKey::UNKNOWN();

    std::atomic<DataSegment<DataLocation>> data{};
    std::atomic<BufferRecycler*> owningBufferRecycler{};
    //std::function<void(DataSegment<InMemoryLocation>&, BufferRecycler*)> recycleCallback;

    //False means second chance was not at the buffer yet, true means it was seen already and gets evicted next time its seen.
    std::atomic_flag clockReference = false;

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

}
}
