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
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <functional>
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferPrimitives.hpp>
#include <Runtime/DataSegment.hpp>
#include <Time/Timestamp.hpp>

// #ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    #include <deque>
    #include <mutex>
    #include <thread>
    #include <unordered_map>
    #include <cpptrace.hpp>
// #endif


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
    friend RepinBCBLock;
    friend UniqueMutexBCBLock;
    friend SharedMutexBCBLock;

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
    /// Returns true if 0 is reached and the buffers memory is marked for spilling
    bool pinnedRelease();

    /// Decrease the data reference counter by one
    /// Returns true if 0 is reached and the buffer is recycled
    bool dataRelease();


    template <bool pinned>
    RefCountedBCB<pinned> getCounter() noexcept;

    std::optional<RepinBCBLock> startRepinning() noexcept;
    std::optional<UniqueMutexBCBLock> tryLockUnique() const noexcept;
    std::optional<SharedMutexBCBLock> tryLockShared() const noexcept;

    ///Resets everything to a state as if this was a BCB for a newly created pinned buffer.
    ///Ensure that you pinned the BCB when you had the lock from startRepinning, otherwise the segments could get spilled again
    void markRepinningDone() noexcept;


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

    bool swapSegment(DataSegment<DataLocation>&, ChildOrMainDataKey key) noexcept;
    bool swapDataSegment(DataSegment<DataLocation>& segment) noexcept;
    bool swapChild(DataSegment<DataLocation>&, ChildKey key) noexcept;


    template <UniqueBCBLock Lock>
    bool swapSegment(DataSegment<DataLocation>& segment, ChildOrMainDataKey key, Lock& lock) noexcept;
    template <UniqueBCBLock Lock>
    bool swapDataSegment(DataSegment<DataLocation>& segment, Lock& lock) noexcept;
    template <UniqueBCBLock Lock>
    bool swapChild(DataSegment<DataLocation>& segment, ChildKey key, Lock& lock) noexcept;

    // bool swapSegment(DataSegment<DataLocation>&, ChildOrMainDataKey) noexcept;
    // bool swapDataSegment(DataSegment<DataLocation>& segment) noexcept;
    // bool swapChild(DataSegment<DataLocation>&, ChildKey) noexcept;

    ///@brief Unregisters a child data segment.
    ///@return whether the passed data segment was a child of this node
    bool unregisterChild(ChildKey child) noexcept;
    template <UniqueBCBLock Lock>
    bool unregisterChild(ChildKey child, Lock& lock) noexcept;

    ///@brief Registers the data segment as a child of this BCB.
    ///@return true when the passed data segment is a new child, false if it was already registerd
    std::optional<ChildKey> registerChild(const DataSegment<DataLocation>& child) noexcept;
    template <UniqueBCBLock Lock>
    std::optional<ChildKey> registerChild(const DataSegment<DataLocation>& child, Lock& lock) noexcept;

    std::optional<ChildKey> findChild(const DataSegment<DataLocation>& child) noexcept;
    template <SharedBCBLock Lock>
    std::optional<ChildKey> findChild(const DataSegment<DataLocation>& child, Lock& lock) noexcept;

    // std::optional<ChildKey> findChild(const DataSegment<DataLocation>& child, std::unique_lock<std::shared_mutex>& lock) noexcept;

    ///@brief Deletes a child data segment, the data is not accessible afterward anymore
    ///@return whether the argument was a child. If false, child remains valid.
    // bool deleteChild(DataSegment<DataLocation>&& child);

    ///@brief Self-destructs this BCB if there is only one owner left
    ///@return the owned data segment if self-destructed, nullopt otherwise
    std::optional<DataSegment<DataLocation>> stealDataSegment();
    template <UniqueBCBLock Lock>
    std::optional<DataSegment<DataLocation>> stealDataSegment(Lock& lock);

    ///WARNING: This method is inherently not thread-safe and using it without synchronizing access to this BCB will lead to data races
    [[nodiscard]] uint32_t getNumberOfChildrenBuffers() const noexcept;

    std::optional<DataSegment<DataLocation>> getChild(ChildKey key) const noexcept;
    template <SharedBCBLock Lock>
    std::optional<DataSegment<DataLocation>> getChild(ChildKey key, Lock& lock) const noexcept;

    std::optional<DataSegment<DataLocation>> getSegment(ChildOrMainDataKey key) const noexcept;
    template <SharedBCBLock Lock>
    std::optional<DataSegment<DataLocation>> getSegment(ChildOrMainDataKey key, Lock& lock) const noexcept;

// #ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    void dumpOwningThreadInfo();
// #endif

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

    mutable std::shared_mutex segmentMutex;
    ///Not thread safe
    ///Max size is max uint32 - 2.
    ///When accessing, make sure to use the the index stored in the buffers - 2.
    ///See ChildKey and ChildOrMainDataKey
    std::vector<DataSegment<DataLocation>> children{};
    ///Not thread safe, to be used in second chance to avoid initiating spilling for the same segment multiple times
    ///Unknown indicates that no spilling was attempted yet, >= 2 indicates that for up to (inclusive) that child spilling was initiated,
    ///Main indicates spilling was initiated for all children and the main data segment.
    std::atomic<ChildOrMainDataKey> skipSpillingUpTo = ChildOrMainDataKey::UNKNOWN();

    std::atomic<ChildOrMainDataKey> isSpilledUpTo = ChildOrMainDataKey::UNKNOWN();

    std::atomic<DataSegment<DataLocation>> data{};
    std::atomic<BufferRecycler*> owningBufferRecycler{};
    //std::function<void(DataSegment<InMemoryLocation>&, BufferRecycler*)> recycleCallback;

    //False means second chance was not at the buffer yet, true means it was seen already and gets evicted next time its seen.
    std::atomic_flag clockReference = false;
    std::atomic_flag isRepinning = false;

// #ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
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
// #endif
};
static_assert(sizeof(BufferControlBlock) % 64 == 0);
static_assert(alignof(BufferControlBlock) % 64 == 0);

}
}
