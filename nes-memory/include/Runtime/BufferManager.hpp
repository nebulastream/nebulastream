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
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <vector>
#include <liburing.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferFiles.hpp>
#include <Runtime/BufferManagerImpl.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/DataSegment.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManagerFuture.hpp>
#include <folly/MPMCQueue.h>
#include <gtest/gtest_prod.h>

namespace NES::Memory
{
namespace detail
{
// class GetInMemorySegmentFuture;
// class SubmitIOWriteTask;
// class NewSegmentAwaiter;
}
class PinnedBuffer;
class FloatingBuffer;

/**
 * @brief The BufferManager is responsible for:
 * 1. Pooled Buffers: preallocated fixed-size buffers of memory that must be reference counted
 * 2. Unpooled Buffers: variable sized buffers that are allocated on-the-fly. They are also subject to reference
 * counting.
 *
 * The reference counting mechanism of the TupleBuffer is explained in PinnedBuffer.hpp
 *
 * The BufferManager stores the pooled buffers as MemorySegment-s. When a component asks for a Pooled buffer,
 * then the BufferManager retrieves an available buffer (it blocks the calling thread, if no buffer is available).
 * It then hands out a TupleBuffer that is constructed through the pointer stored inside a MemorySegment.
 * This is necessary because the BufferManager must keep all buffers stored to ensure that when its
 * destructor is called, all buffers that it has ever created are deallocated. Note the BufferManager will check also
 * that no reference counter is non-zero and will throw a fatal exception, if a component hasnt returned every buffers.
 * This is necessary to avoid memory leaks.
 *
 * Unpooled buffers are either allocated on the spot or served via a previously allocated, unpooled buffer that has
 * been returned to the BufferManager by some component.
 *
 */
class BufferManager : public std::enable_shared_from_this<BufferManager>,
                      public BufferRecycler,
                      public AbstractBufferProvider,
                      public AbstractPoolProvider
{
    friend class PinnedBuffer;
    FRIEND_TEST(BufferManagerTest, SpillChildBufferClock);
    /// Hide the BufferManager constructor and only allow creation via BufferManager::create().
    /// Following: https://en.cppreference.com/w/cpp/memory/enable_shared_from_this
    struct Private
    {
        explicit Private() = default;
    };

private:
    static constexpr auto DEFAULT_BUFFER_SIZE = 8 * 1024;
    static constexpr auto DEFAULT_NUMBER_OF_BUFFERS = 1024;
    static constexpr auto DEFAULT_ALIGNMENT = 64;
    static constexpr auto DEFAULT_CHECKS_ADDED_PER_NEW_BUFFER = 2;
    static constexpr auto DEFAULT_BUFFER_CHECKS_THRESHOLD = 128;
    static constexpr auto DEFAULT_URING_RING_SIZE = 2048;
    static constexpr auto DEFAULT_SPILL_BATCH_SIZE = 128;
    static constexpr auto DEFAULT_MAX_ZERO_WRITE_ROUNDS = 32;
    static constexpr auto DEFAULT_MAX_CONCURRENT_MEMORY_REQS = DEFAULT_URING_RING_SIZE;
    static constexpr auto DEFAULT_MAX_CONCURRENT_READ_SUBMISSIONS = DEFAULT_URING_RING_SIZE;
    static constexpr auto DEFAULT_MAX_CONCURRENT_PUNCH_HOLE_SUBMISSIONS = DEFAULT_URING_RING_SIZE;
    //4TB is supported by ext4 and ntfs
    static constexpr unsigned long long DEFAULT_MAX_FILE_SIZE = 1024UL * 1024 * 1024 * 1024 * 4;
    // static constexpr auto DEFAULT_SPILL_DIRECTORY = std::filesystem::absolute("/tmp/nebulastream/spilling");
    static constexpr auto DEFAULT_SPILL_DIRECTORY = "/tmp/nebulastream/spilling";


public:
    explicit BufferManager(
        Private,
        uint32_t bufferSize,
        uint32_t numOfBuffers,
        const std::shared_ptr<std::pmr::memory_resource>& memoryResource,
        uint32_t withAlignment,
        uint32_t checksAddedPerNewBuffer,
        uint64_t bufferChecksThreshold,
        uint32_t uringRingSize,
        uint32_t uringBatchSize,
        uint32_t maxZeroWriteRounds,
        uint32_t maxConcurrentMemoryReqs,
        uint32_t maxConcurrentReadSubmissions,
        uint32_t maxConcurrentHolePunchSubmissions,
        const std::filesystem::path& spillDirectory);

    /**
   * @brief Creates a new global buffer manager
   * @param bufferSize the size of each buffer in bytes
   * @param numOfBuffers the total number of buffers in the pool
   * @param withAlignment the alignment of each buffer, default is 64 so ony cache line aligned buffers, This value must be a pow of two and smaller than page size
   */
    static std::shared_ptr<BufferManager> create(
        uint32_t bufferSize = DEFAULT_BUFFER_SIZE,
        uint32_t numOfBuffers = DEFAULT_NUMBER_OF_BUFFERS,
        std::shared_ptr<std::pmr::memory_resource> memoryResource = std::make_shared<NesDefaultMemoryAllocator>(),
        uint32_t withAlignment = DEFAULT_ALIGNMENT,
        uint32_t checksAddedPerNewBuffer = DEFAULT_CHECKS_ADDED_PER_NEW_BUFFER,
        uint64_t bufferChecksThreshold = DEFAULT_BUFFER_CHECKS_THRESHOLD,
        uint32_t uringRingSize = DEFAULT_URING_RING_SIZE,
        uint32_t spillBatchSize = DEFAULT_SPILL_BATCH_SIZE,
        uint32_t maxZeroWriteRounds = DEFAULT_MAX_ZERO_WRITE_ROUNDS,
        uint32_t maxConcurrentMemoryReqs = DEFAULT_MAX_CONCURRENT_MEMORY_REQS,
        uint32_t maxConcurrentReadSubmissions = DEFAULT_MAX_CONCURRENT_READ_SUBMISSIONS,
        uint32_t maxConcurrentHolePunchSubmissions = DEFAULT_MAX_CONCURRENT_PUNCH_HOLE_SUBMISSIONS,
        std::filesystem::path spillDirectory = DEFAULT_SPILL_DIRECTORY);

    BufferManager(const BufferManager&) = delete;
    BufferManager& operator=(const BufferManager&) = delete;
    ~BufferManager() override;

    BufferManagerType getBufferManagerType() const override;

private:
    /**
   * @brief Configure the BufferManager to use numOfBuffers buffers of size bufferSize bytes.
   * This is a one shot call. A second invocation of this call will fail
   * @param withAlignment
   */
    void initialize(uint32_t withAlignment);
    PinnedBuffer makeBufferAndRegister(detail::DataSegment<detail::InMemoryLocation>) noexcept;



public:
    [[nodiscard]] PinnedBuffer pinBuffer(FloatingBuffer&&);
    [[nodiscard]] GetInMemorySegmentFuture getInMemorySegment(size_t amount) noexcept;
    [[nodiscard]] ReadSegmentFuture
    readOnDiskSegment(detail::DataSegment<detail::OnDiskLocation> source, detail::DataSegment<detail::InMemoryLocation> target) noexcept;
    [[nodiscard]] GetInMemorySegmentFuture getBuffer() noexcept;

    [[nodiscard]] RepinBufferFuture repinBuffer(FloatingBuffer&&) noexcept override;

    /// This blocks until a buffer is available.
    PinnedBuffer getBufferBlocking() override;

    /// invalid optional if there is no buffer.
    std::optional<PinnedBuffer> getBufferNoBlocking() override;


    /**
   * @brief Returns a new Buffer wrapped in an optional or an invalid option if there is no buffer available within
   * timeoutMs.
   * @param timeoutMs the amount of time to wait for a new buffer to be retuned
   * @return a new buffer
   */
    std::optional<PinnedBuffer> getBufferWithTimeout(std::chrono::milliseconds timeoutMs) override;

    /**
   * @brief Returns an unpooled buffer of size bufferSize wrapped in an optional or an invalid option if an error
   * occurs.
   * @param bufferSize
   * @return a new buffer
   */
    std::optional<PinnedBuffer> getUnpooledBuffer(size_t bufferSize) override;


    size_t getBufferSize() const override;
    size_t getNumOfPooledBuffers() const override;
    size_t getNumOfUnpooledBuffers() const override;
    size_t getAvailableBuffers() const override;
    size_t getAvailableBuffersInFixedSizePools() const;

    /**
   * @brief Create a local buffer manager that is assigned to one pipeline or thread
   * @param numberOfReservedBuffers number of exclusive buffers to give to the pool
   * @return a local buffer manager with numberOfReservedBuffers exclusive buffer
   */
    std::optional<std::shared_ptr<AbstractBufferProvider>> createLocalBufferPool(size_t numberOfReservedBuffers) override;

    /**
    * @brief Create a local buffer manager that is assigned to one pipeline or thread
    * @param numberOfReservedBuffers number of exclusive buffers to give to the pool
    * @return a fixed buffer manager with numberOfReservedBuffers exclusive buffer
    */
    std::optional<std::shared_ptr<AbstractBufferProvider>> createFixedSizeBufferPool(size_t numberOfReservedBuffers) override;

    /**
   * @brief Recycle a pooled buffer by making it available to others
   * @param buffer
   */
    void recycleSegment(detail::DataSegment<detail::InMemoryLocation>&& segment) override;


    /**
   * @brief Recycle a pooled buffer by making it available to others
   * @param buffer
   */
    bool recycleSegment(detail::DataSegment<detail::OnDiskLocation>&& segment) override;

    /**
   * @brief this method clears all local buffers pools and remove all buffers from the global buffer manager
   */

    void destroy() override;


private:

    folly::MPMCQueue<detail::BufferControlBlock*> newBuffers;

    mutable std::shared_mutex allBuffersMutex{};
    //All pooled buffers
    std::vector<detail::BufferControlBlock*> allBuffers;
    ///How many buffer checks (for example with cleanupAllBuffers) are outstanding.
    ///Increased by checksAddedPerNewBuffer for each new BCB added, and reset either when reaching bufferCheckThreshold and calling all buffer cleanup,
    ///Or when spilling by the amount of buffers passed while doing 2nd chance.
    std::atomic<size_t> buffersToCheck{0};
    const uint32_t checksAddedPerNewBuffer;
    const size_t bufferChecksThreshold;


    folly::MPMCQueue<detail::DataSegment<detail::InMemoryLocation>> availableBuffers;
    std::vector<detail::BufferControlBlock*> unpooledBuffers;

    mutable std::recursive_mutex availableBuffersMutex{};

    mutable std::recursive_mutex unpooledBuffersMutex{};

    unsigned int bufferSize;
    size_t numOfBuffers;

    uint8_t* bufferBasePointer{nullptr};
    size_t allocatedBufferAreaSize;

    mutable std::recursive_mutex localBufferPoolsMutex{};
    std::vector<std::shared_ptr<AbstractBufferProvider>> localBufferPools;
    std::shared_ptr<std::pmr::memory_resource> memoryResource;
    std::atomic<bool> isDestroyed{false};


    const std::filesystem::path spillDirectory;

    //Writing to disk
    folly::MPMCQueue<std::shared_ptr<detail::AvailableSegmentAwaiter<GetInMemorySegmentPromise<GetInMemorySegmentFuture>>>>
        waitingSegmentRequests;
    std::atomic_flag isSpilling = false;
    mutable std::shared_mutex writeSqeMutex{};
    mutable std::recursive_mutex writeCqeMutex{};
    //Access to everything from here must be synchronized
    io_uring uringWriteRing;
    std::atomic<size_t> buffersBeingSpilled{0};
    std::atomic<size_t> requiredSegments{0};
    std::map<uint8_t, detail::File> files;
    uint64_t fileOffset{0};
    uint32_t spillBatchSize;
    uint32_t maxZeroWriteRounds;
    uint64_t clockAt{0};
    int writeErrorCounter{0};

    //Reading from disk
    folly::MPMCQueue<std::shared_ptr<detail::SubmitSegmentReadAwaiter<ReadSegmentPromise<ReadSegmentFuture>>>>
        waitingReadRequests;

    mutable std::mutex readSqeMutex{};
    mutable std::mutex readCqeMutex{};
    io_uring uringReadRing;
    size_t readsInFlight;
    int readErrorCounter{0};



    //Deleting from file
    folly::MPMCQueue<std::shared_ptr<detail::SubmitPunchHoleSegmentAwaiter<PunchHolePromise<PunchHoleFuture>>>>
        waitingPunchHoleRequests;

    mutable std::mutex punchHoleSqeMutex{};
    mutable std::mutex punchHoleCqeMutex{};
    io_uring uringPunchHoleRing;
    int punchHoleErrorCounter{0};

    folly::MPMCQueue<PunchHoleFuture> holesInProgress;
    mutable std::mutex holeMutex{};
    //For guaranteed cleanup with FALLOC_FL_COLLAPSE_RANGE that will only work with larger continuous blocks
    std::set<detail::DataSegment<detail::OnDiskLocation>> holePunchedSegments;
    //DataSegments that for whatever reason could not be holepunched.
    std::set<detail::DataSegment<detail::OnDiskLocation>> failedToHolePunch;



    // class SpillResult
    // {
    // private:
    //     uint32_t index;
    //     std::shared_future<std::vector<std::vector<detail::DataSegment<detail::InMemoryLocation>>>> segments;
    //
    // public:
    //     std::vector<detail::DataSegment<detail::InMemoryLocation>> waitForSegments()
    //     {
    //         segments.wait();
    //         return segments.get()[index];
    //     }
    // };
    //@return a wrapper around a shared future that provides a vector with up to amount inMemory segments.
    //It is not guaranteed that the amount of returned segments equals the requested amount, multiple calls might be necessary.


    ///@brief tries to retrieve `amount` inMemory buffers by spilling segments of unpinned buffers.
    ///If no other thread is currently spilling buffers, the calling thread will block until amount of buffers have been successfully spilled.
    ///If another thread is currently spilling, will return an empty vector
    // std::unique_ptr<std::vector<detail::DataSegment<detail::InMemoryLocation>>> spillSegments(size_t amount);

    /// <b>Unsynchronized access to allBuffers</b>
    /// Flushes newBuffers MPMC queue into all buffers
    void flushNewBuffers() noexcept;

    ///<b>Unsynchronized access to write SQE!</b>
    ///Non-blocking function that writes Sqe entries for up to spillBatchSize unpinned buffers.
    ///Returns the amount of buffers submitted to ioUring, or on failrue -errno
    int64_t secondChancePass() noexcept;

    void pollWriteCompletionEventsOnce() noexcept;
    void waitForWriteCompletionEventsOnce() noexcept;
    ///<b>Unsynchronized access to write CQE!</b>
    size_t processWriteCompletionEvents() noexcept;

    void pollReadSubmissionEntriesOnce() noexcept;
    void waitForReadSubmissionEntriesOnce() noexcept;
    ///<b>Unsynchronized access to read SQE!</b>
    int64_t processReadSubmissionEntries() noexcept;

    void pollReadCompletionEntriesOnce() noexcept;
    void waitForReadCompletionEntriesOnce() noexcept;
    ///<b>Unsynchronized access to read CQE</b>
    size_t processReadCompletionEvents() noexcept;

    static detail::File prepareFile(const std::filesystem::path& dirPath, uint8_t id);

    PunchHoleFuture punchHoleSegment(detail::DataSegment<detail::OnDiskLocation>&& segment) noexcept;

    void pollPunchHoleSubmissionEntriesOnce() noexcept;
    void waitForPunchHoleSubmissionEntriesOnce() noexcept;
    ///<b>Unsynchronized access to punch hole SQE</b>
    size_t processPunchHoleSubmissionEntries() noexcept;

    void pollPunchHoleCompletionEntriesOnce() noexcept;
    void waitForPunchHoleCompletionEntriesOnce() noexcept;
    ///<b>Unsynchronized access to punch hole CQE</b>
    size_t processPunchHoleCompletionEntriesOnce() noexcept;


    ///<b>Unsynchronized access to allBuffers!</b>
    ///Iterate through allBuffers, erase unused BCBs and shrink array
    ///@param maxIter the maximum of number of BCBs to check, pass zero for checking all
    void cleanupAllBuffers(size_t maxIter);
};


}
