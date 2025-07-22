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
#include <Runtime/BufferManager.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <ranges>
#include <shared_mutex>
#include <string>
#include <vector>
#include <unistd.h>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/BufferManagerFuture.hpp>
#include <Runtime/DataSegment.hpp>
#include <Runtime/FloatingBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Variant.hpp>
#include <folly/MPMCQueue.h>
#include <ErrorHandling.hpp>
#include <FixedSizeBufferPool.hpp>
#include <TupleBufferImpl.hpp>

#include <liburing.h>

#include <Runtime/BufferManagerImpl.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <fmt/core.h>
#include <folly/Synchronized.h>

namespace NES::Memory
{

static inline std::atomic<size_t> spilled = 0;
static inline std::atomic<size_t> repinned = 0;


std::shared_ptr<BufferManager> BufferManager::create(
    uint32_t bufferSize,
    uint32_t numOfBuffers,
    std::shared_ptr<std::pmr::memory_resource> memoryResource,
    uint32_t withAlignment,
    uint32_t checksAddedPerNewBuffer,
    uint64_t bufferChecksThreshold,
    uint32_t uringRingSize,
    uint32_t spillBatchSize,
    uint64_t maxFileSize,
    uint32_t maxZeroWriteRounds,
    uint32_t maxConcurrentMemoryReqs,
    uint32_t maxConcurrentReadSubmissions,
    uint32_t maxConcurrentHolePunchSubmissions,
    std::filesystem::path spillDirectory,
    std::chrono::duration<float> waitForUringMutexSeconds)
{
    return std::make_shared<BufferManager>(
        Private{},
        bufferSize,
        numOfBuffers,
        std::move(memoryResource),
        withAlignment,
        checksAddedPerNewBuffer,
        bufferChecksThreshold,
        uringRingSize,
        spillBatchSize,
        maxFileSize,
        maxZeroWriteRounds,
        maxConcurrentMemoryReqs,
        maxConcurrentReadSubmissions,
        maxConcurrentHolePunchSubmissions,
        spillDirectory,
        waitForUringMutexSeconds);
}


BufferManager::BufferManager(
    Private,
    const uint32_t bufferSize,
    const uint32_t numOfBuffers,
    const std::shared_ptr<std::pmr::memory_resource>& memoryResource,
    const uint32_t withAlignment,
    const uint32_t checksAddedPerNewBuffer,
    const uint64_t bufferChecksThreshold,
    const uint32_t uringRingSize,
    const uint32_t spillBatchSize,
    const uint64_t maxFileSize,
    const uint32_t maxZeroWriteRounds,
    const uint32_t maxConcurrentMemoryReqs,
    const uint32_t maxConcurrentReadSubmissions,
    const uint32_t maxConcurrentHolePunchSubmissions,
    const std::filesystem::path& spillDirectory,
    const std::chrono::duration<float> waitForUringMutexSeconds)
    : waitForUringMutexSeconds(waitForUringMutexSeconds)
    , newBuffers((bufferChecksThreshold / checksAddedPerNewBuffer) * 64)
    , checksAddedPerNewBuffer(checksAddedPerNewBuffer)
    , bufferChecksThreshold(bufferChecksThreshold)
    , unpooledChunksManager(memoryResource, withAlignment, this)
    , availableBuffers(numOfBuffers)
    , bufferSize(bufferSize)
    , numOfBuffers(numOfBuffers)
    , memoryResource(memoryResource)
    , spillDirectory(spillDirectory)
    , maxFileSize(maxFileSize)
    , waitingSegmentRequests(maxConcurrentMemoryReqs)
    , files{std::pair{1, prepareFile(spillDirectory, 1)}}
    , spillBatchSize(spillBatchSize)
    , maxZeroWriteRounds(maxZeroWriteRounds)
    , waitingReadRequests(maxConcurrentReadSubmissions)
    , waitingPunchHoleRequests(maxConcurrentHolePunchSubmissions)
    , holesInProgress(maxConcurrentHolePunchSubmissions * 10)
{
    ((void)withAlignment);
    initialize(DEFAULT_ALIGNMENT);
    if (const auto e = io_uring_queue_init(uringRingSize, &uringWriteRing, 0); e < 0)
    {
        throw FailedToInitUringBuffers(
            "Error while initializing io_uring write ring with size {}: {}", uringRingSize, std::string{std::strerror(-e)});
    }
    if (const auto e = io_uring_queue_init(uringRingSize, &uringReadRing, 0); e < 0)
    {
        throw FailedToInitUringBuffers(
            "Error while initializing io_uring read ring with size {}: {}", uringRingSize, std::string{std::strerror(-e)});
    }
    if (const auto e = io_uring_queue_init(uringRingSize, &uringPunchHoleRing, 0); e < 0)
    {
        throw FailedToInitUringBuffers(
            "Error while initializing io_uring punch hole ring with size {}: {}", uringRingSize, std::string{std::strerror(-e)});
    }
}

BufferManager::~BufferManager()
{
    BufferManager::destroy();
}

void BufferManager::initialize(uint32_t withAlignment)
{
    std::unique_lock lock(availableBuffersMutex);

    const size_t pages = sysconf(_SC_PHYS_PAGES);
    size_t page_size = sysconf(_SC_PAGE_SIZE);
    auto memorySizeInBytes = pages * page_size;

    uint64_t requiredMemorySpace = this->bufferSize * this->numOfBuffers;
    double percentage = (100.0 * requiredMemorySpace) / memorySizeInBytes;
    NES_DEBUG("NES memory allocation requires {} out of {} (so {}%) available bytes", requiredMemorySpace, memorySizeInBytes, percentage);

    ///    NES_ASSERT2_FMT(bufferSize && !(bufferSize & (bufferSize - 1)), "size must be power of two " << bufferSize);
    INVARIANT(
        requiredMemorySpace < memorySizeInBytes,
        "NES tries to allocate more memory than physically available requested={} available={}",
        requiredMemorySpace,
        memorySizeInBytes);
    if (withAlignment > 0)
    {
        PRECONDITION(
            !(withAlignment & (withAlignment - 1)), "NES tries to align memory but alignment={} is not a pow of two", withAlignment);
    }
    PRECONDITION(withAlignment <= page_size, "NES tries to align memory but alignment is invalid: {} <= {}", withAlignment, page_size);

    PRECONDITION(
        alignof(detail::BufferControlBlock) <= withAlignment,
        "Requested alignment is too small, must be at least {}",
        alignof(detail::BufferControlBlock));

    allBuffers.reserve(numOfBuffers);

    auto controlBlockSize = alignBufferSize(sizeof(detail::BufferControlBlock), withAlignment);
    // auto initialControlBlockAreaSize = controlBlockSize * numOfBuffers;
    // initialControlBlockAreaSize = alignBufferSize(initialControlBlockAreaSize, withAlignment);
    // auto controlBlockBasePointer = static_cast<uint8_t*>(memoryResource->allocate(initialControlBlockAreaSize, withAlignment));

    auto alignedBufferSize = alignBufferSize(bufferSize, withAlignment);
    allocatedBufferAreaSize = alignBufferSize(alignedBufferSize, withAlignment);
    const size_t offsetBetweenBuffers = allocatedBufferAreaSize;
    allocatedBufferAreaSize *= numOfBuffers;
    bufferBasePointer = static_cast<uint8_t*>(memoryResource->allocate(allocatedBufferAreaSize, withAlignment));
    NES_TRACE(
        "Allocated {} bytes with alignment {} buffer size {} num buffer {} controlBlockSize {} {}",
        allocatedBufferAreaSize,
        withAlignment,
        alignedBufferSize,
        numOfBuffers,
        controlBlockSize,
        alignof(detail::BufferControlBlock));
    INVARIANT(bufferBasePointer, "memory allocation failed, because 'basePointer' was a nullptr");
    uint8_t* ptr = bufferBasePointer;
    for (size_t i = 0; i < numOfBuffers; ++i)
    {
        availableBuffers.write(detail::DataSegment{detail::InMemoryLocation{ptr}, bufferSize});
        ptr += offsetBetweenBuffers;
    }
    NES_DEBUG("BufferManager configuration bufferSize={} numOfBuffers={}", this->bufferSize, this->numOfBuffers);
}


detail::File BufferManager::prepareFile(const std::filesystem::path& dirPath, const uint8_t id)
{
    const std::filesystem::path filePath = dirPath / std::to_string(id);
    // const auto filePath = dirPath / "XXXXXXXX";
    std::filesystem::create_directories(dirPath);
    // auto tmpFilePath = std::make_unique<char[]>(filePath.native().length());
    // filePath.native().copy(tmpFilePath.get(), filePath.native().length());

    auto fd = open(filePath.c_str(), O_TRUNC | O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    //Not using O_DIRECT for now because apparently buffers need to be aligned, which the unpooled buffers are not
    // const auto fd = mkstemp(tmpFilePath.get());
    INVARIANT(fd >= 0, "Failed to open swap file {} with error \"{}\"", filePath.string(), std::strerror(errno));
    return detail::File{id, fd};
}


void BufferManager::destroy()
{
    bool expected = false;
    if (isDestroyed.compare_exchange_strong(expected, true))
    {
        NES_DEBUG("Shutting down Buffer Manager");
        std::scoped_lock lock(availableBuffersMutex, unpooledBuffersMutex, localBufferPoolsMutex, writeSqeMutex, allBuffersMutex);
        flushNewBuffers();
        for (auto& localPool : localBufferPools)
        {
            if (auto locked = localPool.lock())
            {
                locked->destroy();
            }
        }
        localBufferPools.clear();
        cleanupAllBuffers(0);

        PunchHoleFuture punchHoleFuture;
        while (holesInProgress.read(punchHoleFuture))
        {
        }
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
        if (allBuffers.size() > 0)
        {
            NES_WARNING("[BufferManager] Destroying BufferManager while buffers are still used")
            for (auto& buffer : allBuffers)
            {
                if (buffer->getDataReferenceCount() != 0)
                {
                    buffer.controlBlock->dumpOwningThreadInfo();
                    success = false;
                }
            }
        }
#endif
        INVARIANT(
            allBuffers.size() == 0,
            "[BufferManager] Requested buffer manager shutdown but a buffer is still used allBuffers={}",
            allBuffers.size());
        /// RAII takes care of deallocating memory here
        allBuffers.clear();

        availableBuffers = decltype(availableBuffers)();
        for (auto& holder : unpooledBuffers)
        {
            if (!holder || holder->getDataReferenceCount() != 0)
            {
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
                if (holder.segment)
                {
                    holder.segment->controlBlock->dumpOwningThreadInfo();
                }
#endif
                INVARIANT(
                    false,
                    "Deletion of unpooled buffer invoked on used memory segment size={} refcnt={}",
                    holder->getData().getSize(),
                    holder->getDataReferenceCount());
            }
        }
        unpooledBuffers.clear();
        NES_DEBUG("Shutting down Buffer Manager completed");
        memoryResource->deallocate(bufferBasePointer, allocatedBufferAreaSize);

        io_uring_queue_exit(&uringWriteRing);

        for (auto file : files)
        {
            if (close(file.second.getFd()) != 0)
            {
                NES_ERROR("Failed to close swap file {} with error \"{}\"", file.first, std::strerror(errno));
            }

            const std::filesystem::path filePath = spillDirectory / std::to_string(file.second.getId());
            std::remove(filePath.c_str());
        }
        NES_INFO("Spilled {}, and repinned {} buffers", spilled.load(), repinned.load());
    }
    else
    {
        NES_ERROR("Trying to destroy buffer manager more then once");
    }
}

void BufferManager::flushNewBuffers() noexcept
{
    auto toFlush = newBuffers.size();
    NES_DEBUG("Flushing {} new buffers to allBuffers", toFlush);
    allBuffers.reserve(allBuffers.size() + toFlush);
    detail::BufferControlBlock* bcb;
    while (toFlush-- > 0 && newBuffers.read(bcb))
    {
        allBuffers.push_back(bcb);
    }
}

void BufferManager::cleanupAllBuffers(size_t maxIter)
{
    //Instead of implementing the logic for updating indices while erasing while iterating over an interval, I just use the
    //remove-erase idiom that's common in c++ two times.
    //This makes this a "wrapping" iteration that goes back to the beginning once the end has been reached, but we want to process more entries.
    //First interval: current position up until the maximum number of BCBs to be checked or the end, whichever comes first
    //Second interval if the first interval goes to end: Beginning till maxiter BCBs have been checked, or all BCBs have been checked.
    //Then, we can use the normal remove-erase idiom to remove elements from the container

    auto begin = allBuffers.begin() + clockAt;
    //exclusive end indices
    decltype(begin) firstEnd;
    decltype(begin) secondEnd;

    if (maxIter == 0)
    {
        maxIter = allBuffers.size();
    }
    else
    {
        //We only need to check every buffer once
        maxIter = std::min(maxIter, allBuffers.size());
    }

    //Strictly less than, because if all buffers are processed we must wrap around the index for the next time
    if (clockAt + maxIter < allBuffers.size())
    {
        firstEnd = allBuffers.begin() + (clockAt + maxIter);
        secondEnd = firstEnd;
    }
    else
    {
        firstEnd = allBuffers.end();
        secondEnd = allBuffers.begin() + ((clockAt + maxIter) - allBuffers.size());
    }

    auto check = [&](detail::BufferControlBlock* bcb)
    {
        if (bcb->segmentMutex.try_lock())
        {
            if (bcb->getDataReferenceCount() == 0)
            {
                delete bcb;
                return true;
            }
            bcb->segmentMutex.unlock();
        }
        return false;
    };

    //Remove-Erase idiom two times
    const auto firstRemovedFrom = std::remove_if(begin, firstEnd, check);
    auto erased = allBuffers.erase(firstRemovedFrom, firstEnd);
    NES_DEBUG("Freed {} buffer control blocks.", erased - begin);
    if (secondEnd != firstEnd)
    {
        const auto secondRemovedFrom = std::remove_if(allBuffers.begin(), secondEnd, check);
        erased = allBuffers.erase(secondRemovedFrom, secondEnd);
        NES_DEBUG("Freed {} buffer control blocks.", erased - allBuffers.begin());
    }
    ptrdiff_t difference = erased - allBuffers.begin();
    allBuffers.shrink_to_fit();

    clockAt = difference;
}

RepinBufferFuture BufferManager::repinBuffer(FloatingBuffer&& floating_) noexcept
{
    using Promise = RepinBufferPromise<RepinBufferFuture>;
    PRECONDITION(floating_.controlBlock != nullptr, "Cannot repin empty floating buffer");
    /* Repinning Buffers in a thread safe manner:
     * 1. Acquire lock on BCB with startRepinning. StartRepinning also indicates to second chance that this buffer should be skipped.
     * 2. Do the actual repinning:
     *      1. Get memory segments for the spilled segments,
     *      2. Read data from disk into in memory segments,
     *      3. Initiate deletion of on disk data.
     * 3. Pin the BCB before releasing the lock, if we wouldn't, the buffer could get spilled again.
     * 4. Reset BCBs data structures so that when unpinning it could get spilled again with markRepinningDone.
     * 5. Release lock.
     * 6. Create pinned buffer.
     * 7. Release the additional pin and that of the floating buffer.
    */
    FloatingBuffer floating = floating_;
    detail::RefCountedBCB<true> pin{};
    {
        //Locking and unlocking the BCB must be done from the same thread.
        //We assume that we are here in the thread that started the coroutine and will enforce later that the lock is given up on the same thread
        if (auto lock = floating.controlBlock->startRepinning())
        {
            //Ensure that isRepinning is set to true, so that
            std::vector<detail::ChildOrMainDataKey> toRepin{};
            //toRepin is for most parts enough, but for the deletion we need the old segments as well
            std::vector<detail::DataSegment<detail::OnDiskLocation>> segmentToUnspill{};
            uint32_t numChildren = floating.controlBlock->getNumberOfChildrenBuffers();
            toRepin.reserve(numChildren + 1);
            segmentToUnspill.reserve(numChildren + 1);

            //Find out which segments need to be unspilled
            for (unsigned int i = 0; i < floating.controlBlock->children.size(); ++i)
            {
                if (auto childSegment = floating.controlBlock->getChild(i, *lock); childSegment->isSpilled())
                {
                    toRepin.emplace_back(ChildKey{i});
                    segmentToUnspill.push_back(*childSegment->get<detail::OnDiskLocation>());
                }
            }
            if (auto mainSegment = floating.controlBlock->getData().get<detail::OnDiskLocation>())
            {
                toRepin.emplace_back(detail::ChildOrMainDataKey::MAIN());
                segmentToUnspill.push_back(*mainSegment);
            }

            //get in memory segments for segments that need to be unspilled
            auto memorySegmentFuture = getInMemorySegment(toRepin.size());
            auto awaitMemorySegments
                = detail::AwaitExternalProgress<Promise, decltype(memorySegmentFuture.waitUntilDone()), GetInMemorySegmentFuture>::create(
                    std::bind(&GetInMemorySegmentFuture::pollOnce, memorySegmentFuture),
                    std::bind(&GetInMemorySegmentFuture::waitOnce, memorySegmentFuture),
                    memorySegmentFuture);
            auto memorySegmentsOrError = co_await *awaitMemorySegments;
            if (auto error = getOptional<detail::CoroutineError>(memorySegmentsOrError))
            {
                NES_TRACE("Failed to get memory segments for repinning with error message", typeFromCode(*error).getErrorMessage());
                // auto switchBackAwaiter = std::make_shared<detail::ResumeAfterBlockAwaiter<Promise>>([] {}, [] { return false; });
                // auto aepSwitchBackAwaiter = createAEPFromResumeAfterBlock(switchBackAwaiter);
                // co_await *aepSwitchBackAwaiter;
                // INVARIANT(static_cast<std::unique_lock<std::shared_mutex>&>(lock).owns_lock(), "Didn't own lock on release");
                co_return *error;
            }
            auto memorySegments = std::get<std::vector<detail::DataSegment<detail::InMemoryLocation>>>(memorySegmentsOrError);

            if (memorySegments.size() < toRepin.size())
            {
                NES_DEBUG("Didn't get enough buffer to repin buffer fully");
                for (auto segment : memorySegments)
                {
                    availableBuffers.write(segment);
                }
                // auto switchBackAwaiter = std::make_shared<detail::ResumeAfterBlockAwaiter<Promise>>([] {}, [] { return false; });
                // auto aepSwitchBackAwaiter = createAEPFromResumeAfterBlock(switchBackAwaiter);
                // co_await *aepSwitchBackAwaiter;
                // INVARIANT(static_cast<std::unique_lock<std::shared_mutex>&>(lock).owns_lock(), "Didn't own lock on release");
                co_return static_cast<uint32_t>(ErrorCode::CannotSpillEnoughBuffers);
            }

            //Read data from on disk segments in to in memory segments.
            //First initiate all reading, then await reading
            std::vector<ReadSegmentFuture> readSegmentFutures{};
            readSegmentFutures.reserve(toRepin.size());
            for (unsigned long i = 0; i < toRepin.size(); ++i)
            {
                auto readFuture = readOnDiskSegment(
                    *floating.controlBlock->getSegment(toRepin[i], *lock)->get<detail::OnDiskLocation>(), memorySegments[i]);
                readSegmentFutures.push_back(readFuture);
            }
            for (unsigned long i = 0; i < toRepin.size(); ++i)
            {
                auto readFuture = readSegmentFutures[i];
                auto awaitReadSegment
                    = detail::AwaitExternalProgress<Promise, std::variant<ssize_t, detail::CoroutineError>, ReadSegmentFuture>::create(
                        std::bind(&ReadSegmentFuture::pollOnce, readFuture),
                        std::bind(&ReadSegmentFuture::waitOnce, readFuture),
                        readFuture);

                const auto readBytesOrError = co_await *awaitReadSegment;
                if (const auto error = getOptional<detail::CoroutineError>(readBytesOrError))
                {
                    for (auto segment : memorySegments)
                    {
                        availableBuffers.write(segment);
                    }
                    // auto switchBackAwaiter = std::make_shared<detail::ResumeAfterBlockAwaiter<Promise>>([] {}, [] { return false; });
                    // auto aepSwitchBackAwaiter = createAEPFromResumeAfterBlock(switchBackAwaiter);
                    // co_await *aepSwitchBackAwaiter;
                    co_return *error;
                }
                if (auto readBytes = std::get<long>(readBytesOrError); readBytes < 0)
                {
                    for (auto segment : memorySegments)
                    {
                        availableBuffers.write(segment);
                    }
                    NES_ERROR("Failed to read spilled buffer with error {}", std::strerror(-readBytes));

                    // auto switchBackAwaiter = std::make_shared<detail::ResumeAfterBlockAwaiter<Promise>>([] {}, [] { return false; });
                    // auto aepSwitchBackAwaiter = createAEPFromResumeAfterBlock(switchBackAwaiter);
                    // co_await *aepSwitchBackAwaiter;
                    co_return static_cast<uint32_t>(ErrorCode::CannotReadBufferFromDisk);
                }
                const auto swapped = floating.controlBlock->swapSegment(memorySegments[i], toRepin[i], *lock);
                INVARIANT(swapped, "Failed to swap back in unspilled in memory data segment");

                const auto punchHoleFuture = punchHoleSegment(std::move(segmentToUnspill[i]));
                //Directly awaiting here is fine because we never actually suspend in punchHole until we have yielded a value
                //Still, would be good to have some mechanism here that doesn't require hard coupling of the implementation of the coroutines
                auto submittedDeletion = co_await punchHoleFuture.awaitYield();
                if (!submittedDeletion)
                {
                    NES_WARNING("Failed to submit segment deletion task, segment will only be removed from disk on system shutdown.");
                }
            }
            pin = floating.controlBlock->getCounter<true>();

            //switch back to thread that started the coroutine to make sure the lock is released correctly
            // auto switchBackAwaiter = std::make_shared<detail::ResumeAfterBlockAwaiter<Promise>>([] {}, [] { return false; });
            // auto aepSwitchBackAwaiter = createAEPFromResumeAfterBlock(switchBackAwaiter);
            // co_await *aepSwitchBackAwaiter;
        }
        else
        {
            co_return static_cast<uint32_t>(ErrorCode::BufferAlreadyRepinning);
        }
    }

    const auto memorySegment = floating.getSegment().get<detail::InMemoryLocation>();
    INVARIANT(memorySegment, "Floating buffer repinned successfully but data segment was still spilled");
    auto result = TupleBuffer{floating.controlBlock, *memorySegment, floating.childOrMainData};
    floating.controlBlock = nullptr;
    floating.childOrMainData = detail::ChildOrMainDataKey::UNKNOWN();
    result.controlBlock->dataRelease();
    repinned.fetch_add(1);
    co_return result;
}


TupleBuffer BufferManager::pinBuffer(FloatingBuffer&& floating)
{
    auto repinResult = repinBuffer(std::move(floating)).waitUntilDone();
    if (const auto error = getOptional<detail::CoroutineError>(repinResult))
    {
        throw typeFromCode(*error).create();
    }
    return std::get<TupleBuffer>(repinResult);
}


GetInMemorySegmentFuture BufferManager::getInMemorySegment(size_t amount) noexcept
{
    using Promise = GetInMemorySegmentPromise<GetInMemorySegmentFuture>;
    //Making a shared ptr out of this is probably not worth it, as in most request amount == 1 and DataSegments are small,
    //so copying is probably cheaper
    std::vector<detail::DataSegment<detail::InMemoryLocation>> result{};
    result.reserve(amount);
    auto inMemorySegment = detail::DataSegment<detail::InMemoryLocation>{};
    while (result.size() < amount && availableBuffers.read(inMemorySegment))
    {
        result.push_back(inMemorySegment);
    }
    auto missingSegments = amount - result.size();
    if (missingSegments == 0)
    {
        co_return result;
    }
    auto freeResult = [&]
    {
        for (auto segment : result)
        {
            const auto written = availableBuffers.writeIfNotFull(segment);
            INVARIANT(written, "Failed to write back in memory segment after returning an error in getInMemorySegment coroutine");
        }
    };

    std::vector<std::shared_ptr<detail::AvailableSegmentAwaiter<Promise>>> segmentAwaiters{};
    segmentAwaiters.reserve(missingSegments);


    for (unsigned long i = 0; i < missingSegments; ++i)
    {
        std::optional<std::shared_ptr<detail::AvailableSegmentAwaiter<GetInMemorySegmentPromise<GetInMemorySegmentFuture>>>>
            inMemorySegmentRequest = detail::AvailableSegmentAwaiter<Promise>::create(waitingSegmentRequests);
        if (!inMemorySegmentRequest)
        {
            freeResult();
            //System is so overloaded that we can't even submit new requests, returning the nullptr segment
            co_return static_cast<uint32_t>(ErrorCode::CannotSubmitMemoryRequest);
        }
        segmentAwaiters.push_back(*inMemorySegmentRequest);
    }
    requiredSegments.fetch_add(missingSegments);

    //Check if there are enough spilling operations in flight to satisfy the currently waiting coroutines
    auto fillSQE = false;
    {
        auto checked = false;
        while (!checked)
        {
            // std::shared_lock checkNeedLock{writeSqeMutex, std::defer_lock};
            auto resumeAfterBlock = std::make_shared<detail::ResumeAfterBlockAwaiter<Promise>>(
                [&] { writeSqeMutex.lock_shared(); },
                // std::bind(&std::shared_lock<std::shared_timed_mutex>::try_lock_for, &checkNeedLock),
                [&] { return writeSqeMutex.try_lock_shared(); });

            auto awaiter = createAEPFromResumeAfterBlock(resumeAfterBlock);
            co_await *awaiter;
            {
                if (requiredSegments.load() > buffersBeingSpilled.load())
                {
                    fillSQE = true;
                }
                checked = true;
            }
            writeSqeMutex.unlock_shared();
        }
    }

    size_t lastCompleted = 0;
    unsigned int zeroRounds = 0;
    while (fillSQE)
    {
        //atomic flag is not strictly necessary for this method, but it allows the other member functions to know whether we currently spill
        //TODO change to atomic int so that we can mark sqe and cqe processing in parallel
        auto resumeAfterBlock = std::make_shared<detail::ResumeAfterBlockAwaiter<Promise>>(
            [&] { std::lock(allBuffersMutex, writeSqeMutex); }, [&] { return std::try_lock(allBuffersMutex, writeSqeMutex) == -1; });
        auto awaiter = createAEPFromResumeAfterBlock(resumeAfterBlock, zeroRounds > 0, true);
        co_await *awaiter;
        auto added = secondChancePass();
        buffersBeingSpilled.fetch_add(added);
        missingSegments = std::max(static_cast<int64_t>(missingSegments) - static_cast<int64_t>(added), 0L);
        //Coroutines only need to wait until they initiated the spilling of one segment.
        //If other threads need more memory, they can try to spill some buffers as well.
        if (missingSegments == 0)
        {
            fillSQE = false;
        }
        if (added == 0)
        {
            //Another thread might have completed our awaiters and we don't even notice because we are still spinning in the while loop
            //trying to spill when it might not even be possible to spill more
            //TODO replace spinning over segment awaiters with an awaitEither
            size_t completed = 0;
            for (auto segmentAwaiter : segmentAwaiters)
            {
                if (segmentAwaiter->isDone())
                {
                    completed++;
                }
            }
            if (lastCompleted == completed)
            {
                zeroRounds++;
            }
            lastCompleted = completed;
        }

        if (zeroRounds == maxZeroWriteRounds)
        {
            requiredSegments.fetch_sub(missingSegments);
            freeResult();
        }
        allBuffersMutex.unlock();
        writeSqeMutex.unlock();
        if (zeroRounds == maxZeroWriteRounds)
        {
            co_return static_cast<detail::CoroutineError>(ErrorCode::CannotSpillEnoughBuffers);
        }
    }


    //Polling the completion events is not done inside the coroutine, because:
    //1. CEs might indicate failure
    //2. other coroutines might be waiting longer for segments than the one processing them, so fulfilling them through the request queue guarantees some fairness
    //3. in the future, we would probably like to have segmentRequests for multiple segments at once to avoid filling up the request queue
    //      and make progress in the workers more deterministic
    for (auto segmentAwaiter : segmentAwaiters)
    {
        auto awaitForProgressPtr = detail::AwaitExternalProgress<
            Promise,
            std::optional<detail::DataSegment<detail::InMemoryLocation>>,
            detail::AvailableSegmentAwaiter<Promise>,
            true>::
            create(
                //If too many of the buffers that we tried to spill get repinned in the meantime, the awaiters will not be completed via processing the completion queue
                //because we didn't initiate the spilling of more buffers.
                //In that case, we resume the awaiters directly with an empty buffer, so that we can spill some more buffers
                [&] { pollWriteCompletionEventsOnce(); },
                [&] { waitForWriteCompletionEventsOnce(); },
                std::weak_ptr(segmentAwaiter));
        auto freedSegment = co_await *awaitForProgressPtr;
        INVARIANT(freedSegment.has_value(), "GetInMemorySegmentFuture resumed before inMemory segment was set");
        if (freedSegment->getLocation().getPtr() != nullptr)
        {
            requiredSegments.fetch_sub(1);
            result.push_back(*freedSegment);
        }
    }
    requiredSegments.fetch_sub(segmentAwaiters.size() - result.size());
    co_return result;
}

int64_t BufferManager::secondChancePass() noexcept
{
    //Flush outstanding new buffers while we are at it
    flushNewBuffers();
    unsigned int currentBatchSize = 0;
    //Can we allocate the SegmentWriteRequest directly in the vector?
    auto addToBatch
        = [&](detail::BufferControlBlock* bcb, const detail::DataSegment<detail::DataLocation>& dataSegment, detail::ChildOrMainDataKey key)
    {
        if (auto inMemorySegment = dataSegment.get<detail::InMemoryLocation>())
        {
            //Switch to next file if size of current file would exceed the given maximum size
            fileOffset += inMemorySegment->getSize();
            if (fileOffset + inMemorySegment->getSize() > maxFileSize)
            {
                auto file = prepareFile(spillDirectory, files.rbegin()->second.getId() + 1);
                files.insert(std::pair{file.getId(), file});
                fileOffset = 0;
            }
            auto* uringRequest = new detail::SegmentWriteRequest{bcb, *inMemorySegment, key, files.rbegin()->second, fileOffset};
            io_uring_sqe* sqe = io_uring_get_sqe(&uringWriteRing);
            io_uring_prep_write(
                sqe,
                files.rbegin()->second.getFd(),
                uringRequest->getSegment().getLocation().getPtr(),
                uringRequest->getSegment().getSize(),
                uringRequest->getFileOffset());
            io_uring_sqe_set_data(sqe, uringRequest);
            ++currentBatchSize;
        }
    };

    bool spilledLastFully = false;
    const size_t clockStart = clockAt;
    size_t deleted = 0;
    size_t i = clockAt;
    //We only do two rounds over the buffers to avoid looping forever in case there are less than spillBatchSize spillable buffers
    //The starting point can be hit three times, but we explicitly move the clock behind the last fully spilled buffer, so that it is guaranteed that its last in the next round.
    for (; (((i - (clockStart)) + allBuffers.size() - 1) / allBuffers.size()) < 3 && currentBatchSize < spillBatchSize; i++)
    {
        //We only move the clock forward inside the loop, so if not all segments of a buffer where spilled it doesn't move forward
        clockAt = i % allBuffers.size();
        detail::BufferControlBlock* const currentBCB = allBuffers[clockAt];
        //Do cleanup of unused BCBs as we go
        if (currentBCB->getDataReferenceCount() == 0
            && ((clockStart + deleted < allBuffers.size() && (clockAt >= (clockStart + deleted) || clockAt < clockStart))
                || (clockStart + deleted >= allBuffers.size() && (clockAt > (clockStart + deleted) % allBuffers.size())
                    && clockAt < clockStart)))
        {
            size_t swapTo = (clockStart + deleted++) % allBuffers.size();
            //Swap deleted entries at the beginning of the clock range.
            //std::remove would swap them towards the end, but because we do not have a hard end bound swapping to the clock start seems more reasonable
            //This does mess up the order of the buffers a bit, but the effect on second chance should be small, because we are only swapping
            //with positions that we have seen at least once.
            std::swap(allBuffers[swapTo], allBuffers[clockAt]);
        }
        else if (
            currentBCB->getDataReferenceCount() != 0 && currentBCB->getPinnedReferenceCount() == 0
            && currentBCB->clockReference.test_and_set() && !currentBCB->isRepinning.test())
        {
            auto spillLock = currentBCB->tryLockUnique();
            if (!spillLock)
                continue;
            uint32_t childrenToTake = std::min(spillBatchSize - currentBatchSize, static_cast<uint32_t>(currentBCB->children.size()));
            uint32_t startAt;
            auto skipSpillingUpTo = currentBCB->skipSpillingUpTo.load();
            if (auto startChild = skipSpillingUpTo.asChildKey())
            {
                startAt = *startChild + 1;
            }
            else if (skipSpillingUpTo == detail::ChildOrMainDataKey::UNKNOWN())
            {
                startAt = 0;
            }
            else
            {
                //equivalent to:
                //  currentBCB->skipSpillingUpTo == detail::ChildOrMainDataKey::MAIN()
                //Spilling is already initiated for all children and the main segment;
                spilledLastFully = true;
                continue;
            }
            auto child = currentBCB->children.begin();
            for (child = child + startAt; child != currentBCB->children.end() && childrenToTake != 0; ++child)
            {
                if (!child->isSpilled() && !child->isNotPreAllocated())
                {
                    addToBatch(
                        currentBCB,
                        *child,
                        detail::ChildOrMainDataKey{ChildKey{static_cast<unsigned long>(child - currentBCB->children.begin())}});
                    childrenToTake--;
                }
            }
            //When all children are spilled, spill also the main data segment
            if (spillBatchSize - currentBatchSize > 0)
            {
                auto segment = currentBCB->data.load().get<detail::InMemoryLocation>();
                if (segment && segment->getLocation().getPtr() != nullptr && !segment->isNotPreAllocated())
                {
                    addToBatch(currentBCB, *segment, detail::ChildOrMainDataKey::MAIN());
                    currentBCB->skipSpillingUpTo.compare_exchange_strong(skipSpillingUpTo, detail::ChildOrMainDataKey::MAIN());
                }
                spilledLastFully = true;
            }
            else
            {
                spilledLastFully = false;
                currentBCB->skipSpillingUpTo.compare_exchange_strong(
                    skipSpillingUpTo,
                    detail::ChildOrMainDataKey{ChildKey{static_cast<unsigned long>(child - currentBCB->children.begin()) - 1}});
            }
        }
    }

    buffersToCheck = std::max(static_cast<signed long long>(buffersToCheck) - static_cast<signed long long>(i), 0LL);

    if (deleted > 0)
    {
        //You could also swap everything to the start or end and then do only one erase,
        //But I assume that wrapping around in deletion is relatively rare because
        //All buffers should be very large and I expect most second chance invocations to not pass all buffers even once.
        auto firstBegin = allBuffers.begin() + clockStart;
        auto firstEnd = allBuffers.begin() + std::min(clockStart + deleted, allBuffers.size());
        auto oldAllBuffersSize = allBuffers.size();
        std::for_each(firstBegin, firstEnd, [&](detail::BufferControlBlock* bcb) { delete bcb; });
        allBuffers.erase(firstBegin, firstEnd);

        if (clockStart + deleted > oldAllBuffersSize)
        {
            auto secondBegin = allBuffers.begin();
            auto secondEnd = allBuffers.begin() + (clockStart + deleted - oldAllBuffersSize);
            std::for_each(secondBegin, secondEnd, [](const auto* bcb) { delete bcb; });
            allBuffers.erase(secondBegin, secondEnd);
        }
        //Add oldBufferSize to avoid becoming negative
        clockAt = (clockAt + oldAllBuffersSize - deleted) % allBuffers.size();
    }
    //Move clock one forward if the last buffer was spilled completely.
    //spilledLastFully is false when we spilled children, but then not the parent.
    if (spilledLastFully)
    {
        clockAt = (clockAt + 1) % allBuffers.size();
    }

    const int uringSubmitted = io_uring_submit(&uringWriteRing);
    NES_DEBUG(
        "Did second chance pass to try spill buffers, send requests for {} buffers, and deleted {} empty BCB. "
        "Clock was at {}, is now at {}, iterated over {} BCBs, while allBuffers size was {}",
        uringSubmitted,
        deleted,
        clockStart,
        clockAt,
        i,
        allBuffers.size());

    return uringSubmitted;
}

void BufferManager::pollWriteCompletionEventsOnce() noexcept
{
    std::unique_lock cqeLock{writeCqeMutex, std::defer_lock};
    if (cqeLock.try_lock())
    {
        processWriteCompletionEvents();
    }
}
void BufferManager::waitForWriteCompletionEventsOnce() noexcept
{
    std::unique_lock cqeLock{writeCqeMutex};
    processWriteCompletionEvents();
}

size_t BufferManager::processWriteCompletionEvents() noexcept
{
    size_t counter = 0;
    io_uring_cqe* completion = nullptr;
    while (io_uring_peek_cqe(&uringWriteRing, &completion) == 0)
    {
        auto* uringData = reinterpret_cast<detail::SegmentWriteRequest*>(completion->user_data);
        if (completion->res != static_cast<long>(uringData->getSegment().getSize()))
        {
            //Buffer was not spilled completely, so we can't reuse its memory segment
            //We just skip it, the buffer would get spilled again at a later point anyway
            //If there is an error, we log it up to 5 times
            if (completion->res < 0 && writeErrorCounter < 6)
            {
                std::string lastMessage;
                if (writeErrorCounter == 5)
                {
                    lastMessage = "\nReached maximum number of error messages for this spilling attempt, muting further errors";
                }
                NES_ERROR(
                    "Failed to spill buffer of size {} to file {} with error message {}{}",
                    uringData->getSegment().getSize(),
                    uringData->getFileId().getId(),
                    std::strerror(-completion->res),
                    lastMessage);
                ++writeErrorCounter;
            }
            continue;
        }

        auto onDiskSegment = detail::DataSegment{
            detail::OnDiskLocation{uringData->getFileId().getId(), uringData->getFileOffset()}, uringData->getSegment().getSize()};
        bool successFullSwap = false;

        if (uringData->getKey() == detail::ChildOrMainDataKey::MAIN())
        {
            successFullSwap = uringData->getControlBlock()->swapDataSegment(onDiskSegment);
            // if (!successFullSwap)
            // {
            //     auto resetLock = uringData->getControlBlock()->startRepinning();
            // }
        }
        else if (auto childKey = uringData->getKey().asChildKey())
        {
            successFullSwap = uringData->getControlBlock()->swapChild(onDiskSegment, *childKey);
        }
        else
        {
            INVARIANT(false, "Cannot swap segment at unknown BufferControlBlock position");
        }
        if (successFullSwap)
        {
            if (std::weak_ptr<detail::AvailableSegmentAwaiter<GetInMemorySegmentPromise<GetInMemorySegmentFuture>>> weakAwaiter{};
                waitingSegmentRequests.read(weakAwaiter))
            {
                if (auto awaiter = weakAwaiter.lock())
                {
                    //save result in awaiter and resume coroutine
                    awaiter->setResultAndContinue(uringData->getSegment());
                }
                //Without this else clause in memory segments sometimes get lost, which I don't really understand because in theory
                //the available segment awaiters are all only completed in this function which is always behind a lock
                else
                {
                    availableBuffers.write(uringData->getSegment());
                }
            }
            else
            {
                availableBuffers.write(uringData->getSegment());
            }
        }
        buffersBeingSpilled.fetch_sub(1);
        delete uringData;
        io_uring_cqe_seen(&uringWriteRing, completion);
        ++counter;
    }
    if (buffersBeingSpilled == 0)
    {
        unsigned int flushed = 0;
        std::weak_ptr<detail::AvailableSegmentAwaiter<GetInMemorySegmentPromise<GetInMemorySegmentFuture>>> weakAwaiter{};
        while (waitingSegmentRequests.read(weakAwaiter))
        {
            flushed++;
            if (auto awaiter = weakAwaiter.lock())
            {
                NES_TRACE("Resuming coroutine with empty segment");
                //save result in awaiter and resume coroutine
                awaiter->setResultAndContinue(detail::DataSegment<detail::InMemoryLocation>{});
            }
        }
        // NES_DEBUG("Flushed out {} segment awaiters for which there weren't enough requests in flight anymore", flushed);
    }
    spilled.fetch_add(counter);
    return counter;
}


ReadSegmentFuture BufferManager::readOnDiskSegment(
    detail::DataSegment<detail::OnDiskLocation> source, detail::DataSegment<detail::InMemoryLocation> target) noexcept
{
    PRECONDITION(
        source.getSize() <= target.getSize(),
        "Target in memory segment was {} bytes big, must be at least as big as the on disk segment with {} bytes",
        target.getSize(),
        source.getSize());
    using Promise = ReadSegmentPromise<ReadSegmentFuture>;
    auto underlyingSubmissionAwaiter = detail::SubmitSegmentReadAwaiter<Promise>::create(waitingReadRequests, source, target);
    if (!underlyingSubmissionAwaiter)
    {
        co_return -1;
    }

    auto submissionAwaiter = detail::
        AwaitExternalProgress<Promise, std::shared_ptr<detail::ReadSegmentAwaiter<Promise>>, detail::SubmitSegmentReadAwaiter<Promise>>::
            create(
                std::bind(&BufferManager::pollReadSubmissionEntriesOnce, this),
                std::bind(&BufferManager::waitForReadSubmissionEntriesOnce, this),
                std::weak_ptr{underlyingSubmissionAwaiter.value()});

    auto underlyingReadAwaiter = co_await *submissionAwaiter;
    auto readAwaiter = detail::AwaitExternalProgress<Promise, std::optional<ssize_t>, detail::ReadSegmentAwaiter<Promise>>::create(
        std::bind(&BufferManager::pollReadCompletionEntriesOnce, this),
        std::bind(&BufferManager::waitForReadCompletionEntriesOnce, this),
        std::weak_ptr{underlyingReadAwaiter});
    auto readBytes = co_await *readAwaiter;
    co_return *readBytes;
}

void BufferManager::pollReadSubmissionEntriesOnce() noexcept
{
    std::unique_lock sqeLock(readSqeMutex, std::defer_lock);
    if (sqeLock.try_lock())
    {
        processReadSubmissionEntries();
    }
}
void BufferManager::waitForReadSubmissionEntriesOnce() noexcept
{
    std::unique_lock sqeLock(readSqeMutex);
    processReadSubmissionEntries();
}

int64_t BufferManager::processReadSubmissionEntries() noexcept
{
    //If this method should be shared by other coroutines, it would need to be templated accordingly
    using Promise = ReadSegmentPromise<ReadSegmentFuture>;
    std::shared_ptr<detail::SubmitSegmentReadAwaiter<Promise>> nextRequest{};
    //Process a fixed amount of requests, if another request comes it while flushing its fine, its coroutine can flush again itself
    auto numRequests = waitingReadRequests.size();
    auto awaitersToResume = std::vector<decltype(nextRequest)>{};
    awaitersToResume.reserve(numRequests);
    while (numRequests-- > 0 && waitingReadRequests.read(nextRequest))
    {
        io_uring_sqe* sqe = io_uring_get_sqe(&uringReadRing);
        //Allocate the awaiter for the next step on the heap, because it might be fulfilled before we resume the coroutine
        //Now, If another thread processed the completion event before we resume and return the new awaiter,
        //the awaiter is just prefilled and doesn't suspend
        //TODO track amount of in-flight requests and block if overcrowded
        auto readAwaiter = std::make_shared<detail::ReadSegmentAwaiter<Promise>>();
        //We can only attach pointers to iouring requests
        auto* readAwaiterW = new std::weak_ptr{readAwaiter};
        //The ownership of the request goes back to the coroutine
        nextRequest->setResult(std::move(readAwaiter));
        awaitersToResume.push_back(nextRequest);
        io_uring_prep_read(
            sqe,
            files.find(nextRequest->getSource().getLocation().getFileID())->second.getFd(),
            nextRequest->getTarget().getLocation().getPtr(),
            nextRequest->getSource().getSize(),
            nextRequest->getSource().getLocation().getOffset());
        io_uring_sqe_set_data(sqe, readAwaiterW);
    }
    int processed = io_uring_submit(&uringReadRing);
    for (auto toResume : awaitersToResume)
    {
        toResume->setResultAndContinue();
    }
    NES_DEBUG("Processed {} read requests", processed);
    return processed;
}

void BufferManager::pollReadCompletionEntriesOnce() noexcept
{
    std::unique_lock cqeLock{readCqeMutex, std::defer_lock};
    if (cqeLock.try_lock())
    {
        processReadCompletionEvents();
    }
}

void BufferManager::waitForReadCompletionEntriesOnce() noexcept
{
    std::unique_lock cqeLock{readCqeMutex};
    processReadCompletionEvents();
}

size_t BufferManager::processReadCompletionEvents() noexcept
{
    using Promise = ReadSegmentPromise<ReadSegmentFuture>;
    size_t counter = 0;
    io_uring_cqe* completion = nullptr;
    while (io_uring_peek_cqe(&uringReadRing, &completion) == 0)
    {
        const auto* awaiterW = reinterpret_cast<std::weak_ptr<detail::ReadSegmentAwaiter<Promise>>*>(completion->user_data);
        //We do error handling in the coroutine/the caller of the coroutine.
        //We can't just skip not fully read segments like in the write cqe processing, because there is a 1:1 mapping of coroutines and entries
        if (const auto awaiter = awaiterW->lock())
        {
            awaiter->setResultAndContinue(completion->res);
        }
        delete awaiterW;
        io_uring_cqe_seen(&uringReadRing, completion);
        ++counter;
    }
    return counter;
}

TupleBuffer BufferManager::makeBufferAndRegister(const detail::DataSegment<detail::InMemoryLocation> segment) noexcept
{
    auto* const controlBlock = new detail::BufferControlBlock{segment, this};
    //Ensure that BCBs main segment doesn't get spilled until we create the pinned buffer
    //auto pin = controlBlock->getCounter<true>();
    auto pin = controlBlock->getCounter<true>();
    {
        const auto savedNewBuffer = newBuffers.writeIfNotFull(controlBlock);
        INVARIANT(
            savedNewBuffer,
            "Could not save newly created buffer, new buffers size is {}, buffers to check is {}",
            newBuffers.size(),
            buffersToCheck.load());
        size_t checks = buffersToCheck.fetch_add(checksAddedPerNewBuffer);
        NES_TRACE("Checks at {}", checks);
        //If checksAddedPerBuffer > 1, then 2nd chance might make checks == bufferChecksThreshold miss.
        //But, we only want one thread at maximum trying to flush and cleanup
        if (checks >= bufferChecksThreshold && checks < bufferChecksThreshold + checksAddedPerNewBuffer)
        {
            buffersToCheck.fetch_sub(bufferChecksThreshold);
            std::unique_lock lock{allBuffersMutex};
            //TODO verify that no underflow happens
            flushNewBuffers();
            cleanupAllBuffers(bufferChecksThreshold);
        }
    }
    TupleBuffer newBuffer{controlBlock, segment, detail::ChildOrMainDataKey::MAIN()};
    return newBuffer;
}
TupleBuffer BufferManager::getBufferBlocking()
{
    detail::DataSegment<detail::InMemoryLocation> inMemorySegment = detail::DataSegment<detail::InMemoryLocation>{};
    constexpr auto maxAttempts = 20;
    int attempts = 0;
    //Normal retrieval from available buffers
    while (inMemorySegment.getLocation().getPtr() == nullptr && ++attempts < maxAttempts)
    {
        if (availableBuffers.read(inMemorySegment))
        {
            return makeBufferAndRegister(inMemorySegment);
        }
        //If another thread is spilling, wait for new buffers in availableBuffers
        while (isSpilling.test())
        {
            auto deadline = std::chrono::steady_clock::now() + GET_BUFFER_TIMEOUT;
            if (availableBuffers.tryReadUntil(deadline, inMemorySegment))
            {
                return makeBufferAndRegister(inMemorySegment);
            }
        }
        auto retSegment = getInMemorySegment(1).waitUntilDone();
        std::string errorMessage;
        if (std::holds_alternative<std::vector<detail::DataSegment<detail::InMemoryLocation>>>(retSegment))
        {
            auto segments = std::get<std::vector<detail::DataSegment<detail::InMemoryLocation>>>(retSegment);
            if (segments.size() > 0)
            {
                return makeBufferAndRegister(segments.at(0));
            }
            errorMessage = "Could not get enough buffers due to contention";
        }
        else
        {
            errorMessage = typeFromCode(std::get<detail::CoroutineError>(retSegment)).getErrorMessage();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        NES_INFO("Failed to get in get new in memory segments with error message {}", errorMessage);
    }
    /// Throw exception if no buffer was returned allocated after timeout.
    throw BufferAllocationFailure("Global buffer pool could not allocate buffer after {} attempts", maxAttempts);
}

std::optional<TupleBuffer> BufferManager::getBufferNoBlocking()
{
    auto inMemorySegment = detail::DataSegment<detail::InMemoryLocation>{};
    if (!availableBuffers.read(inMemorySegment))
    {
        return std::nullopt;
    }
    return makeBufferAndRegister(inMemorySegment);
}

std::optional<TupleBuffer> BufferManager::getBufferWithTimeout(const std::chrono::milliseconds timeoutMs)
{
    detail::DataSegment<detail::InMemoryLocation> inMemorySegment = detail::DataSegment<detail::InMemoryLocation>{};
    const auto deadline = std::chrono::steady_clock::now() + timeoutMs;
    if (!availableBuffers.tryReadUntil(deadline, inMemorySegment))
    {
        return std::nullopt;
    }
    return makeBufferAndRegister(inMemorySegment);
}

std::optional<TupleBuffer> BufferManager::getUnpooledBuffer(const size_t bufferSize)
{
    auto unpooledBuffer = unpooledChunksManager.getUnpooledBuffer(bufferSize, DEFAULT_ALIGNMENT);
    std::unique_lock allBuffersLock{allBuffersMutex};
    allBuffers.push_back(unpooledBuffer.controlBlock);
    return unpooledBuffer;
    // std::unique_lock lock(unpooledBuffersMutex);
    //
    // auto alignedBufferSize = alignBufferSize(bufferSize, DEFAULT_ALIGNMENT);
    // auto* bufferPtr = static_cast<uint8_t*>(memoryResource->allocate(alignedBufferSize, DEFAULT_ALIGNMENT));
    // INVARIANT(bufferPtr, "Unpooled memory allocation for buffer failed");
    // auto dataSegment = detail::DataSegment{detail::InMemoryLocation{bufferPtr, true}, bufferSize};
    // auto* controlBlock = new detail::BufferControlBlock{dataSegment, shared_from_this()};
    // INVARIANT(controlBlock, "Unpooled memory allocation for buffer control block failed");
    //
    // NES_TRACE(
    //     "BCB Ptr: {}, Buffer Ptr: {} alignedBufferSize: {}",
    //     reinterpret_cast<uintptr_t>(controlBlock),
    //     reinterpret_cast<uintptr_t>(bufferPtr),
    //     alignedBufferSize);
    // auto pin = controlBlock->getCounter<true>();
    // std::unique_lock allBuffersLock{allBuffersMutex};
    // allBuffers.push_back(controlBlock);
    // TupleBuffer pinnedBuffer(controlBlock, dataSegment, detail::ChildOrMainDataKey::MAIN());
    //
    // return pinnedBuffer;
}

void BufferManager::recycleSegment(detail::DataSegment<detail::InMemoryLocation>&& segment)
{
    if (segment.isNotPreAllocated())
    {
        unpooledChunksManager.recycle(std::move(segment));
    }
    else
    {
        availableBuffers.write(std::move(segment));
    }
}


bool BufferManager::recycleSegment(detail::DataSegment<detail::OnDiskLocation>&& segment)
{
    auto punchHoleFuture = punchHoleSegment(std::move(segment));
    //Ensure request was submitted into queue.
    auto submitted = punchHoleFuture.waitUntilYield();
    if (!submitted)
    {
        NES_WARNING("Failed to submit segment deletion task, segment will only be removed from disk on system shutdown.");
        return false;
    }
    return true;
}

PunchHoleFuture BufferManager::punchHoleSegment(detail::DataSegment<detail::OnDiskLocation>&& segment) noexcept
{
    using Promise = PunchHolePromise<PunchHoleFuture>;

    if (isDestroyed.load())
    {
        co_yield true;
        co_return static_cast<uint32_t>(ErrorCode::BufferManagerAlreadyShutdown);
    }
    //Process previously started hole punching coroutines
    //Fixate number of entries to process
    size_t holesInProgressSize = holesInProgress.size();
    //Before saving a reference to the coroutine frame in holesInProgress, try to process holesInProgress
    if (holesInProgressSize >= holesInProgress.capacity() / 2)
    {
        std::unique_lock holeLock{holeMutex, std::defer_lock};
        //Only reason this would fail, is that another coroutine is already processing holesInProgress
        if (holeLock.try_lock())
        {
            processPunchHoleCompletionEntriesOnce();
            for (size_t i = 0; i < holesInProgressSize; ++i)
            {
                PunchHoleFuture future{};
                if (holesInProgress.read(future))
                {
                    if (auto result = future.getResult())
                    {
                        if (auto uringSuccessOrError = NES::getOptional<UringSuccessOrError>(*result))
                        {
                            INVARIANT(
                                uringSuccessOrError->isSuccess() || uringSuccessOrError->getUringError(),
                                "UringSuccessOrError was neither success or error");
                            if (uringSuccessOrError->isSuccess())
                            {
                                holePunchedSegments.insert(future.getTarget());
                            }
                            else if (auto uringError = uringSuccessOrError->getUringError())
                            {
                                NES_WARNING(
                                    "Failed to delete on disk segment in file {}, offset {} with error message \"{}\"",
                                    future.getTarget().getLocation().getFileID(),
                                    future.getTarget().getLocation().getOffset(),
                                    std::strerror(*uringError));
                                failedToHolePunch.insert(future.getTarget());
                            }
                        }
                        else if (auto internalError = NES::getOptional<detail::CoroutineError>(*result))
                        {
                            NES_WARNING(
                                "Failed to delete on disk segment in file {}, offset {} with error message \"{}\"",
                                future.getTarget().getLocation().getFileID(),
                                future.getTarget().getLocation().getOffset(),
                                typeFromCode(*internalError).getErrorMessage());
                            failedToHolePunch.insert(future.getTarget());
                        }
                    }
                    else
                    {
                        future.pollOnce();
                        //Write unfinished ones back into queue
                        if (!holesInProgress.write(future))
                        {
                            NES_ERROR(
                                "Failed to reinsert unfinished hole punching future into fix sized MPMC queue. "
                                "Segment for file {}, offset {} will be cleaned up on system shutdown. "
                                "You should increase holesInProgress queues capacity.",
                                future.getTarget().getLocation().getFileID(),
                                future.getTarget().getLocation().getOffset());
                        }
                    }
                }
            }
        }
    }


    {
        //Safes the future, thus a reference to the coroutine frame in an extra queue, so that the recycle method can return and does not need
        //to store the handle somewhere.
        //This essentially transfers ownership from the caller to the queue, so that the cleanup step can process it.
        auto handle = co_await detail::GetCoroutineHandle<Promise>{};
        auto future = PunchHoleFuture{handle};
        bool ownershipTransferrred = holesInProgress.write(future);
        co_yield ownershipTransferrred;
        if (!ownershipTransferrred)
        {
            co_return static_cast<detail::CoroutineError>(ErrorCode::FailedToTransferCleanupOwnership);
        }
    }

    auto underlyingSubmissionAwaiter = detail::SubmitPunchHoleSegmentAwaiter<Promise>::create(waitingPunchHoleRequests, segment);
    if (!underlyingSubmissionAwaiter)
    {
        co_return static_cast<detail::CoroutineError>(ErrorCode::CannotSubmitBufferIO);
    }

    auto submissionAwaiter = detail::AwaitExternalProgress<
        Promise,
        std::shared_ptr<detail::PunchHoleSegmentAwaiter<Promise>>,
        detail::SubmitPunchHoleSegmentAwaiter<Promise>>::
        create(
            std::bind(&BufferManager::pollPunchHoleSubmissionEntriesOnce, this),
            std::bind(&BufferManager::waitForPunchHoleSubmissionEntriesOnce, this),
            std::weak_ptr{*underlyingSubmissionAwaiter});
    auto underlyingDeletion = co_await *submissionAwaiter;

    auto deletionAwaiter = detail::AwaitExternalProgress<Promise, std::optional<ssize_t>, detail::PunchHoleSegmentAwaiter<Promise>>::create(
        std::bind(&BufferManager::pollPunchHoleCompletionEntriesOnce, this),
        std::bind(&BufferManager::waitForPunchHoleCompletionEntriesOnce, this),
        std::weak_ptr{underlyingDeletion});

    auto returnCode = co_await *underlyingDeletion;
    if (!returnCode)
    {
        co_return static_cast<detail::CoroutineError>(ErrorCode::CoroutineContinuedWithoutResult);
    }

    co_return UringSuccessOrError{static_cast<uint32_t>(-*returnCode)};
}

void BufferManager::pollPunchHoleSubmissionEntriesOnce() noexcept
{
    std::unique_lock sqeLock{punchHoleSqeMutex, std::defer_lock};
    if (sqeLock.try_lock())
    {
        processPunchHoleSubmissionEntries();
    }
}
void BufferManager::waitForPunchHoleSubmissionEntriesOnce() noexcept
{
    std::unique_lock sqeLock{punchHoleSqeMutex};
    processPunchHoleSubmissionEntries();
}
size_t BufferManager::processPunchHoleSubmissionEntries() noexcept
{
    using Promise = PunchHolePromise<PunchHoleFuture>;
    std::shared_ptr<detail::SubmitPunchHoleSegmentAwaiter<Promise>> nextRequest{};
    //Process a fixed amount of requests, if another request comes it while flushing its fine, its coroutine can flush again itself
    auto numRequests = waitingPunchHoleRequests.size();
    auto awaitersToResume = std::vector<decltype(nextRequest)>{};
    awaitersToResume.reserve(numRequests);
    while (numRequests-- > 0 && waitingPunchHoleRequests.read(nextRequest))
    {
        io_uring_sqe* sqe = io_uring_get_sqe(&uringPunchHoleRing);
        if (sqe == nullptr)
        {
            return 0;
        }
        //Allocate the awaiter for the next step on the heap, because it might be fulfilled before we resume the coroutine
        //Now, If another thread processed the completion event before we resume and return the new awaiter,
        //the awaiter is just prefilled and doesn't suspend
        //TODO track amount of in-flight requests and block if overcrowded
        auto punchHoleAwaiter = std::make_shared<detail::PunchHoleSegmentAwaiter<Promise>>();
        //We can only attach pointers to iouring requests
        auto* punchHoleAwaiterW = new std::weak_ptr{punchHoleAwaiter};
        //The ownership of the request goes back to the coroutine
        nextRequest->setResult(std::move(punchHoleAwaiter));
        awaitersToResume.push_back(nextRequest);
        io_uring_prep_fallocate(
            sqe,
            files.find(nextRequest->getTarget().getLocation().getFileID())->second.getFd(),
            FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            nextRequest->getTarget().getLocation().getOffset(),
            nextRequest->getTarget().getSize());
        io_uring_sqe_set_data(sqe, punchHoleAwaiterW);
    }
    int processed = io_uring_submit(&uringPunchHoleRing);
    for (auto toResume : awaitersToResume)
    {
        toResume->setResultAndContinue();
    }
    return processed;
}
void BufferManager::pollPunchHoleCompletionEntriesOnce() noexcept
{
    std::unique_lock cqeLock{punchHoleCqeMutex, std::defer_lock};
    if (cqeLock.try_lock())
    {
        processPunchHoleCompletionEntriesOnce();
    }
}
void BufferManager::waitForPunchHoleCompletionEntriesOnce() noexcept
{
    std::unique_lock cqeLock{punchHoleCqeMutex};
    processPunchHoleCompletionEntriesOnce();
}
size_t BufferManager::processPunchHoleCompletionEntriesOnce() noexcept
{
    using Promise = PunchHolePromise<PunchHoleFuture>;
    size_t counter = 0;
    io_uring_cqe* completion = nullptr;
    while (io_uring_peek_cqe(&uringPunchHoleRing, &completion) == 0)
    {
        auto* awaiterW = reinterpret_cast<std::weak_ptr<detail::PunchHoleSegmentAwaiter<Promise>>*>(completion->user_data);
        //We do error handling in the coroutine/the caller of the coroutine.
        //We can't just skip not fully read segments like in the write cqe processing, because there is a 1:1 mapping of coroutines and entries
        if (auto awaiter = awaiterW->lock())
        {
            awaiter->setResultAndContinue(completion->res);
        }
        delete awaiterW;
        io_uring_cqe_seen(&uringPunchHoleRing, completion);
        ++counter;
    }
    return counter;
}


size_t BufferManager::getBufferSize() const
{
    return bufferSize;
}

size_t BufferManager::getNumOfPooledBuffers() const
{
    return numOfBuffers;
}

size_t BufferManager::getNumOfUnpooledBuffers() const
{
    return unpooledChunksManager.getNumberOfUnpooledBuffers();
}

size_t BufferManager::getAvailableBuffers() const
{
    return availableBuffers.size();
}

size_t BufferManager::getAvailableBuffersInFixedSizePools() const
{
    const std::unique_lock lock(localBufferPoolsMutex);
    const auto numberOfAvailableBuffers = std::ranges::count_if(localBufferPools, [](auto& weak) { return !weak.expired(); });
    return numberOfAvailableBuffers;
}

BufferManagerType BufferManager::getBufferManagerType() const
{
    return BufferManagerType::GLOBAL;
}

std::optional<std::shared_ptr<AbstractBufferProvider>> BufferManager::createFixedSizeBufferPool(size_t numberOfReservedBuffers)
{
    std::unique_lock lock(availableBuffersMutex);
    std::deque<detail::DataSegment<detail::InMemoryLocation>> buffers;

    const ssize_t numberOfAvailableBuffers = availableBuffers.size();
    if (numberOfAvailableBuffers < 0 || static_cast<size_t>(numberOfAvailableBuffers) < numberOfReservedBuffers)
    {
        NES_WARNING("Could not allocate FixedSizeBufferPool with {} buffers", numberOfReservedBuffers);
        return {};
    }
    for (std::size_t i = 0; i < numberOfReservedBuffers; ++i)
    {
        detail::DataSegment<detail::InMemoryLocation> memorySegment = detail::DataSegment<detail::InMemoryLocation>{};
        availableBuffers.blockingRead(memorySegment);
        buffers.emplace_back(memorySegment);
    }

    auto ret = std::make_shared<FixedSizeBufferPool>(
        shared_from_this(),
        std::move(buffers),
        numberOfReservedBuffers,
        [&](detail::DataSegment<detail::InMemoryLocation>&& segment)
        { memoryResource->deallocate(segment.getLocation().getPtr(), segment.getSize()); });
    {
        std::unique_lock lock(localBufferPoolsMutex);
        localBufferPools.push_back(ret);
    }
    return ret;
}

}
