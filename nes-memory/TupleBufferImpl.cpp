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

#include <cstdint>
#include <utility>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/DataSegment.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    #include <mutex>
    #include <thread>
    #include <cpptrace.hpp>
#endif

namespace NES::Memory
{

namespace detail
{

/// -----------------------------------------------------------------------------
/// ------------------ Core Mechanism for Buffer recycling ----------------------
/// -----------------------------------------------------------------------------


BufferControlBlock::BufferControlBlock(const DataSegment<InMemoryLocation>& inMemorySegment, std::shared_ptr<BufferRecycler> recycler)
    : data(inMemorySegment), owningBufferRecycler(std::move(recycler))
{
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    /// store the current thread that owns the buffer and track which function obtained the buffer
    std::unique_lock lock(owningThreadsMutex);
    ThreadOwnershipInfo info;
    fillThreadOwnershipInfo(info.threadName, info.callstack);
    owningThreads[std::this_thread::get_id()].emplace_back(info);
#endif
}

// BufferControlBlock::BufferControlBlock(const BufferControlBlock& that) : data(that.data.load())
// {
//     pinnedCounter.store(that.pinnedCounter.load());
//     dataCounter.store(that.dataCounter.load());
//     numberOfTuples = that.numberOfTuples;
//     creationTimestamp = that.creationTimestamp;
//     watermark = that.watermark;
//     originId = that.originId;
//
//     std::shared_lock thatChildMutex{that.segmentMutex};
//     children = std::vector{that.children};
// #ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
//     /// store the current thread that owns the buffer and track which function obtained the buffer
//     std::unique_lock lock(owningThreadsMutex);
//     ThreadOwnershipInfo info;
//     fillThreadOwnershipInfo(info.threadName, info.callstack);
//     owningThreads[std::this_thread::get_id()].emplace_back(info);
// #endif
// }
//
// BufferControlBlock& BufferControlBlock::operator=(const BufferControlBlock& that);
// {
//     pinnedCounter.store(that.pinnedCounter.load());
//     numberOfTuples = that.numberOfTuples;
//     data = that.data.load();
//     watermark = that.watermark;
//     creationTimestamp = that.creationTimestamp;
//     originId = that.originId;
//     owningBufferRecycler.store(that.owningBufferRecycler.load());
//
//     std::shared_lock thatChildMutex{that.segmentMutex};
//     children = std::vector{that.children};
//
// #ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
//     /// store the current thread that owns the buffer and track which function obtained the buffer
//     std::unique_lock lock(owningThreadsMutex);
//     ThreadOwnershipInfo info;
//     fillThreadOwnershipInfo(info.threadName, info.callstack);
//     owningThreads[std::this_thread::get_id()].emplace_back(info);
// #endif
//     return *this;
// }

DataSegment<DataLocation> BufferControlBlock::getData() const
{
    return data;
}

void BufferControlBlock::resetBufferRecycler(const std::shared_ptr<BufferRecycler>& recycler)
{
    PRECONDITION(recycler, "invalid recycler");
    const auto oldRecycler = owningBufferRecycler.exchange(recycler);
    INVARIANT(recycler != oldRecycler, "invalid recycler");
}


#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
/**
 * @brief This function collects the thread name and the callstack of the calling thread
 * @param threadName
 * @param callstack
 */
void fillThreadOwnershipInfo(std::string& threadName, cpptrace::raw_trace& callstack)
{
    std::stringbuf threadNameBuffer;
    std::ostream os1(&threadNameBuffer);
    os1 << std::this_thread::get_id();

    threadName = threadNameBuffer.str();
    callstack = cpptrace::raw_trace::current(1);
}
#endif

BufferControlBlock* BufferControlBlock::pinnedRetain()
{
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    /// store the current thread that owns the buffer (shared) and track which function increased the counter of the buffer
    std::unique_lock lock(owningThreadsMutex);
    ThreadOwnershipInfo info;
    fillThreadOwnershipInfo(info.threadName, info.callstack);
    owningThreads[std::this_thread::get_id()].emplace_back(info);
#endif

    //Spin over pinnedCounter, only increase it by one if it is 0 or more, because -1 means the data segment is being modified
    //and might become invalid
    int32_t old = pinnedCounter.load();
    do
    {
    } while (old < 0 || !pinnedCounter.compare_exchange_weak(old, old + 1));
    return this;
}

std::optional<RepinBCBLock> BufferControlBlock::startRepinning() noexcept
{
    //While it would be nice to just use mutexes everywhere, the repinning might be started and progressed on different threads,
    //which makes it impossible to release the mutexes correctly
    auto wasRepinning = isRepinning.test_and_set();
    if (wasRepinning)
    {
        return std::nullopt;
    }
    if (!segmentMutex.try_lock())
    {
        isRepinning.clear();
        return std::nullopt;
    }
    skipSpillingUpTo = ChildOrMainDataKey::MAIN();
    segmentMutex.unlock();
    RepinBCBLock lock{this};
    return std::move(lock);
}


std::optional<UniqueMutexBCBLock> BufferControlBlock::tryLockUnique() const noexcept
{
    std::unique_lock lock{segmentMutex, std::defer_lock};
    if (lock.try_lock() && !isRepinning.test())
    {
        return UniqueMutexBCBLock{std::move(lock), this};
    }
    return std::nullopt;
}

std::optional<SharedMutexBCBLock> BufferControlBlock::tryLockShared() const noexcept
{
    std::shared_lock lock{segmentMutex, std::defer_lock};
    if (lock.try_lock() && !isRepinning.test())
    {
        return SharedMutexBCBLock{std::move(lock), this};
    }
    return std::nullopt;
}

void BufferControlBlock::markRepinningDone() noexcept
{
    isSpilledUpTo = ChildOrMainDataKey::UNKNOWN();
    skipSpillingUpTo = ChildOrMainDataKey::UNKNOWN();
    clockReference.clear();
    isRepinning.clear();
}

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
void BufferControlBlock::dumpOwningThreadInfo()
{
    std::unique_lock lock(owningThreadsMutex);
    NES_FATAL_ERROR("Buffer {} has {} live references", fmt::ptr(getData()), referenceCounter.load());
    for (auto& item : owningThreads)
    {
        for (auto& v : item.second)
        {
            NES_FATAL_ERROR(
                "Thread {} has buffer {} requested on callstack: {}", v.threadName, fmt::ptr(getData()), v.callstack.resolve().to_string());
        }
    }
}
#endif

int32_t BufferControlBlock::getPinnedReferenceCount() const noexcept
{
    return pinnedCounter.load();
}

int32_t BufferControlBlock::getDataReferenceCount() const noexcept
{
    return dataCounter.load();
}

bool BufferControlBlock::pinnedRelease()
{
    if (const uint32_t prevRefCnt = pinnedCounter.fetch_sub(1); prevRefCnt == 1)
    {
        skipSpillingUpTo = ChildOrMainDataKey::UNKNOWN();
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
        {
            std::unique_lock lock(owningThreadsMutex);
            owningThreads.clear();
        }
#endif
        return true;
    }
    else
    {
        INVARIANT(prevRefCnt != 0, "BufferControlBlock: releasing an already released buffer");
    }
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    {
        std::unique_lock lock(owningThreadsMutex);
        auto& v = owningThreads[std::this_thread::get_id()];
        if (!v.empty())
        {
            v.pop_front();
        }
    }
#endif
    return false;
}
BufferControlBlock* BufferControlBlock::dataRetain()
{
    dataCounter++;
    return this;
}
bool BufferControlBlock::dataRelease()
{
    if (const uint32_t prevRefCnt = dataCounter.fetch_sub(1); prevRefCnt == 1)
    {
        std::unique_lock childLock{segmentMutex};
        numberOfTuples = 0;
        const auto bufferRecycler = owningBufferRecycler.load();
        const auto emptySegment = DataSegment<DataLocation>{};
        for (auto& child : children)
        {
            //TODO add flag for unpooled into segment
            bufferRecycler->recycleSegment(std::exchange(child, emptySegment));
        }
        children.clear();
        bufferRecycler->recycleSegment(data.exchange(emptySegment));
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
        {
            std::unique_lock lock(owningThreadsMutex);
            owningThreads.clear();
        }
#endif
        return true;
    }
    else
    {
        INVARIANT(prevRefCnt != 0, "releasing an already released buffer");
    }
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    {
        std::unique_lock lock(owningThreadsMutex);
        auto& v = owningThreads[std::this_thread::get_id()];
        if (!v.empty())
        {
            v.pop_front();
        }
    }
#endif
    return false;
}
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
BufferControlBlock::ThreadOwnershipInfo::ThreadOwnershipInfo(std::string&& threadName, cpptrace::raw_trace&& callstack)
    : threadName(threadName), callstack(callstack)
{
    /// nop
}

BufferControlBlock::ThreadOwnershipInfo::ThreadOwnershipInfo() : threadName("NOT-SAMPLED"), callstack(cpptrace::raw_trace::current(1))
{
    /// nop
}
#endif

template <bool pinned>
RefCountedBCB<pinned> BufferControlBlock::getCounter() noexcept
{
    return RefCountedBCB<pinned>{this};
}

template RefCountedBCB<true> BufferControlBlock::getCounter() noexcept;
template RefCountedBCB<false> BufferControlBlock::getCounter() noexcept;
/// -----------------------------------------------------------------------------
/// ------------------ Utility functions for TupleBuffer ------------------------
/// -----------------------------------------------------------------------------

uint64_t BufferControlBlock::getNumberOfTuples() const noexcept
{
    return numberOfTuples;
}

void BufferControlBlock::setNumberOfTuples(const uint64_t numberOfTuples)
{
    this->numberOfTuples = numberOfTuples;
}

Timestamp BufferControlBlock::getWatermark() const noexcept
{
    return watermark;
}

void BufferControlBlock::setWatermark(const Timestamp watermark)
{
    this->watermark = watermark;
}

SequenceNumber BufferControlBlock::getSequenceNumber() const noexcept
{
    return sequenceNumber;
}

void BufferControlBlock::setSequenceNumber(const SequenceNumber sequenceNumber)
{
    this->sequenceNumber = sequenceNumber;
}

ChunkNumber BufferControlBlock::getChunkNumber() const noexcept
{
    return chunkNumber;
}

void BufferControlBlock::setChunkNumber(const ChunkNumber chunkNumber)
{
    this->chunkNumber = chunkNumber;
}

bool BufferControlBlock::isLastChunk() const noexcept
{
    return lastChunk;
}

void BufferControlBlock::setLastChunk(const bool lastChunk)
{
    this->lastChunk = lastChunk;
}

void BufferControlBlock::setCreationTimestamp(const Timestamp timestamp)
{
    this->creationTimestamp = timestamp;
}

Timestamp BufferControlBlock::getCreationTimestamp() const noexcept
{
    return creationTimestamp;
}

OriginId BufferControlBlock::getOriginId() const noexcept
{
    return originId;
}

void BufferControlBlock::setOriginId(const OriginId originId)
{
    this->originId = originId;
}


bool BufferControlBlock::swapSegment(DataSegment<DataLocation>& segment, ChildOrMainDataKey key) noexcept
{
    if (key == ChildOrMainDataKey::UNKNOWN())
    {
        return false;
    }
    if (key == ChildOrMainDataKey::MAIN())
    {
        return swapDataSegment(segment);
    }
    return swapChild(segment, *key.asChildKey());
}

template <UniqueBCBLock Lock>
bool BufferControlBlock::swapSegment(DataSegment<DataLocation>& segment, ChildOrMainDataKey key, Lock& lock) noexcept
{
    if (key == ChildOrMainDataKey::UNKNOWN())
    {
        return false;
    }
    if (key == ChildOrMainDataKey::MAIN())
    {
        return swapDataSegment(segment, lock);
    }
    return swapChild(segment, *key.asChildKey(), lock);
}


bool BufferControlBlock::swapDataSegment(DataSegment<DataLocation>& segment) noexcept
{
    if (isRepinning.test() && segment.isSpilled())
    {
        return false;
    }
    if (auto lock = tryLockUnique())
    {
        return swapDataSegment(segment, *lock);
    }
    return false;
}

template <UniqueBCBLock Lock>
bool BufferControlBlock::swapDataSegment(DataSegment<DataLocation>& segment, Lock& lock) noexcept
{
    if (isRepinning.test() && segment.isSpilled())
    {
        return false;
    }
    INVARIANT(lock.isOwner(), "Caller didn't acquire the correct mutex for swapping the main segment of the BCB");
    auto zero = 0;
    auto minusOne = -1;
    if (pinnedCounter.compare_exchange_strong(zero, -1))
    {
        data.store(segment);
        //Increase back up again
        INVARIANT(
            pinnedCounter.compare_exchange_strong(minusOne, 0),
            "Concurrent modification of BCBs data segment, expected counter to be -1, was {}",
            minusOne);
        if (segment.isSpilled())
        {
            isSpilledUpTo = ChildOrMainDataKey::MAIN();
        }
        return true;
    }
    return false;
}

bool BufferControlBlock::swapChild(DataSegment<DataLocation>& segment, ChildKey key) noexcept
{
    if (isRepinning.test() && segment.isSpilled())
    {
        return false;
    }
    if (auto lock = tryLockUnique())
    {
        return swapChild(segment, key, *lock);
    }
    return false;
}

template <UniqueBCBLock Lock>
bool BufferControlBlock::swapChild(DataSegment<DataLocation>& segment, ChildKey key, Lock& lock) noexcept
{
    if (isRepinning.test() && segment.isSpilled())
    {
        return false;
    }

    INVARIANT(lock.isOwner(), "Caller didn't acquire the correct mutex for swapping a child segment of the BCB");

    auto zero = 0;
    auto minusOne = -1;
    if (pinnedCounter.compare_exchange_strong(zero, -1))
    {
        children[key] = segment;
        //Increase back up again
        INVARIANT(
            pinnedCounter.compare_exchange_strong(minusOne, 0),
            "Concurrent modification of BCBs data segment, expected counter to be -1, was {}",
            minusOne);
        if (segment.isSpilled())
        {
            isSpilledUpTo = ChildOrMainDataKey{key};
        }
        return true;
    }
    return false;
}
/// -----------------------------------------------------------------------------
/// ------------------ VarLen fields support for TupleBuffer --------------------
/// -----------------------------------------------------------------------------

std::optional<DataSegment<DataLocation>> BufferControlBlock::stealDataSegment()
{
    if (auto lock = tryLockUnique())
    {
        return stealDataSegment(*lock);
    }
    return std::nullopt;
}

template <UniqueBCBLock Lock>
std::optional<DataSegment<DataLocation>> BufferControlBlock::stealDataSegment(Lock& lock)
{
    INVARIANT(lock.isOwner(), "Caller didn't acquire the correct mutex for stealing the main segment of the BCB");
    DataSegment<DataLocation> dataSegment = data;
    int zero = 0;
    int32_t one = 1;
    if (!pinnedCounter.compare_exchange_strong(zero, -1))
    {
        return std::nullopt;
    }
    if (!dataCounter.compare_exchange_strong(one, 0))
    {
        return std::nullopt;
    }
    data.store(DataSegment<DataLocation>{});
    int minusOne = -1;
    INVARIANT(
        pinnedCounter.compare_exchange_strong(minusOne, 0),
        "Concurrent modification of BCBs data segment, expected counter to be -1, was {}",
        minusOne);
    if (isSpilledUpTo == ChildOrMainDataKey::MAIN())
    {
        isSpilledUpTo = ChildOrMainDataKey{ChildKey{children.size() - 1}};
    }
    if (skipSpillingUpTo == ChildOrMainDataKey::MAIN())
    {
        skipSpillingUpTo = ChildOrMainDataKey{ChildKey{children.size() - 1}};
    }
    return dataSegment;
}

// bool BufferControlBlock::deleteChild(DataSegment<DataLocation>&& child)
// {
//     std::unique_lock writeLock{segmentMutex};
//     //Invalidate the spillingUpTo so that
//     if (std::erase(children, child) != 0)
//     {
//         owningBufferRecycler.load()->recycleSegment(std::move(child));
//         return true;
//     }
//     return false;
// }

std::optional<ChildKey> BufferControlBlock::registerChild(const DataSegment<DataLocation>& child) noexcept
{
    if (auto lock = tryLockUnique())
    {
        return registerChild(child, *lock);
    }
    return std::nullopt;
}


template <UniqueBCBLock Lock>
std::optional<ChildKey> BufferControlBlock::registerChild(const DataSegment<DataLocation>& child, Lock& lock) noexcept
{
    INVARIANT(lock.isOwner(), "Caller didn't acquire the correct mutex for registering a new child in the BCB");
    children.emplace_back(child);
    ChildKey newPosition{children.size() - 1};
    if (isSpilledUpTo == ChildOrMainDataKey::MAIN())
    {
        if (newPosition == 0)
        {
            isSpilledUpTo = ChildOrMainDataKey::UNKNOWN();
        }
        else
        {
            isSpilledUpTo = ChildOrMainDataKey{ChildKey{newPosition - 1}};
        }
    }
    return newPosition;
}


std::optional<ChildKey> BufferControlBlock::findChild(const DataSegment<DataLocation>& child) noexcept
{
    if (auto lock = tryLockShared())
    {
        return findChild(child, *lock);
    }
    return std::nullopt;
}

template <SharedBCBLock Lock>
std::optional<ChildKey> BufferControlBlock::findChild(const DataSegment<DataLocation>& childToFind, Lock& lock) noexcept
{
    INVARIANT(lock.isOwner(), "Caller didn't acquire the correct mutex for finding a child of the BCB");
    for (auto child = children.begin(); child != children.end(); ++child)
    {
        if ((*child) == childToFind)
        {
            return child - children.begin();
        }
    }
    return std::nullopt;
}

uint32_t BufferControlBlock::getNumberOfChildrenBuffers() const noexcept
{
    return children.size();
}

std::optional<DataSegment<DataLocation>> BufferControlBlock::getChild(ChildKey key) const noexcept
{
    if (auto lock = tryLockShared())
    {
        return getChild(key, *lock);
    }
    return std::nullopt;
}

template <SharedBCBLock Lock>
std::optional<DataSegment<DataLocation>> BufferControlBlock::getChild(ChildKey index, Lock& lock) const noexcept
{
    INVARIANT(lock.isOwner(), "Caller didn't acquire the correct mutex for retrieving a child of the BCB");
    if (index < children.size())
    {
        return children[index];
    }
    return std::nullopt;
}

std::optional<DataSegment<DataLocation>> BufferControlBlock::getSegment(ChildOrMainDataKey key) const noexcept
{
    if (key == ChildOrMainDataKey::MAIN())
    {
        return data.load();
    }
    if (key == ChildOrMainDataKey::UNKNOWN())
    {
        return std::nullopt;
    }
    return getChild(*key.asChildKey());
}

template <SharedBCBLock Lock>
std::optional<DataSegment<DataLocation>> BufferControlBlock::getSegment(ChildOrMainDataKey key, Lock& lock) const noexcept
{
    if (key == ChildOrMainDataKey::MAIN())
    {
        return data.load();
    }
    if (key == ChildOrMainDataKey::UNKNOWN())
    {
        return std::nullopt;
    }
    return getChild(*key.asChildKey(), lock);
}

bool BufferControlBlock::unregisterChild(ChildKey child) noexcept
{
    if (auto lock = tryLockUnique())
    {
        return unregisterChild(child, *lock);
    }
    return false;
}

template <UniqueBCBLock Lock>
bool BufferControlBlock::unregisterChild(const ChildKey child, Lock& lock) noexcept
{
    //Assert that there is only a single owner, because if other buffers would have a on this BCB and some childkey,
    //Their childkey would point to the wrong data
    if (dataCounter != 1)
    {
        return false;
    }
    INVARIANT(lock.isOwner(), "Caller didn't acquire the correct mutex for unregistering a child of the BCB");
    children.erase(children.begin() + child);
    if (auto skipToChild = skipSpillingUpTo.load().asChildKey())
    {
        //If everything or nothing was spilled, we don't need to update the counters
        if (skipToChild >= child)
        {
            if (skipToChild == 0)
            {
                //Because we spill first the children and then the main data segment, we go back to unknown (i.e. nothing spilled)
                //If the first child segment was spilled.
                skipSpillingUpTo = ChildOrMainDataKey::UNKNOWN();
            }
            else
            {
                skipSpillingUpTo = ChildOrMainDataKey{ChildKey{*skipToChild - 1}};
            }
        }
    }
    if (auto spilledUpToChild = isSpilledUpTo.load().asChildKey())
    {
        if (spilledUpToChild >= child)
        {
            if (spilledUpToChild == 0)
            {
                isSpilledUpTo = ChildOrMainDataKey::UNKNOWN();
            }
            else
            {
                isSpilledUpTo = ChildOrMainDataKey{ChildKey{*spilledUpToChild - 1}};
            }
        }
    }
    return true;
}


template bool BufferControlBlock::swapSegment(DataSegment<DataLocation>& segment, ChildOrMainDataKey key, UniqueMutexBCBLock&) noexcept;
template bool BufferControlBlock::swapSegment(DataSegment<DataLocation>& segment, ChildOrMainDataKey key, RepinBCBLock&) noexcept;
template bool BufferControlBlock::swapDataSegment(DataSegment<DataLocation>& segment, UniqueMutexBCBLock& lock) noexcept;
template bool BufferControlBlock::swapDataSegment(DataSegment<DataLocation>& segment, RepinBCBLock& lock) noexcept;
template bool BufferControlBlock::swapChild(DataSegment<DataLocation>& segment, ChildKey key, UniqueMutexBCBLock& lock) noexcept;
template bool BufferControlBlock::swapChild(DataSegment<DataLocation>& segment, ChildKey key, RepinBCBLock& lock) noexcept;

template bool BufferControlBlock::unregisterChild(ChildKey child, UniqueMutexBCBLock& lock) noexcept;
template bool BufferControlBlock::unregisterChild(ChildKey child, RepinBCBLock& lock) noexcept;

template std::optional<ChildKey>
BufferControlBlock::registerChild(const DataSegment<DataLocation>& child, UniqueMutexBCBLock& lock) noexcept;
template std::optional<ChildKey> BufferControlBlock::registerChild(const DataSegment<DataLocation>& child, RepinBCBLock& lock) noexcept;

template std::optional<ChildKey> BufferControlBlock::findChild(const DataSegment<DataLocation>& child, UniqueMutexBCBLock& lock) noexcept;
template std::optional<ChildKey> BufferControlBlock::findChild(const DataSegment<DataLocation>& child, RepinBCBLock& lock) noexcept;
template std::optional<ChildKey> BufferControlBlock::findChild(const DataSegment<DataLocation>& child, SharedMutexBCBLock& lock) noexcept;

template std::optional<DataSegment<DataLocation>> BufferControlBlock::stealDataSegment(UniqueMutexBCBLock& lock);
template std::optional<DataSegment<DataLocation>> BufferControlBlock::stealDataSegment(RepinBCBLock& lock);

template std::optional<DataSegment<DataLocation>> BufferControlBlock::getChild(ChildKey key, UniqueMutexBCBLock& lock) const noexcept;
template std::optional<DataSegment<DataLocation>> BufferControlBlock::getChild(ChildKey key, RepinBCBLock& lock) const noexcept;
template std::optional<DataSegment<DataLocation>> BufferControlBlock::getChild(ChildKey key, SharedMutexBCBLock& lock) const noexcept;

template std::optional<DataSegment<DataLocation>>
BufferControlBlock::getSegment(ChildOrMainDataKey key, UniqueMutexBCBLock& lock) const noexcept;
template std::optional<DataSegment<DataLocation>> BufferControlBlock::getSegment(ChildOrMainDataKey key, RepinBCBLock& lock) const noexcept;
template std::optional<DataSegment<DataLocation>>
BufferControlBlock::getSegment(ChildOrMainDataKey key, SharedMutexBCBLock& lock) const noexcept;
}
}
