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
#include <functional>
#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
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

MemorySegment::MemorySegment(const MemorySegment& other) = default;

MemorySegment& MemorySegment::operator=(const MemorySegment& other) = default;

MemorySegment::MemorySegment(
    uint8_t* ptr,
    const uint32_t size,
    std::function<void(MemorySegment*, BufferRecycler*)>&& recycleFunction,
    uint8_t* controlBlock) /// NOLINT (readability-non-const-parameter)
    : ptr(ptr), size(size), controlBlock(new(controlBlock) BufferControlBlock(this, std::move(recycleFunction)))
{
    INVARIANT(this->ptr, "invalid pointer");
    INVARIANT(this->size, "invalid size={}", this->size);
    INVARIANT(this->controlBlock, "invalid control block");
}

MemorySegment::MemorySegment(uint8_t* ptr, const uint32_t size, std::function<void(MemorySegment*, BufferRecycler*)>&& recycleFunction)
    : ptr(ptr), size(size)
{
    INVARIANT(this->ptr, "invalid pointer");
    INVARIANT(this->size, "invalid size={}", this->size);
    controlBlock.reset(new BufferControlBlock(this, std::move(recycleFunction)), magic_enum::enum_integer(MemorySegmentType::Wrapped));
}

MemorySegment::~MemorySegment()
{
    if (ptr)
    {
        /// XXX: If we want to make `release` noexcept as we discussed, we need to make sure that the
        ///      MemorySegment is noexcept destructible. I therefore transformed this error into an assertion
        ///      (I also consider this to be consistent with our handeling of the referenceCount in
        ///      the release function in general. Do you agree?).
        {
            const auto refCnt = controlBlock->getReferenceCount();
            INVARIANT(refCnt == 0, "invalid reference counter {} on mem segment dtor", refCnt);
        }

        /// Release the controlBlock, which is either allocated via 'new' or placement new. In the latter case, we only
        /// have to call the destructor, as the memory segment that contains the controlBlock is managed separately.
        if (controlBlock.tag() == magic_enum::enum_integer(MemorySegmentType::Wrapped))
        {
            delete controlBlock.get();
        }
        else
        {
            controlBlock->~BufferControlBlock();
        }

        std::exchange(controlBlock, nullptr);
        std::exchange(ptr, nullptr);
    }
}

BufferControlBlock::BufferControlBlock(MemorySegment* owner, std::function<void(MemorySegment*, BufferRecycler*)>&& recycleCallback)
    : owner(owner), recycleCallback(std::move(recycleCallback))
{
}

MemorySegment* BufferControlBlock::getOwner() const
{
    return owner;
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
bool BufferControlBlock::prepare(const std::shared_ptr<BufferRecycler>& recycler)
{
    int32_t expected = 0;
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    /// store the current thread that owns the buffer and track which function obtained the buffer
    std::unique_lock lock(owningThreadsMutex);
    ThreadOwnershipInfo info;
    fillThreadOwnershipInfo(info.threadName, info.callstack);
    owningThreads[std::this_thread::get_id()].emplace_back(info);
#endif
    if (referenceCounter.compare_exchange_strong(expected, 1))
    {
        const auto previousOwner = std::exchange(this->owningBufferRecycler, recycler);
        INVARIANT(previousOwner == nullptr, "Buffer should not retain a reference to its owner while unused");
        return true;
    }
    NES_ERROR("Invalid reference counter: {}", expected);
    return false;
}

BufferControlBlock* BufferControlBlock::retain()
{
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    /// store the current thread that owns the buffer (shared) and track which function increased the coutner of the buffer
    std::unique_lock lock(owningThreadsMutex);
    ThreadOwnershipInfo info;
    fillThreadOwnershipInfo(info.threadName, info.callstack);
    owningThreads[std::this_thread::get_id()].emplace_back(info);
#endif
    referenceCounter++;
    return this;
}

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
void BufferControlBlock::dumpOwningThreadInfo()
{
    std::unique_lock lock(owningThreadsMutex);
    NES_FATAL_ERROR("Buffer {} has {} live references", fmt::ptr(getOwner()), referenceCounter.load());
    for (auto& item : owningThreads)
    {
        for (auto& v : item.second)
        {
            NES_FATAL_ERROR(
                "Thread {} has buffer {} requested on callstack: {}",
                v.threadName,
                fmt::ptr(getOwner()),
                v.callstack.resolve().to_string());
        }
    }
}
#endif

int32_t BufferControlBlock::getReferenceCount() const noexcept
{
    return referenceCounter.load();
}

bool BufferControlBlock::release()
{
    if (const uint32_t prevRefCnt = referenceCounter.fetch_sub(1); prevRefCnt == 1)
    {
        numberOfTuples = 0;
        for (auto&& child : children)
        {
            child->controlBlock->release();
        }
        children.clear();
        auto recycler = std::move(owningBufferRecycler);
        recycleCallback(owner, recycler.get());
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

uint64_t BufferControlBlock::getUsedMemorySize() const noexcept
{
    return usedMemorySize;
}

void BufferControlBlock::setUsedMemorySize(const uint64_t usedMemorySize)
{
    this->usedMemorySize = usedMemorySize;
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

/// -----------------------------------------------------------------------------
/// ------------------ VarLen fields support for TupleBuffer --------------------
/// -----------------------------------------------------------------------------

uint32_t BufferControlBlock::storeChildBuffer(BufferControlBlock* control)
{
    control->retain();
    children.emplace_back(control->owner);
    return children.size() - 1;
}

bool BufferControlBlock::loadChildBuffer(uint16_t index, BufferControlBlock*& control, uint8_t*& ptr, uint32_t& size) const
{
    PRECONDITION(index < children.size(), "Index={} is out of range={}", index, children.size());

    auto* child = children[index];
    control = child->controlBlock->retain();
    ptr = child->ptr;
    size = child->size;

    return true;
}
}
}
