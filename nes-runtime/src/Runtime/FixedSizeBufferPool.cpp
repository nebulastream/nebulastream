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
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/detail/TupleBufferImpl.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime
{

FixedSizeBufferPool::FixedSizeBufferPool(
    const BufferManagerPtr& bufferManager, std::deque<detail::MemorySegment*>&& buffers, size_t numberOfReservedBuffers)
    : bufferManager(bufferManager)
    , exclusiveBuffers(numberOfReservedBuffers)
    , numberOfReservedBuffers(numberOfReservedBuffers)
    , isDestroyed(false)
{
    while (!buffers.empty())
    {
        auto* memSegment = buffers.front();
        buffers.pop_front();
        NES_VERIFY(memSegment, "null memory segment");
        memSegment->controlBlock->resetBufferRecycler(this);
        NES_ASSERT2_FMT(memSegment->isAvailable(), "Buffer not available");

        exclusiveBuffers.write(memSegment);
    }
}

FixedSizeBufferPool::~FixedSizeBufferPool()
{
    /// nop
}

BufferManagerType FixedSizeBufferPool::getBufferManagerType() const
{
    return BufferManagerType::FIXED;
}

void FixedSizeBufferPool::destroy()
{
    NES_DEBUG("Destroying LocalBufferPool");
    ///    std::unique_lock lock(mutex);
    bool expected = false;
    if (!isDestroyed.compare_exchange_strong(expected, true))
    {
        return;
    }

    detail::MemorySegment* memSegment = nullptr;
    while (exclusiveBuffers.read(memSegment))
    {
        /// return exclusive buffers to the global pool
        memSegment->controlBlock->resetBufferRecycler(bufferManager.get());
        bufferManager->recyclePooledBuffer(memSegment);
    }
}

size_t FixedSizeBufferPool::getAvailableBuffers() const
{
    auto qSize = exclusiveBuffers.size();
    return qSize > 0 ? qSize : 0;
}

std::optional<TupleBuffer> FixedSizeBufferPool::getBufferTimeout(std::chrono::milliseconds timeout)
{
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    std::unique_lock lock(mutex);
    auto pred = [this]()
    {
        return !exclusiveBuffers.empty(); /// false if waiting must be continued
    };
    if (!cvar.wait_for(lock, timeout, std::move(pred)))
    {
        return std::nullopt;
    }
    detail::MemorySegment* memSegment = exclusiveBuffers.front();
    exclusiveBuffers.pop_front();
    lock.unlock();
    if (memSegment->controlBlock->prepare())
    {
        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
    }
    NES_THROW_RUNTIME_ERROR(
        "[FixedSizeBufferPool] got buffer with invalid reference counter: " << memSegment->controlBlock->getReferenceCount());
#else
    auto now = std::chrono::steady_clock::now();
    detail::MemorySegment* memSegment;
    if (exclusiveBuffers.tryReadUntil(now + timeout, memSegment))
    {
        if (memSegment->controlBlock->prepare())
        {
            return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
        }
        NES_THROW_RUNTIME_ERROR(
            "[BufferManager] got buffer with invalid reference counter " << memSegment->controlBlock->getReferenceCount());
    }
    return std::nullopt;
}

TupleBuffer FixedSizeBufferPool::getBufferBlocking()
{
    detail::MemorySegment* memSegment;
    exclusiveBuffers.blockingRead(memSegment);
    if (memSegment->controlBlock->prepare())
    {
        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
    }
    NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter " << memSegment->controlBlock->getReferenceCount());
#endif
}

void FixedSizeBufferPool::recyclePooledBuffer(detail::MemorySegment* memSegment)
{
    NES_VERIFY(memSegment, "null memory segment");
    if (isDestroyed)
    {
        /// return recycled buffer to the global pool
        memSegment->controlBlock->resetBufferRecycler(bufferManager.get());
        bufferManager->recyclePooledBuffer(memSegment);
    }
    else
    {
        if (!memSegment->isAvailable())
        {
            NES_THROW_RUNTIME_ERROR(
                "Recycling buffer callback invoked on used memory segment refcnt=" << memSegment->controlBlock->getReferenceCount());
        }
        /// add back an exclusive buffer to the local pool
        exclusiveBuffers.write(memSegment);
    }
}

void FixedSizeBufferPool::recycleUnpooledBuffer(detail::MemorySegment*)
{
    NES_THROW_RUNTIME_ERROR("This feature is not supported here");
}
size_t FixedSizeBufferPool::getBufferSize() const
{
    return bufferManager->getBufferSize();
}
size_t FixedSizeBufferPool::getNumOfPooledBuffers() const
{
    return numberOfReservedBuffers;
}
size_t FixedSizeBufferPool::getNumOfUnpooledBuffers() const
{
    NES_ASSERT2_FMT(false, "This is not supported currently");
    return 0;
}
std::optional<TupleBuffer> FixedSizeBufferPool::getBufferNoBlocking()
{
    NES_ASSERT2_FMT(false, "This is not supported currently");
    return std::optional<TupleBuffer>();
}
std::optional<TupleBuffer> FixedSizeBufferPool::getUnpooledBuffer(size_t)
{
    NES_ASSERT2_FMT(false, "This is not supported currently");
    return std::optional<TupleBuffer>();
}
} /// namespace NES::Runtime
