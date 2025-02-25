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

#include "FixedSizeBufferPool.hpp"
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include "TupleBufferImpl.hpp"

namespace NES::Memory
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
        INVARIANT(memSegment, "null memory segment");
        memSegment->controlBlock->resetBufferRecycler(this);
        INVARIANT(memSegment->isAvailable(), "Buffer not available");

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

std::optional<TupleBuffer> FixedSizeBufferPool::getBufferWithTimeout(const std::chrono::milliseconds timeout)
{
    auto now = std::chrono::steady_clock::now();
    detail::MemorySegment* memSegment;
    if (exclusiveBuffers.tryReadUntil(now + timeout, memSegment))
    {
        if (memSegment->controlBlock->prepare())
        {
            return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
        }
        throw InvalidRefCountForBuffer();
    }
    return std::nullopt;
}

TupleBuffer FixedSizeBufferPool::getBufferBlocking()
{
    detail::MemorySegment* memSegment;
    auto buffer = getBufferWithTimeout(GET_BUFFER_TIMEOUT);
    if (buffer.has_value())
    {
        return buffer.value();
    }
    else
    {
        throw BufferAllocationFailure("FixedSizeBufferPool could not allocate buffer before timeout: {}", GET_BUFFER_TIMEOUT);
    }
}

void FixedSizeBufferPool::recyclePooledBuffer(detail::MemorySegment* memSegment)
{
    INVARIANT(memSegment, "null memory segment");
    if (isDestroyed)
    {
        /// return recycled buffer to the global pool
        memSegment->controlBlock->resetBufferRecycler(bufferManager.get());
        bufferManager->recyclePooledBuffer(memSegment);
    }
    else
    {
        INVARIANT(
            memSegment->isAvailable(),
            "Recycling buffer callback invoked on used memory segment refcnt={}",
            memSegment->controlBlock->getReferenceCount());

        /// add back an exclusive buffer to the local pool
        exclusiveBuffers.write(memSegment);
    }
}

void FixedSizeBufferPool::recycleUnpooledBuffer(detail::MemorySegment*)
{
    throw UnsupportedOperation("This function is not supported here");
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
    throw UnsupportedOperation("This function is not supported here");
}
std::optional<TupleBuffer> FixedSizeBufferPool::getBufferNoBlocking()
{
    throw UnsupportedOperation("This function is not supported here");
}
std::optional<TupleBuffer> FixedSizeBufferPool::getUnpooledBuffer(size_t)
{
    throw UnsupportedOperation("This function is not supported here");
}
}
