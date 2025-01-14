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

#include "LocalBufferPool.hpp"
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include "TupleBufferImpl.hpp"

namespace NES::Memory
{
LocalBufferPool::LocalBufferPool(
    const BufferManagerPtr& bufferManager, std::deque<detail::MemorySegment*>&& buffers, size_t numberOfReservedBuffers)
    : bufferManager(bufferManager)
    , exclusiveBuffers(numberOfReservedBuffers)
    , exclusiveBufferCount(numberOfReservedBuffers)
    , numberOfReservedBuffers(numberOfReservedBuffers)
{
    NES_ASSERT2_FMT(this->bufferManager, "Invalid buffer manager");
    while (!buffers.empty())
    {
        auto* memSegment = buffers.front();
        buffers.pop_front();
        NES_VERIFY(memSegment, "null memory segment");
        memSegment->controlBlock->resetBufferRecycler(this);
        NES_ASSERT2_FMT(memSegment->isAvailable(), "Buffer not available");

        exclusiveBuffers.write(memSegment);
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
        allSegments.emplace_back(memSegment);
#endif
    }
}

LocalBufferPool::~LocalBufferPool()
{
    /// nop
}

BufferManagerType LocalBufferPool::getBufferManagerType() const
{
    return BufferManagerType::LOCAL;
}

void LocalBufferPool::destroy()
{
    NES_DEBUG("Destroying LocalBufferPool");
    std::unique_lock lock(mutex);
    if (bufferManager == nullptr)
    {
        return; /// already destroyed
    }
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    if (numberOfReservedBuffers != (size_t)exclusiveBuffers.size())
    {
        for (auto segment : allSegments)
        {
            segment->controlBlock->dumpOwningThreadInfo();
        }
    }
#endif
    size_t exclusiveBufferCount = this->exclusiveBufferCount.load();

    NES_ASSERT2_FMT(
        numberOfReservedBuffers == exclusiveBufferCount,
        "one or more buffers were not returned to the pool: " << exclusiveBufferCount << " but expected " << numberOfReservedBuffers);

    NES_DEBUG("buffers before={} size of local buffers={}", bufferManager->getAvailableBuffers(), exclusiveBuffers.size());

    detail::MemorySegment* memSegment = nullptr;
    while (exclusiveBuffers.read(memSegment))
    {
        /// return exclusive buffers to the global pool
        memSegment->controlBlock->resetBufferRecycler(bufferManager.get());
        bufferManager->recyclePooledBuffer(memSegment);
    }
    bufferManager.reset();
}

size_t LocalBufferPool::getAvailableBuffers() const
{
    const auto qSize = exclusiveBuffers.size();
    return qSize > 0 ? qSize : 0;
}

TupleBuffer LocalBufferPool::getBufferBlocking()
{
    detail::MemorySegment* memSegment;
    auto buffer = getBufferWithTimeout(GET_BUFFER_TIMEOUT);
    if (buffer.has_value())
    {
        NES_TRACE("get buffer from local pool");
        return buffer.value();
    }
    else
    {
        /// if no buffer remains in the local pool, draw from the global pool
        NES_TRACE("get buffer from global pool");
        return bufferManager->getBufferBlocking();
    }
}

void LocalBufferPool::recyclePooledBuffer(detail::MemorySegment* memSegment)
{
    NES_VERIFY(memSegment, "null memory segment");
    if (!memSegment->isAvailable())
    {
        NES_THROW_RUNTIME_ERROR(
            "Recycling buffer callback invoked on used memory segment refcnt=" << memSegment->controlBlock->getReferenceCount());
    }
    exclusiveBuffers.write(memSegment);
    exclusiveBufferCount.fetch_add(1);
}

void LocalBufferPool::recycleUnpooledBuffer(detail::MemorySegment*)
{
    NES_THROW_RUNTIME_ERROR("This feature is not supported here");
}
size_t LocalBufferPool::getBufferSize() const
{
    return bufferManager->getBufferSize();
}

size_t LocalBufferPool::getNumOfPooledBuffers() const
{
    return numberOfReservedBuffers;
}

size_t LocalBufferPool::getNumOfUnpooledBuffers() const
{
    return bufferManager->getNumOfUnpooledBuffers();
}

std::optional<TupleBuffer> LocalBufferPool::getBufferNoBlocking()
{
    NES_ASSERT2_FMT(false, "This is not supported currently");
}
std::optional<TupleBuffer> LocalBufferPool::getBufferWithTimeout(std::chrono::milliseconds timeout)
{
    const auto now = std::chrono::steady_clock::now();
    detail::MemorySegment* memSegment;
    if (exclusiveBuffers.tryReadUntil(now + timeout, memSegment))
    {
        if (memSegment->controlBlock->prepare())
        {
            exclusiveBufferCount.fetch_sub(1);
            return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
        }
        throw InvalidRefCountForBuffer();
    }
    return std::nullopt;
}

std::optional<TupleBuffer> LocalBufferPool::getUnpooledBuffer(size_t size)
{
    return bufferManager->getUnpooledBuffer(size);
}
}
