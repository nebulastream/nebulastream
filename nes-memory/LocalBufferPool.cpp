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
#include <chrono>
#include <cstddef>
#include <deque>
#include <memory>
#include <Runtime/BufferManager.hpp>
#include <Runtime/PinnedBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <LocalBufferPool.hpp>
#include <TupleBufferImpl.hpp>

namespace NES::Memory
{
LocalBufferPool::LocalBufferPool(
    const std::shared_ptr<BufferManager>& bufferManager,
    std::deque<detail::DataSegment<detail::InMemoryLocation>>& buffers,
    const size_t numberOfReservedBuffers,
    const std::function<void(detail::DataSegment<detail::InMemoryLocation>&&)>& deallocator)
    : bufferManager(bufferManager)
    , deallocator(deallocator)
    , exclusiveBuffers(numberOfReservedBuffers)
    , exclusiveBufferCount(numberOfReservedBuffers)
    , numberOfReservedBuffers(numberOfReservedBuffers)
{
    PRECONDITION(this->bufferManager, "Invalid buffer manager");
    while (!buffers.empty())
    {
        auto memSegment = buffers.front();
        buffers.pop_front();
        INVARIANT(memSegment.getLocation().getPtr(), "null memory segment");
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

    INVARIANT(
        numberOfReservedBuffers == exclusiveBufferCount,
        "one or more buffers were not returned to the pool: {} but expected {}",
        exclusiveBufferCount,
        numberOfReservedBuffers);

    NES_DEBUG("buffers before={} size of local buffers={}", bufferManager->getAvailableBuffers(), exclusiveBuffers.size());

    detail::DataSegment<detail::InMemoryLocation> memSegment = detail::DataSegment{detail::InMemoryLocation{nullptr}, 0};
    while (exclusiveBuffers.read(memSegment))
    {
        /// return exclusive buffers to the global pool
        // memSegment->controlBlock->resetBufferRecycler(bufferManager.get());
        bufferManager->recycleSegment(std::move(memSegment));
    }
    bufferManager.reset();
}

size_t LocalBufferPool::getAvailableBuffers() const
{
    const auto qSize = exclusiveBuffers.size();
    return qSize > 0 ? qSize : 0;
}

PinnedBuffer LocalBufferPool::getBufferBlocking()
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

void LocalBufferPool::recycleSegment(detail::DataSegment<detail::InMemoryLocation>&& memSegment)
{
    INVARIANT(memSegment.getLocation().getPtr(), "null memory segment");
    // if (!memSegment->isAvailable())
    // {
    //     NES_THROW_RUNTIME_ERROR(
    //         "Recycling buffer callback invoked on used memory segment refcnt=" << memSegment->controlBlock->getReferenceCount());
    // }
    if (memSegment.isNotPreAllocated())
    {
        deallocator(memSegment);
    }
    else
    {
        exclusiveBuffers.write(memSegment);
        exclusiveBufferCount.fetch_add(1);
    }
}


bool LocalBufferPool::recycleSegment(detail::DataSegment<detail::OnDiskLocation>&&)
{
    throw UnsupportedOperation("Recycling pooled on disk segments is not supported");
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

std::optional<PinnedBuffer> LocalBufferPool::getBufferNoBlocking()
{
    detail::DataSegment<detail::InMemoryLocation> inMemorySegment = detail::DataSegment{detail::InMemoryLocation{nullptr}, 0};
    if (exclusiveBuffers.read(inMemorySegment))
    {
        const auto controlBlock = new detail::BufferControlBlock{inMemorySegment, this};
        const std::unique_lock lock{allBuffersMutex};
        allBuffers.push_back(controlBlock);
        return PinnedBuffer(controlBlock, inMemorySegment, detail::ChildOrMainDataKey::MAIN());
    }
    return std::nullopt;
}
std::optional<PinnedBuffer> LocalBufferPool::getBufferWithTimeout(std::chrono::milliseconds timeout)
{
    const auto now = std::chrono::steady_clock::now();
    detail::DataSegment<detail::InMemoryLocation> inMemorySegment = detail::DataSegment{detail::InMemoryLocation{nullptr}, 0};
    if (exclusiveBuffers.tryReadUntil(now + timeout, inMemorySegment))
    {
        //TODO use buffer count correctly
        exclusiveBufferCount.fetch_sub(1);
        auto* const controlBlock = new detail::BufferControlBlock{inMemorySegment, this};
        const std::unique_lock lock{allBuffersMutex};
        allBuffers.push_back(controlBlock);
        return PinnedBuffer(controlBlock, inMemorySegment, detail::ChildOrMainDataKey::MAIN());
    }
    return std::nullopt;
}

std::optional<PinnedBuffer> LocalBufferPool::getUnpooledBuffer(const size_t size)
{
    return bufferManager->getUnpooledBuffer(size);
}
}
