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

#include <cstddef>
#include <deque>
#include <memory>
#include <Runtime/BufferManager.hpp>
#include <Runtime/PinnedBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FixedSizeBufferPool.hpp>
#include <TupleBufferImpl.hpp>

namespace NES::Memory
{

FixedSizeBufferPool::FixedSizeBufferPool(
    const std::shared_ptr<BufferManager>& bufferManager,
    std::deque<detail::DataSegment<detail::InMemoryLocation>>&& buffers,
    const size_t numberOfReservedBuffers,
    const std::function<void(detail::DataSegment<detail::InMemoryLocation>&&)>& deallocator)
    : bufferManager(bufferManager)
    , deallocator(deallocator)
    , exclusiveBuffers(numberOfReservedBuffers)
    , numberOfReservedBuffers(numberOfReservedBuffers)
    , isDestroyed(false)
{
    while (!buffers.empty())
    {
        auto memSegment = buffers.front();
        buffers.pop_front();
        INVARIANT(memSegment.getLocation().getPtr(), "null memory segment");

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

    detail::DataSegment<detail::InMemoryLocation> memSegment = detail::DataSegment{detail::InMemoryLocation{nullptr}, 0};
    while (exclusiveBuffers.read(memSegment))
    {
        /// return exclusive buffers to the global pool
        bufferManager->recycleSegment(std::move(memSegment));
    }
}

size_t FixedSizeBufferPool::getAvailableBuffers() const
{
    const auto qSize = exclusiveBuffers.size();
    return qSize > 0 ? qSize : 0;
}

std::optional<PinnedBuffer> FixedSizeBufferPool::getBufferWithTimeout(const std::chrono::milliseconds timeout)
{
    const auto now = std::chrono::steady_clock::now();
    detail::DataSegment<detail::InMemoryLocation> inMemorySegment = detail::DataSegment{detail::InMemoryLocation{nullptr}, 0};
    if (exclusiveBuffers.tryReadUntil(now + timeout, inMemorySegment))
    {
        const auto controlBlock = new detail::BufferControlBlock{inMemorySegment, this};
        auto pin = controlBlock->getCounter<true>();
        std::unique_lock lock{allBuffersMutex};
        allBuffers.push_back(controlBlock);
        PinnedBuffer pinnedBuffer(controlBlock, inMemorySegment, detail::ChildOrMainDataKey::MAIN());
        return pinnedBuffer;
    }
    return std::nullopt;
}
RepinBufferFuture FixedSizeBufferPool::repinBuffer(FloatingBuffer&&) noexcept
{
    co_return static_cast<CoroutineError>(ErrorCode::NotImplemented);
}
PinnedBuffer FixedSizeBufferPool::getBufferBlocking()
{
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

void FixedSizeBufferPool::recycleSegment(detail::DataSegment<detail::InMemoryLocation>&& memSegment)
{
    INVARIANT(memSegment.getLocation().getPtr(), "null memory segment");
    if (isDestroyed)
    {
        /// return recycled buffer to the global pool
        bufferManager->recycleSegment(std::move(memSegment));
    }
    else
    {
        // if (!memSegment.isAvailable())
        // {
        //     INVARIANT( false
        //         "Recycling buffer callback invoked on used memory segment refcnt=" << memSegment->controlBlock->getReferenceCount());
        // }

        /// add back an exclusive buffer to the local pool
        if (memSegment.isNotPreAllocated())
        {
            deallocator(memSegment);
        }
        else
        {
            exclusiveBuffers.write(memSegment);
        }
    }
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

std::optional<PinnedBuffer> FixedSizeBufferPool::getBufferNoBlocking()
{
    detail::DataSegment<detail::InMemoryLocation> inMemorySegment = detail::DataSegment{detail::InMemoryLocation{nullptr}, 0};
    if (exclusiveBuffers.read(inMemorySegment))
    {
        const auto controlBlock = new detail::BufferControlBlock{inMemorySegment, this};
        auto pin = controlBlock->getCounter<true>();
        std::unique_lock lock{allBuffersMutex};
        allBuffers.push_back(controlBlock);
        auto pinnedBuffer = PinnedBuffer{controlBlock, inMemorySegment, detail::ChildOrMainDataKey::MAIN()};
        return pinnedBuffer;
    }
    return std::nullopt;
}
std::optional<PinnedBuffer> FixedSizeBufferPool::getUnpooledBuffer(size_t)
{
    throw UnsupportedOperation("This function is not supported here");
}

bool FixedSizeBufferPool::recycleSegment(detail::DataSegment<detail::OnDiskLocation>&&)
{
    throw NotImplemented("Recycling on disk data segments is not supported currently");
}
}
