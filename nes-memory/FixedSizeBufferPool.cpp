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

#include <FixedSizeBufferPool.hpp>

#include <chrono>
#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

namespace NES::Memory
{

FixedSizeBufferPool::FixedSizeBufferPool(
    const std::shared_ptr<BufferManager>& bufferManager, std::deque<detail::MemorySegment*>& buffers, const size_t numberOfReservedBuffers)
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
        INVARIANT(memSegment->isAvailable(), "Buffer not available");
        INVARIANT(
            memSegment->controlBlock->owningBufferRecycler == nullptr,
            "Buffer should not retain a reference to its parent while not in use.");

        exclusiveBuffers.write(memSegment);
    }
}

FixedSizeBufferPool::~FixedSizeBufferPool()
{
    FixedSizeBufferPool::destroy();
}

BufferManagerType FixedSizeBufferPool::getBufferManagerType() const
{
    return BufferManagerType::FIXED;
}

void FixedSizeBufferPool::destroy()
{
    if (isDestroyed.exchange(true))
    {
        /// already destroyed
        return;
    }

    detail::MemorySegment* memSegment = nullptr;
    /// Recycle all resident buffers immediatly.
    /// In-flight buffers will keep the FixedSizeBufferPool alive and are returned to the global buffer provider
    /// once they are no longer in use. Once all in-flight buffers are destroyed the FixedSizeBufferPool is truly destroyed.
    while (exclusiveBuffers.read(memSegment))
    {
        INVARIANT(
            memSegment->controlBlock->owningBufferRecycler == nullptr,
            "Buffer should not retain a reference to its parent while not in use");
        bufferManager->recyclePooledBuffer(memSegment);
    }
}

size_t FixedSizeBufferPool::getAvailableBuffers() const
{
    const auto qSize = exclusiveBuffers.size();
    return qSize > 0 ? qSize : 0;
}

std::optional<TupleBuffer> FixedSizeBufferPool::getBufferWithTimeout(const std::chrono::milliseconds timeout)
{
    const auto now = std::chrono::steady_clock::now();
    detail::MemorySegment* memSegment = nullptr;
    if (exclusiveBuffers.tryReadUntil(now + timeout, memSegment))
    {
        if (memSegment->controlBlock->prepare(shared_from_this()))
        {
            return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
        }
        throw InvalidRefCountForBuffer();
    }
    return std::nullopt;
}

TupleBuffer FixedSizeBufferPool::getBufferBlocking()
{
    detail::MemorySegment* memSegment = nullptr;
    exclusiveBuffers.blockingRead(memSegment);
    if (memSegment->controlBlock->prepare(shared_from_this()))
    {
        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
    }
    throw InvalidRefCountForBuffer();
}

void FixedSizeBufferPool::recyclePooledBuffer(detail::MemorySegment* memSegment)
{
    INVARIANT(memSegment, "null memory segment");
    if (isDestroyed)
    {
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
    throw UnknownOperation("This function is not supported here");
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
    throw UnknownOperation("This function is not supported here");
}
size_t FixedSizeBufferPool::getTotalSizeOfUnpooledBufferChunks() const
{
    return bufferManager->getTotalSizeOfUnpooledBufferChunks();
}
std::optional<TupleBuffer> FixedSizeBufferPool::getBufferNoBlocking()
{
    throw UnknownOperation("This function is not supported here");
}
std::optional<TupleBuffer> FixedSizeBufferPool::getUnpooledBuffer(const size_t bufferSize)
{
    return bufferManager->getUnpooledBuffer(bufferSize);
}
}
