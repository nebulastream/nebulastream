/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Runtime/NodeEngine.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/detail/TupleBufferImpl.hpp>

namespace NES::Runtime {

FixedSizeBufferPool::FixedSizeBufferPool(const BufferManagerPtr& bufferManager,
                                         std::deque<detail::MemorySegment*>&& buffers,
                                         size_t numberOfReservedBuffers)
    : bufferManager(bufferManager),
#ifdef NES_USE_LATCH_FREE_BUFFER_MANAGER
      exclusiveBuffers(numberOfReservedBuffers),
#endif
      numberOfReservedBuffers(numberOfReservedBuffers), isDestroyed(false) {

    while (!buffers.empty()) {
        auto* memSegment = buffers.front();
        buffers.pop_front();
        NES_VERIFY(memSegment, "null memory segment");
        memSegment->controlBlock->resetBufferRecycler(this);
        NES_ASSERT2_FMT(memSegment->isAvailable(), "Buffer not available");
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
        exclusiveBuffers.emplace_back(memSegment);
#else
        exclusiveBuffers.write(memSegment);
#endif
    }
}

FixedSizeBufferPool::~FixedSizeBufferPool() {
    // nop
}

BufferManagerType FixedSizeBufferPool::getBufferManagerType() const
{
    return BufferManagerType::FIXED;
}

void FixedSizeBufferPool::destroy() {
    NES_DEBUG("Destroying LocalBufferPool");
    std::unique_lock lock(mutex);
    if (isDestroyed) {
        return;
    }
    auto ownedBufferManager = bufferManager.lock();
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    while (!exclusiveBuffers.empty()) {
        // return exclusive buffers to the global pool
        auto* memSegment = exclusiveBuffers.front();
        exclusiveBuffers.pop_front();
        memSegment->controlBlock->resetBufferRecycler(ownedBufferManager.get());
        ownedBufferManager->recyclePooledBuffer(memSegment);
    }
    NES_DEBUG("buffers after=" << ownedBufferManager->getAvailableBuffers()
                               << " size of local buffers=" << exclusiveBuffers.size());
#else
    detail::MemorySegment* memSegment = nullptr;
    while (exclusiveBuffers.read(memSegment)) {
        // return exclusive buffers to the global pool
        memSegment->controlBlock->resetBufferRecycler(ownedBufferManager.get());
        ownedBufferManager->recyclePooledBuffer(memSegment);
    }
#endif
    isDestroyed = true;
}

size_t FixedSizeBufferPool::getAvailableBuffers() const {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    std::unique_lock lock(mutex);
    return exclusiveBuffers.size();
#else
    auto qSize = exclusiveBuffers.size();
    return qSize > 0 ? qSize : 0;
#endif
}

std::optional<TupleBuffer> FixedSizeBufferPool::getBufferTimeout(std::chrono::seconds timeout) {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    std::unique_lock lock(mutex);
    auto pred = [this]() {
        return !exclusiveBuffers.empty();// false if waiting must be continued
    };
    if (!cvar.wait_for(lock, timeout, std::move(pred))) {
        return std::nullopt;
    }
    detail::MemorySegment* memSegment = exclusiveBuffers.front();
    exclusiveBuffers.pop_front();
    lock.unlock();
    if (memSegment->controlBlock->prepare()) {
        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
    }
    NES_THROW_RUNTIME_ERROR(
        "[FixedSizeBufferPool] got buffer with invalid reference counter: " << memSegment->controlBlock->getReferenceCount());
#else
    auto now = std::chrono::steady_clock::now();
    detail::MemorySegment* memSegment;
    if (exclusiveBuffers.tryReadUntil(now + timeout, memSegment)) {
        if (memSegment->controlBlock->prepare()) {
            return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
        }
        NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter "
                                << memSegment->controlBlock->getReferenceCount());
    }
    return std::nullopt;
#endif
}

TupleBuffer FixedSizeBufferPool::getBufferBlocking() {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    // try to get an exclusive buffer
    std::unique_lock lock(mutex);
    while (exclusiveBuffers.empty()) {
        cvar.wait(lock);
    }
    auto* memSegment = exclusiveBuffers.front();
    NES_VERIFY(memSegment, "null memory segment");
    exclusiveBuffers.pop_front();
    if (memSegment->controlBlock->prepare()) {
        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
    }
    NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter "
                            << memSegment->controlBlock->getReferenceCount());
#else
    detail::MemorySegment* memSegment;
    exclusiveBuffers.blockingRead(memSegment);
    if (memSegment->controlBlock->prepare()) {
        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
    }
    NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter "
                            << memSegment->controlBlock->getReferenceCount());
#endif
}

void FixedSizeBufferPool::recyclePooledBuffer(detail::MemorySegment* memSegment) {
    std::unique_lock lock(mutex);
    NES_VERIFY(memSegment, "null memory segment");
    if (isDestroyed) {
        // return recycled buffer to the global pool
        auto ownedBufferManager = bufferManager.lock();
        memSegment->controlBlock->resetBufferRecycler(ownedBufferManager.get());
        ownedBufferManager->recyclePooledBuffer(memSegment);
    } else {
        if (!memSegment->isAvailable()) {
            NES_THROW_RUNTIME_ERROR("Recycling buffer callback invoked on used memory segment refcnt="
                                    << memSegment->controlBlock->getReferenceCount());
        }
        // add back an exclusive buffer to the local pool
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
        exclusiveBuffers.emplace_back(memSegment);
        cvar.notify_all();
#else
        exclusiveBuffers.write(memSegment);
#endif
    }
}

void FixedSizeBufferPool::recycleUnpooledBuffer(detail::MemorySegment*) {
    NES_THROW_RUNTIME_ERROR("This feature is not supported here");
}
}// namespace NES::Runtime
