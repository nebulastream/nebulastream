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
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/detail/TupleBufferImpl.hpp>

namespace NES::Runtime {
LocalBufferPool::LocalBufferPool(const BufferManagerPtr& bufferManager,
                                 std::deque<detail::MemorySegment*>&& buffers,
                                 size_t numberOfReservedBuffers)
    : bufferManager(bufferManager),
#ifdef NES_USE_LATCH_FREE_BUFFER_MANAGER
      exclusiveBuffers(numberOfReservedBuffers),
#endif
      numberOfReservedBuffers(numberOfReservedBuffers) {

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
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
        allSegments.emplace_back(memSegment);
#endif
    }
}

LocalBufferPool::~LocalBufferPool() {
    // nop
}

BufferManagerType LocalBufferPool::getBufferManagerType() const { return BufferManagerType::LOCAL; }

void LocalBufferPool::destroy() {
    NES_DEBUG("Destroying LocalBufferPool");
    std::unique_lock lock(mutex);
    auto ownedBufferManager = bufferManager.lock();

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    if (numberOfReservedBuffers != exclusiveBuffers.size()) {
        for (auto segment : allSegments) {
            segment->controlBlock->dumpOwningThreadInfo();
        }
    }
#endif

    NES_ASSERT2_FMT(numberOfReservedBuffers == (size_t)exclusiveBuffers.size(),
                    "one or more buffers were not returned to the pool: " << exclusiveBuffers.size() << " but expected "
                                                                          << numberOfReservedBuffers);

    NES_DEBUG("buffers before=" << ownedBufferManager->getAvailableBuffers()
                                << " size of local buffers=" << exclusiveBuffers.size());
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    while (!exclusiveBuffers.empty()) {
        // return exclusive buffers to the global pool
        detail::MemorySegment* memSegment = exclusiveBuffers.front();
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
}

size_t LocalBufferPool::getAvailableBuffers() const {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    std::unique_lock lock(mutex);
    return exclusiveBuffers.size();
#else
    auto qSize = exclusiveBuffers.size();
    return qSize > 0 ? qSize : 0;
#endif
}

TupleBuffer LocalBufferPool::getBufferBlocking() {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    {
        // try to get an exclusive buffer
        std::unique_lock lock(mutex);
        if (!exclusiveBuffers.empty()) {
            detail::MemorySegment* memSegment = exclusiveBuffers.front();
            NES_VERIFY(memSegment, "null memory segment");
            exclusiveBuffers.pop_front();
            if (memSegment->controlBlock->prepare()) {
                return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
            }
            NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter "
                                    << memSegment->controlBlock->getReferenceCount());
        }
    }
    // fallback to global pool
    if (bufferManager.expired()) {
        NES_THROW_RUNTIME_ERROR("Buffer manager weak ptr is expired");
    }
    // TODO potential problem here: what if we are blocked here but one exclusive buffer is returned to the pool?
    return bufferManager.lock()->getBufferBlocking();
#else
    detail::MemorySegment* memSegment;
    if (exclusiveBuffers.read(memSegment)) {
        if (memSegment->controlBlock->prepare()) {
            return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
        } else {
            NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter "
                                    << memSegment->controlBlock->getReferenceCount());
        }
    }
    return bufferManager.lock()->getBufferBlocking();
#endif
}

void LocalBufferPool::recyclePooledBuffer(detail::MemorySegment* memSegment) {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    std::unique_lock lock(mutex);
    NES_VERIFY(memSegment, "null memory segment");
    if (!memSegment->isAvailable()) {
        NES_THROW_RUNTIME_ERROR(
            "Recycling buffer callback invoked on used memory segment refcnt=" << memSegment->controlBlock->getReferenceCount());
    }
    // add back an exclusive buffer to the local pool
    exclusiveBuffers.emplace_back(memSegment);
#else
    NES_VERIFY(memSegment, "null memory segment");
    if (!memSegment->isAvailable()) {
        NES_THROW_RUNTIME_ERROR(
            "Recycling buffer callback invoked on used memory segment refcnt=" << memSegment->controlBlock->getReferenceCount());
    }
    exclusiveBuffers.write(memSegment);
#endif
}

void LocalBufferPool::recycleUnpooledBuffer(detail::MemorySegment*) {
    NES_THROW_RUNTIME_ERROR("This feature is not supported here");
}
}// namespace NES::Runtime
