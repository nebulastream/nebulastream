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

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/LocalBufferPool.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/detail/TupleBufferImpl.hpp>

namespace NES::NodeEngine {
LocalBufferPool::LocalBufferPool(BufferManagerPtr bufferManager, std::deque<detail::MemorySegment*>&& buffers,
                                 size_t numberOfReservedBuffers)
    : bufferManager(bufferManager), exclusiveBuffers(), numberOfReservedBuffers(numberOfReservedBuffers) {

    while (!buffers.empty()) {
        auto memSegment = buffers.front();
        buffers.pop_front();
        NES_VERIFY(memSegment, "null memory segment");
        memSegment->controlBlock->resetBufferRecycler(this);
        NES_ASSERT2_FMT(memSegment->isAvailable(), "Buffer not available");
        exclusiveBuffers.emplace_back(memSegment);
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
        allSegments.emplace_back(memSegment);
#endif
    }
}

LocalBufferPool::~LocalBufferPool() {
    // nop
}

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

    NES_ASSERT2_FMT(numberOfReservedBuffers == exclusiveBuffers.size(),
                    "one or more buffers were not returned to the pool: " << exclusiveBuffers.size() << " but expected "
                                                                          << numberOfReservedBuffers);

    NES_DEBUG("buffers before=" << ownedBufferManager->getAvailableBuffers()
                                << " size of local buffers=" << exclusiveBuffers.size());
    while (!exclusiveBuffers.empty()) {
        // return exclusive buffers to the global pool
        detail::MemorySegment* memSegment = exclusiveBuffers.front();
        exclusiveBuffers.pop_front();
        memSegment->controlBlock->resetBufferRecycler(ownedBufferManager.get());
        ownedBufferManager->recyclePooledBuffer(memSegment);
    }
    NES_DEBUG("buffers after=" << ownedBufferManager->getAvailableBuffers()
                               << " size of local buffers=" << exclusiveBuffers.size());
}

size_t LocalBufferPool::getAvailableExclusiveBuffers() const {
    std::unique_lock lock(mutex);
    return exclusiveBuffers.size();
}

TupleBuffer LocalBufferPool::getBufferBlocking() {
    {
        // try to get an exclusive buffer
        std::unique_lock lock(mutex);
        if (exclusiveBuffers.size() > 0) {
            detail::MemorySegment* memSegment = exclusiveBuffers.front();
            NES_VERIFY(memSegment, "null memory segment");
            exclusiveBuffers.pop_front();
            if (memSegment->controlBlock->prepare()) {
                return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
            } else {
                NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter "
                                        << memSegment->controlBlock->getReferenceCount());
            }
        }
    }
    // fallback to global pool
    if (bufferManager.expired()) {
        NES_THROW_RUNTIME_ERROR("Buffer manager weak ptr is expired");
    }
    // TODO potential problem here: what if we are blocked here but one exclusive buffer is returned to the pool?
    return bufferManager.lock()->getBufferBlocking();
}

void LocalBufferPool::recyclePooledBuffer(detail::MemorySegment* memSegment) {
    std::unique_lock lock(mutex);
    NES_VERIFY(memSegment, "null memory segment");
    if (!memSegment->isAvailable()) {
        NES_THROW_RUNTIME_ERROR(
            "Recycling buffer callback invoked on used memory segment refcnt=" << memSegment->controlBlock->getReferenceCount());
    }
    // add back an exclusive buffer to the local pool
    exclusiveBuffers.emplace_back(memSegment);
}

void LocalBufferPool::recycleUnpooledBuffer(detail::MemorySegment*) {
    NES_THROW_RUNTIME_ERROR("This feature is not supported here");
}
}// namespace NES::NodeEngine
