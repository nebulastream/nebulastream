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
#include <NodeEngine/FixedSizeBufferPool.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/detail/TupleBufferImpl.hpp>

namespace NES::NodeEngine {
FixedSizeBufferPool::FixedSizeBufferPool(BufferManagerPtr bufferManager, std::deque<detail::MemorySegment*>&& buffers,
                                         size_t numberOfReservedBuffers)
    : bufferManager(bufferManager), exclusiveBuffers(), numberOfReservedBuffers(numberOfReservedBuffers), isDestroyed(false) {

    while (!buffers.empty()) {
        auto memSegment = buffers.front();
        buffers.pop_front();
        NES_VERIFY(memSegment, "null memory segment");
        memSegment->controlBlock->resetBufferRecycler(this);
        NES_ASSERT2_FMT(memSegment->isAvailable(), "Buffer not available");
        exclusiveBuffers.emplace_back(memSegment);
    }
}

FixedSizeBufferPool::~FixedSizeBufferPool() {
    // nop
}

void FixedSizeBufferPool::destroy() {
    NES_DEBUG("Destroying LocalBufferPool");
    std::unique_lock lock(mutex);
    if (isDestroyed) {
        return;
    }
    auto ownedBufferManager = bufferManager.lock();
    while (!exclusiveBuffers.empty()) {
        // return exclusive buffers to the global pool
        auto memSegment = exclusiveBuffers.front();
        exclusiveBuffers.pop_front();
        memSegment->controlBlock->resetBufferRecycler(ownedBufferManager.get());
        ownedBufferManager->recyclePooledBuffer(memSegment);
    }
    NES_DEBUG("buffers after=" << ownedBufferManager->getAvailableBuffers()
                               << " size of local buffers=" << exclusiveBuffers.size());
    isDestroyed = true;
}

size_t FixedSizeBufferPool::getAvailableExclusiveBuffers() const {
    std::unique_lock lock(mutex);
    return exclusiveBuffers.size();
}

std::optional<TupleBuffer> FixedSizeBufferPool::getBufferTimeout(std::chrono::milliseconds timeout_ms) {
    std::unique_lock lock(mutex);
    auto pred = [=]() {
        return !exclusiveBuffers.empty();
    };
    if (!cvar.wait_for(lock, timeout_ms, std::move(pred))) {
        return std::nullopt;
    }
    auto memSegment = exclusiveBuffers.front();
    exclusiveBuffers.pop_front();
    if (memSegment->controlBlock->prepare()) {
        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
    } else {
        NES_THROW_RUNTIME_ERROR("[FixedSizeBufferPool] got buffer with invalid reference counter");
    }
}

TupleBuffer FixedSizeBufferPool::getBufferBlocking() {
    // try to get an exclusive buffer
    std::unique_lock lock(mutex);
    while (exclusiveBuffers.size() == 0) {
        cvar.wait(lock);
    }
    auto memSegment = exclusiveBuffers.front();
    NES_VERIFY(memSegment, "null memory segment");
    exclusiveBuffers.pop_front();
    if (memSegment->controlBlock->prepare()) {
        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
    } else {
        NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter "
                                << memSegment->controlBlock->getReferenceCount());
    }
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
        exclusiveBuffers.emplace_back(memSegment);
        cvar.notify_all();
    }
}

void FixedSizeBufferPool::recycleUnpooledBuffer(detail::MemorySegment*) {
    NES_THROW_RUNTIME_ERROR("This feature is not supported here");
}
}// namespace NES::NodeEngine
