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
#include <NodeEngine/LocalBufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/detail/TupleBufferImpl.hpp>

namespace NES::NodeEngine {
LocalBufferManager::LocalBufferManager(BufferManagerPtr bufferManager, std::deque<detail::MemorySegment*>&& buffers,
                                       size_t numberOfReservedBuffers)
    : bufferManager(bufferManager), exclusiveBuffers(), numberOfReservedBuffers(numberOfReservedBuffers) {

    while (!buffers.empty()) {
        auto memSegment = buffers.front();
        buffers.pop_front();
        NES_VERIFY(memSegment, "null memory segment");
        memSegment->controlBlock->resetBufferRecycler(this);
        NES_ASSERT2_FMT(memSegment->isAvailable(), "Buffer not available");
        exclusiveBuffers.emplace_back(memSegment);
    }
}

LocalBufferManager::~LocalBufferManager() {
    std::unique_lock lock(mutex);
    while (!exclusiveBuffers.empty()) {
        // return exclusive buffers to the global pool
        auto memSegment = exclusiveBuffers.front();
        exclusiveBuffers.pop_front();
        memSegment->controlBlock->resetBufferRecycler(bufferManager.get());
        bufferManager->recyclePooledBuffer(memSegment);
    }
}

size_t LocalBufferManager::getAvailableExclusiveBuffers() const {
    std::unique_lock lock(mutex);
    return exclusiveBuffers.size();
}

TupleBuffer LocalBufferManager::getBuffer() {
    {
        // try to get an exclusive buffer
        std::unique_lock lock(mutex);
        if (exclusiveBuffers.size() > 0) {
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
    }
    // fallback to global pool
    return bufferManager->getBufferBlocking();
}

void LocalBufferManager::recyclePooledBuffer(detail::MemorySegment* memSegment) {
    std::unique_lock lock(mutex);
    NES_VERIFY(memSegment, "null memory segment");
    if (!memSegment->isAvailable()) {
        NES_THROW_RUNTIME_ERROR(
            "Recycling buffer callback invoked on used memory segment refcnt=" << memSegment->controlBlock->getReferenceCount());
    }
    // add back an exclusive buffer to the local pool
    exclusiveBuffers.emplace_back(memSegment);
}

void LocalBufferManager::recycleUnpooledBuffer(detail::MemorySegment*) {
    NES_THROW_RUNTIME_ERROR("This feature is not supported here");
}
}// namespace NES::NodeEngine
