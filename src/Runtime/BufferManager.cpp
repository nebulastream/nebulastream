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

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/detail/TupleBufferImpl.hpp>
#include <Util/Logger.hpp>
#include <cstring>
#ifdef NES_USE_LATCH_FREE_BUFFER_MANAGER
#include <folly/MPMCQueue.h>
#endif
#include <iostream>
#include <thread>
#include <unistd.h>
namespace NES::Runtime {

BufferManager::BufferManager(uint32_t bufferSize, uint32_t numOfBuffers, uint32_t withAlignment)
    :
#ifdef NES_USE_LATCH_FREE_BUFFER_MANAGER
      availableBuffers(numOfBuffers),
#endif
      bufferSize(bufferSize), numOfBuffers(numOfBuffers) {
    initialize(withAlignment);
}

void BufferManager::destroy() {
    std::scoped_lock lock(availableBuffersMutex, unpooledBuffersMutex, localBufferPoolsMutex);
    auto success = true;
    NES_DEBUG("Shutting down Buffer Manager " << this);
    for (auto& localPool : localBufferPools) {
        localPool->destroy();
    }
    localBufferPools.clear();
    if (allBuffers.size() != (size_t) availableBuffers.size()) {
        NES_ERROR("[BufferManager] total buffers " << allBuffers.size() << " :: available buffers " << availableBuffers.size());
        success = false;
    }
    for (auto& buffer : allBuffers) {
        if (!buffer.isAvailable()) {
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
            buffer.controlBlock->dumpOwningThreadInfo();
#endif
            success = false;
        }
    }
    if (!success) {
        NES_THROW_RUNTIME_ERROR("[BufferManager] Requested buffer manager shutdown but a buffer is still used allBuffers="
                                << allBuffers.size() << " available=" << availableBuffers.size());
    }
    // RAII takes care of deallocating memory here
    allBuffers.clear();
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    availableBuffers.clear();
#else
    availableBuffers = decltype(availableBuffers)();
#endif
    for (auto& holder : unpooledBuffers) {
        if (!holder.segment || holder.segment->controlBlock->getReferenceCount() != 0) {
            NES_THROW_RUNTIME_ERROR("Deletion of unpooled buffer invoked on used memory segment");
        }
    }
    unpooledBuffers.clear();
    NES_DEBUG("Shutting down Buffer Manager completed " << this);
}

BufferManager::~BufferManager() {
    // nop
}

void BufferManager::initialize(uint32_t withAlignment) {
    std::unique_lock lock(availableBuffersMutex);

    long pages = sysconf(_SC_PHYS_PAGES);
    long page_size = sysconf(_SC_PAGE_SIZE);
    auto memorySizeInBytes = static_cast<uint64_t>(pages * page_size);

    uint64_t requiredMemorySpace = (uint64_t) this->bufferSize * (uint64_t) this->numOfBuffers;
    NES_DEBUG("NES malloc " << requiredMemorySpace << " out of " << memorySizeInBytes << " available bytes");

    NES_ASSERT2_FMT(requiredMemorySpace < memorySizeInBytes,
                    "NES tries to malloc more memory than physically available requested=" << requiredMemorySpace
                                                                                           << " available=" << memorySizeInBytes);
    if (withAlignment > 0) {
        if ((withAlignment & (withAlignment - 1))) {// not a pow of two
            NES_THROW_RUNTIME_ERROR("NES tries to align memory but alignment is not a pow of two");
        }
    } else if (withAlignment > page_size) {
        NES_THROW_RUNTIME_ERROR("NES tries to align memory but alignment is invalid");
    }

    allBuffers.reserve(numOfBuffers);
    size_t realSize = bufferSize + sizeof(detail::BufferControlBlock);
    for (size_t i = 0; i < numOfBuffers; ++i) {
        uint8_t* ptr = nullptr;
        if (withAlignment == 0) {
            ptr = static_cast<uint8_t*>(malloc(realSize));
        } else {
            void* tmp = nullptr;
            NES_ASSERT(posix_memalign(&tmp, withAlignment, realSize) == 0, "memory allocation failed with alignment");
            ptr = static_cast<uint8_t*>(tmp);
        }
        if (ptr == nullptr) {
            NES_THROW_RUNTIME_ERROR("memory allocation failed");
        }
        //We touch each buffer to make sure that is really allocated
        std::memset(ptr, 0, realSize);
        allBuffers.emplace_back(ptr, bufferSize, this, [](detail::MemorySegment* segment, BufferRecycler* recycler) {
            recycler->recyclePooledBuffer(segment);
        });
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
        availableBuffers.emplace_back(&allBuffers.back());
#else
        availableBuffers.write(&allBuffers.back());
#endif
    }
    NES_DEBUG("BufferManager configuration bufferSize=" << this->bufferSize << " numOfBuffers=" << this->numOfBuffers);
}

TupleBuffer BufferManager::getBufferBlocking() {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    std::unique_lock lock(availableBuffersMutex);
    while (availableBuffers.empty()) {
        NES_TRACE("All global Buffers are exhausted");
        availableBuffersCvar.wait(lock);
    }
    auto* memSegment = availableBuffers.front();
    availableBuffers.pop_front();
#else
    detail::MemorySegment* memSegment = nullptr;
    availableBuffers.blockingRead(memSegment);
#endif
    if (memSegment->controlBlock->prepare()) {
        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
    }
    NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter");
}

std::optional<TupleBuffer> BufferManager::getBufferNoBlocking() {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    std::unique_lock lock(availableBuffersMutex);
    if (availableBuffers.empty()) {
        return std::nullopt;
    }
    auto* memSegment = availableBuffers.front();
    availableBuffers.pop_front();
#else
    detail::MemorySegment* memSegment = nullptr;
    if (!availableBuffers.read(memSegment)) {
        return std::nullopt;
    }
#endif
    if (memSegment->controlBlock->prepare()) {
        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
    }
    NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter");
}

std::optional<TupleBuffer> BufferManager::getBufferTimeout(std::chrono::milliseconds timeout_ms) {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    std::unique_lock lock(availableBuffersMutex);
    auto pred = [this]() {
        return !availableBuffers.empty();
    };
    if (!availableBuffersCvar.wait_for(lock, timeout_ms, std::move(pred))) {
        return std::nullopt;
    }
    auto* memSegment = availableBuffers.front();
    availableBuffers.pop_front();
#else
    detail::MemorySegment* memSegment;
    auto deadline = std::chrono::steady_clock::now() + timeout_ms;
    if (!availableBuffers.tryReadUntil(deadline, memSegment)) {
        return std::nullopt;
    }
#endif
    if (memSegment->controlBlock->prepare()) {
        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
    }
    NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter");
}

std::optional<TupleBuffer> BufferManager::getUnpooledBuffer(size_t bufferSize) {
    std::unique_lock lock(unpooledBuffersMutex);
    UnpooledBufferHolder probe(bufferSize);
    auto candidate = std::lower_bound(unpooledBuffers.begin(), unpooledBuffers.end(), probe);
    if (candidate != unpooledBuffers.end()) {
        // it points to a segment of size at least bufferSize;
        for (auto it = candidate; it != unpooledBuffers.end(); ++it) {
            if (it->size == bufferSize) {
                if (it->free) {
                    auto* memSegment = (*it).segment.get();
                    it->free = false;
                    if (memSegment->controlBlock->prepare()) {
                        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
                    }
                    NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter");
                }
            } else {
                break;
            }
        }
    }
    // we could not find a buffer, allocate it
    auto* ptr = static_cast<uint8_t*>(malloc(bufferSize + sizeof(detail::BufferControlBlock)));
    if (ptr == nullptr) {
        NES_THROW_RUNTIME_ERROR("BufferManager: unpooled memory allocation failed");
    }
    auto memSegment = std::make_unique<detail::MemorySegment>(ptr,
                                                              bufferSize,
                                                              this,
                                                              [](detail::MemorySegment* segment, BufferRecycler* recycler) {
                                                                  recycler->recycleUnpooledBuffer(segment);
                                                              });
    auto* leakedMemSegment = memSegment.get();
    unpooledBuffers.emplace_back(std::move(memSegment), bufferSize);
    if (leakedMemSegment->controlBlock->prepare()) {
        return TupleBuffer(leakedMemSegment->controlBlock, leakedMemSegment->ptr, leakedMemSegment->size);
    }
    NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter");
}

void BufferManager::recyclePooledBuffer(detail::MemorySegment* segment) {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    std::unique_lock lock(availableBuffersMutex);
    if (!segment->isAvailable()) {
        NES_THROW_RUNTIME_ERROR("Recycling buffer callback invoked on used memory segment");
    }
    availableBuffers.emplace_back(segment);
    availableBuffersCvar.notify_all();
#else
    if (!segment->isAvailable()) {
        NES_THROW_RUNTIME_ERROR("Recycling buffer callback invoked on used memory segment");
    }
    availableBuffers.write(segment);
#endif
}

void BufferManager::recycleUnpooledBuffer(detail::MemorySegment* segment) {
    std::unique_lock lock(unpooledBuffersMutex);
    if (!segment->isAvailable()) {
        NES_THROW_RUNTIME_ERROR("Recycling buffer callback invoked on used memory segment");
    }
    UnpooledBufferHolder probe(segment->getSize());
    auto candidate = std::lower_bound(unpooledBuffers.begin(), unpooledBuffers.end(), probe);
    if (candidate != unpooledBuffers.end()) {
        for (auto it = candidate; it != unpooledBuffers.end(); ++it) {
            if (it->size == probe.size) {
                if (it->segment->ptr == segment->ptr) {
                    it->markFree();
                    return;
                }
            } else {
                break;
            }
        }
    }
}

size_t BufferManager::getBufferSize() const { return bufferSize; }

size_t BufferManager::getNumOfPooledBuffers() const { return numOfBuffers; }

size_t BufferManager::getNumOfUnpooledBuffers() const {
    std::unique_lock lock(unpooledBuffersMutex);
    return unpooledBuffers.size();
}

size_t BufferManager::getAvailableBuffers() const {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    std::unique_lock lock(availableBuffersMutex);
    return availableBuffers.size();
#else
    std::unique_lock lock(localBufferPoolsMutex);
    auto qSize = availableBuffers.size();
    auto avail = qSize > 0 ? qSize : 0;
    for (auto& pool : localBufferPools) {
        avail += pool->getAvailableBuffers();
    }
    return avail;
#endif
}

size_t BufferManager::getAvailableBuffersInFixedSizePools() const {
    std::unique_lock lock(localBufferPoolsMutex);
    size_t sum = 0;
    for (auto& pool : localBufferPools) {
        auto type = pool->getBufferManagerType();
        if (type == BufferManagerType::FIXED) {
            sum += pool->getAvailableBuffers();
        }
    }
    return sum;
}

BufferManagerType BufferManager::getBufferManagerType() const { return BufferManagerType::GLOBAL; }

BufferManager::UnpooledBufferHolder::UnpooledBufferHolder() { segment.reset(); }

BufferManager::UnpooledBufferHolder::UnpooledBufferHolder(uint32_t bufferSize) : size(bufferSize), free(false) {
    segment.reset();
}

BufferManager::UnpooledBufferHolder::UnpooledBufferHolder(std::unique_ptr<detail::MemorySegment>&& mem, uint32_t size)
    : segment(std::move(mem)), size(size), free(false) {
    // nop
}

void BufferManager::UnpooledBufferHolder::markFree() { free = true; }

LocalBufferPoolPtr BufferManager::createLocalBufferPool(size_t numberOfReservedBuffers) {
    std::unique_lock lock(availableBuffersMutex);
    std::deque<detail::MemorySegment*> buffers;
    NES_ASSERT2_FMT((size_t) availableBuffers.size() >= numberOfReservedBuffers, "not enough buffers");//TODO improve error
    for (std::size_t i = 0; i < numberOfReservedBuffers; ++i) {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
        auto* memSegment = availableBuffers.front();
        availableBuffers.pop_front();
        buffers.emplace_back(memSegment);
#else
        detail::MemorySegment* memorySegment;
        availableBuffers.blockingRead(memorySegment);
        buffers.emplace_back(memorySegment);
#endif
    }
    auto ret = std::make_shared<LocalBufferPool>(shared_from_this(), std::move(buffers), numberOfReservedBuffers);
    {
        std::unique_lock lock(localBufferPoolsMutex);
        localBufferPools.push_back(ret);
    }
    return ret;
}

FixedSizeBufferPoolPtr BufferManager::createFixedSizeBufferPool(size_t numberOfReservedBuffers) {
    std::unique_lock lock(availableBuffersMutex);
    std::deque<detail::MemorySegment*> buffers;
    NES_ASSERT2_FMT((size_t) availableBuffers.size() >= numberOfReservedBuffers, "not enough buffers");//TODO improve error
    for (std::size_t i = 0; i < numberOfReservedBuffers; ++i) {
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
        auto* memSegment = availableBuffers.front();
        availableBuffers.pop_front();
        buffers.emplace_back(memSegment);
#else
        detail::MemorySegment* memorySegment;
        availableBuffers.blockingRead(memorySegment);
        buffers.emplace_back(memorySegment);
#endif
    }
    auto ret = std::make_shared<FixedSizeBufferPool>(shared_from_this(), std::move(buffers), numberOfReservedBuffers);
    {
        std::unique_lock lock(localBufferPoolsMutex);
        localBufferPools.push_back(ret);
    }
    return ret;
}

}// namespace NES::Runtime
