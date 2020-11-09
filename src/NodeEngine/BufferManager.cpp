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
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/detail/TupleBufferImpl.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <thread>

namespace NES {

BufferManager::BufferManager() : bufferSize(0), numOfBuffers(0), isConfigured(false) {
    // nop
}

BufferManager::BufferManager(size_t bufferSize, size_t numOfBuffers)
    : bufferSize(0), numOfBuffers(0), isConfigured(false) {
    configure(bufferSize, numOfBuffers);
}

BufferManager::~BufferManager() {
    std::scoped_lock lock(availableBuffersMutex, unpooledBuffersMutex);
    auto success = true;
    if (allBuffers.size() != availableBuffers.size()) {
        NES_ERROR("[BufferManager] total buffers " << allBuffers.size() << " :: available buffers "
                                                   << availableBuffers.size());
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
        NES_THROW_RUNTIME_ERROR("[BufferManager] Requested buffer manager shutdown but a buffer is still used");
    }
    // RAII takes care of deallocating memory here
    allBuffers.clear();
    for (auto& holder : unpooledBuffers) {
        if (!holder.segment || holder.segment->controlBlock->getReferenceCount() != 0) {
            NES_THROW_RUNTIME_ERROR("Deletion of unpooled buffer invoked on used memory segment");
        }
    }
    unpooledBuffers.clear();
}

void BufferManager::configure(size_t bufferSize, size_t numOfBuffers) {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    if (isConfigured) {
        NES_THROW_RUNTIME_ERROR(
            "[BufferManager] Already configured - we cannot change the buffer manager at runtime for now!");
    }
    this->bufferSize = bufferSize;
    this->numOfBuffers = numOfBuffers;
    allBuffers.reserve(numOfBuffers);
    auto realSize = bufferSize + sizeof(detail::BufferControlBlock);
    for (size_t i = 0; i < numOfBuffers; ++i) {
        auto ptr = static_cast<uint8_t*>(malloc(realSize));
        if (ptr == nullptr) {
            NES_THROW_RUNTIME_ERROR("[BufferManager] memory allocation failed");
        }
        allBuffers.emplace_back(ptr, bufferSize,
                                [this](detail::MemorySegment* segment) {
                                    recyclePooledBuffer(segment);
                                });
        availableBuffers.emplace_back(&allBuffers.back());
    }
    isConfigured = true;
}

TupleBuffer BufferManager::getBufferBlocking() {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    while (availableBuffers.empty()) {
        availableBuffersCvar.wait(lock);
    }
    auto memSegment = availableBuffers.front();
    availableBuffers.pop_front();
    if (memSegment->controlBlock->prepare()) {
        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
    } else {
        NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter");
    }
}

std::optional<TupleBuffer> BufferManager::getBufferNoBlocking() {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    if (availableBuffers.empty()) {
        return std::nullopt;
    }
    auto memSegment = availableBuffers.front();
    availableBuffers.pop_front();
    if (memSegment->controlBlock->prepare()) {
        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
    } else {
        NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter");
    }
}

std::optional<TupleBuffer> BufferManager::getBufferTimeout(std::chrono::milliseconds timeout_ms) {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    auto pred = [=]() {
        return !availableBuffers.empty();
    };
    if (!availableBuffersCvar.wait_for(lock, timeout_ms, std::move(pred))) {
        return std::nullopt;
    }
    auto memSegment = availableBuffers.front();
    availableBuffers.pop_front();
    if (memSegment->controlBlock->prepare()) {
        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
    } else {
        NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter");
    }
}

std::optional<TupleBuffer> BufferManager::getUnpooledBuffer(size_t bufferSize) {
    std::unique_lock<std::mutex> lock(unpooledBuffersMutex);
    UnpooledBufferHolder probe(bufferSize);
    auto candidate = std::lower_bound(unpooledBuffers.begin(), unpooledBuffers.end(), probe);
    if (candidate != unpooledBuffers.end()) {
        // it points to a segment of size at least bufferSize;
        for (auto it = candidate; it != unpooledBuffers.end(); ++it) {
            if (it->size == bufferSize) {
                if (it->free) {
                    auto memSegment = (*it).segment.get();
                    it->free = false;
                    if (memSegment->controlBlock->prepare()) {
                        return TupleBuffer(memSegment->controlBlock, memSegment->ptr, memSegment->size);
                    } else {
                        NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter");
                    }
                }
            } else {
                break;
            }
        }
    }
    // we could not find a buffer, allocate it
    auto ptr = static_cast<uint8_t*>(malloc(bufferSize + sizeof(detail::BufferControlBlock)));
    if (ptr == nullptr) {
        NES_THROW_RUNTIME_ERROR("BufferManager: unpooled memory allocation failed");
    }
    auto memSegment = std::make_unique<detail::MemorySegment>(
        ptr, bufferSize, [this](detail::MemorySegment* segment) {
            recycleUnpooledBuffer(segment);
        });
    auto leakedMemSegment = memSegment.get();
    unpooledBuffers.emplace_back(std::move(memSegment), bufferSize);
    if (leakedMemSegment->controlBlock->prepare()) {
        return TupleBuffer(leakedMemSegment->controlBlock, leakedMemSegment->ptr, leakedMemSegment->size);
    } else {
        NES_THROW_RUNTIME_ERROR("[BufferManager] got buffer with invalid reference counter");
    }
}

void BufferManager::recyclePooledBuffer(detail::MemorySegment* segment) {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    if (!segment->isAvailable()) {
        NES_THROW_RUNTIME_ERROR("Recycling buffer callback invoked on used memory segment");
    }
    availableBuffers.emplace_back(segment);
    availableBuffersCvar.notify_all();
}

void BufferManager::recycleUnpooledBuffer(detail::MemorySegment* segment) {
    std::unique_lock<std::mutex> lock(unpooledBuffersMutex);
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
    NES_THROW_RUNTIME_ERROR("recycleUnpooledBuffer called on invalid buffer");
    //    // we could not recycle a position
    //    probe.segment.reset(segment);
    //    probe.markFree();
    //    unpooledBuffers.insert(candidate, std::move(probe));
}

size_t BufferManager::getBufferSize() const { return bufferSize; }

size_t BufferManager::getNumOfPooledBuffers() const { return numOfBuffers; }

size_t BufferManager::getNumOfUnpooledBuffers() {
    std::unique_lock<std::mutex> lock(unpooledBuffersMutex);
    return unpooledBuffers.size();
}

size_t BufferManager::getAvailableBuffers() {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    return availableBuffers.size();
}

void BufferManager::printStatistics() {
    NES_INFO("BufferManager Statistics:");
}

BufferManager::UnpooledBufferHolder::UnpooledBufferHolder()
    : size(0), free(false) {
    segment.reset();
}

BufferManager::UnpooledBufferHolder::UnpooledBufferHolder(uint32_t bufferSize) : size(bufferSize), free(false) {
    segment.reset();
}

BufferManager::UnpooledBufferHolder::UnpooledBufferHolder(std::unique_ptr<detail::MemorySegment>&& mem, uint32_t size)
    : segment(std::move(mem)), size(size), free(false) {
    // nop
}

void BufferManager::UnpooledBufferHolder::markFree() { free = true; }

}// namespace NES
