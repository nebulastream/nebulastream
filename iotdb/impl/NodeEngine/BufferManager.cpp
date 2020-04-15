#include <NodeEngine/BufferManager.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <utility>
#include <Util/Logger.hpp>
#include <NodeEngine/TupleBuffer.hpp>

namespace NES {

BufferManager::BufferManager() : bufferSize(0), numOfBuffers(0), isConfigured(false), shutdownRequested(false) {

}

BufferManager::~BufferManager() {
    // RAII takes care of deallocating memory here
    allBuffers.clear();
    for (auto& holder : unpooledBuffers) {
        assert(holder.segment != nullptr);
        delete holder.segment;
    }
}

void BufferManager::configure(size_t bufferSize, size_t numOfBuffers) {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    assert(!shutdownRequested);
    if (isConfigured) {
        NES_DEBUG("[BufferManager] Already configured - we cannot change the buffer manager at runtime for now!");
        assert(false);
    }
    this->bufferSize = bufferSize;
    this->numOfBuffers = numOfBuffers;
    allBuffers.reserve(numOfBuffers);
    availableBuffers.reserve(numOfBuffers);
    for (size_t i = 0; i < numOfBuffers; ++i) {
        auto ptr = static_cast<uint8_t*>(malloc(bufferSize));
        assert(ptr != nullptr);
        allBuffers.emplace_back(ptr, bufferSize, [this](MemorySegment* segment) {
            recyclePooledBuffer(segment);
        });
        availableBuffers.emplace_back(&allBuffers.back());
    }
    isConfigured = true;
}

void BufferManager::requestShutdown() {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    if (shutdownRequested) {
        NES_ERROR("[BufferManager] Requested buffer manager shutdown but it was already blocked");
    }
    shutdownRequested = true;
    assert(allBuffers.size() == availableBuffers.size());
    lock.unlock();
    for (size_t i = 0; i < numOfBuffers; ++i) {
        assert(allBuffers[i].isAvailable());
    }
}

TupleBuffer BufferManager::getBufferBlocking() {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    assert(!shutdownRequested);
    while (availableBuffers.empty()) {
        availableBuffersCvar.wait(lock);
    }
    auto memSegment = availableBuffers.back();
    availableBuffers.pop_back();
    return memSegment->toTupleBuffer();
}

std::optional<TupleBuffer> BufferManager::getBufferNoBlocking() {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    assert(!shutdownRequested);
    if (availableBuffers.empty()) {
        return std::nullopt;
    }
    auto memSegment = availableBuffers.back();
    availableBuffers.pop_back();
    return memSegment->toTupleBuffer();
}

std::optional<TupleBuffer> BufferManager::getBufferTimeout(std::chrono::milliseconds timeout_ms) {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    assert(!shutdownRequested);
    auto res = availableBuffersCvar.wait_for(lock, timeout_ms, [=]() {
        return !availableBuffers.empty();
    });
    if (!res) {
        return std::nullopt;
    }
    auto memSegment = availableBuffers.back();
    availableBuffers.pop_back();
    return memSegment->toTupleBuffer();
}

std::optional<TupleBuffer> BufferManager::getUnpooledBuffer(size_t bufferSize) {
    std::unique_lock<std::mutex> lock(unpooledBuffersMutex);
    UnpooledBufferHolder probe;
    probe.size = bufferSize;
    probe.segment = nullptr;
    auto candidate = std::lower_bound(unpooledBuffers.begin(), unpooledBuffers.end(), probe);
    if (candidate != unpooledBuffers.end()) {
        // it points to a segment of size at least bufferSize;
        for (auto it = candidate; it != unpooledBuffers.end(); ++it) {
            if (it->size == bufferSize) {
                if (it->segment != nullptr) {
                    auto segment = (*it).segment;
                    (*it).segment = nullptr;
                    return segment->toTupleBuffer();
                }
            } else {
                break;
            }
        }
    }
    // we could not find a buffer, allocate it
    auto ptr = static_cast<uint8_t*>(malloc(bufferSize));
    auto segment = new MemorySegment(ptr, bufferSize, [this](MemorySegment* segment) {
      recycleUnpooledBuffer(segment);
    });
    unpooledBuffers.push_back(probe);
    return segment->toTupleBuffer();
}

void BufferManager::recyclePooledBuffer(MemorySegment* segment) {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    assert(!shutdownRequested);
    assert(segment->isAvailable());
    availableBuffers.emplace_back(segment);
    availableBuffersCvar.notify_all();
}

void BufferManager::recycleUnpooledBuffer(MemorySegment* segment) {
    std::unique_lock<std::mutex> lock(unpooledBuffersMutex);
    assert(!shutdownRequested);
    assert(segment->isAvailable());
    UnpooledBufferHolder probe;
    probe.size = segment->getSize();
    probe.segment = nullptr;
    auto candidate = std::lower_bound(unpooledBuffers.begin(), unpooledBuffers.end(), probe);
    if (candidate != unpooledBuffers.end()) {
        // it points to a segment of size at least bufferSize;
        for (auto it = candidate; it != unpooledBuffers.end(); ++it) {
            if (it->size == probe.size) {
                if (it->segment == nullptr) {
                    it->segment = segment;
                    return;
                }
            } else {
                break;
            }
        }
    }
    // we could not recycle a position
    probe.segment = segment;
    unpooledBuffers.insert(candidate, probe);
}

size_t BufferManager::getBufferSize() const {
    return bufferSize;
}

size_t BufferManager::getNumOfPooledBuffers() const {
    return numOfBuffers;
}

size_t BufferManager::getNumOfUnpooledBuffers() {
    std::unique_lock<std::mutex> lock(unpooledBuffersMutex);
    return unpooledBuffers.size();
}

size_t BufferManager::getAvailableBuffers() {
    std::unique_lock<std::mutex> lock(availableBuffersMutex);
    return availableBuffers.size();
}

BufferManager& BufferManager::instance() {
    static BufferManager instance;
    return instance;
}

void BufferManager::printStatistics() {
    NES_INFO("BufferManager Statistics:")
}

}  // namespace NES
