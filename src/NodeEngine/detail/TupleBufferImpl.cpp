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

#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <bitset>
#include <cstring>
#include <exception>
#include <iostream>
#include <sstream>

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
#include <NodeEngine/internal/backtrace.hpp>
#include <mutex>
#include <thread>
#endif

namespace NES::NodeEngine {

namespace detail {

// -----------------------------------------------------------------------------
// ------------------ Core Mechanism for Buffer recycling ----------------------
// -----------------------------------------------------------------------------

MemorySegment::MemorySegment(const MemorySegment& other) : ptr(other.ptr), size(other.size), controlBlock(other.controlBlock) {}

MemorySegment& MemorySegment::operator=(const MemorySegment& other) {
    ptr = other.ptr;
    size = other.size;
    controlBlock = other.controlBlock;
    return *this;
}

MemorySegment::MemorySegment() : ptr(nullptr), size(0), controlBlock(nullptr) {}

MemorySegment::MemorySegment(uint8_t* ptr,
                             uint32_t size,
                             BufferRecycler* recycler,
                             std::function<void(MemorySegment*, BufferRecycler*)>&& recycleFunction)
    : ptr(ptr), size(size) {
    controlBlock = new (ptr + size) BufferControlBlock(this, recycler, std::move(recycleFunction));
    if (!this->ptr) {
        NES_THROW_RUNTIME_ERROR("[MemorySegment] invalid pointer");
    }
    if (!this->size) {
        NES_THROW_RUNTIME_ERROR("[MemorySegment] invalid size");
    }
}

MemorySegment::MemorySegment(uint8_t* ptr,
                             uint32_t size,
                             BufferRecycler* recycler,
                             std::function<void(MemorySegment*, BufferRecycler*)>&& recycleFunction,
                             bool)
    : ptr(ptr), size(size) {
    // TODO ensure this doesnt break zmq recycle callback (Ventura)
    controlBlock = new BufferControlBlock(this, recycler, std::move(recycleFunction));
    if (!this->ptr) {
        NES_THROW_RUNTIME_ERROR("[MemorySegment] invalid pointer");
    }
    if (!this->size) {
        NES_THROW_RUNTIME_ERROR("[MemorySegment] invalid size");
    }
    controlBlock->prepare();
}

MemorySegment::~MemorySegment() {
    if (ptr) {
        auto refCnt = controlBlock->getReferenceCount();
        if (refCnt != 0) {
            NES_ERROR("Expected 0 as ref cnt but got " << refCnt);
            NES_THROW_RUNTIME_ERROR("[MemorySegment] invalid reference counter on mem segment dtor");
        }
        if ((ptr + size) != reinterpret_cast<uint8_t*>(controlBlock)) {
            delete controlBlock;
        }
        free(ptr);
        ptr = nullptr;
    }
}

BufferControlBlock::BufferControlBlock(MemorySegment* owner,
                                       BufferRecycler* recycler,
                                       std::function<void(MemorySegment*, BufferRecycler*)>&& recycleCallback)
    : referenceCounter(0), numberOfTuples(0), owner(owner), owningBufferRecycler(recycler), recycleCallback(recycleCallback),
      watermark(0), originId(0) {}

BufferControlBlock::BufferControlBlock(const BufferControlBlock& that) {
    referenceCounter.store(that.referenceCounter.load());
    numberOfTuples = that.numberOfTuples.load();
    creationTimestamp = that.creationTimestamp.load();
    recycleCallback = that.recycleCallback;
    owner = that.owner;
    watermark = that.watermark.load();
    originId = that.originId.load();
}

BufferControlBlock& BufferControlBlock::operator=(const BufferControlBlock& that) {
    referenceCounter.store(that.referenceCounter.load());
    numberOfTuples = that.numberOfTuples.load();
    recycleCallback = that.recycleCallback;
    owner = that.owner;
    watermark = that.watermark.load();
    creationTimestamp = that.creationTimestamp.load();
    originId = that.originId.load();
    return *this;
}

MemorySegment* BufferControlBlock::getOwner() { return owner; }

void BufferControlBlock::resetBufferRecycler(BufferRecycler* recycler) {
    NES_ASSERT2_FMT(recycler, "invalid recycler");
    auto oldRecycler = owningBufferRecycler.exchange(recycler);
    NES_ASSERT2_FMT(recycler != oldRecycler, "invalid recycler");
}

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
/**
 * @brief This function collects the thread name and the callstack of the calling thread
 * @param threadName
 * @param callstack
 */
void fillThreadOwnershipInfo(std::string& threadName, std::string& callstack) {
    static constexpr int CALLSTACK_DEPTH = 16;

    backward::StackTrace st;
    backward::Printer p;
    st.load_here(CALLSTACK_DEPTH);
    std::stringbuf callStackBuffer;
    std::ostream os0(&callStackBuffer);
    p.print(st, os0);

    std::stringbuf threadNameBuffer;
    std::ostream os1(&threadNameBuffer);
    os1 << std::this_thread::get_id();

    threadName = threadNameBuffer.str();
    callstack = callStackBuffer.str();
}
#endif
bool BufferControlBlock::prepare() {
    int32_t expected = 0;
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    // store the current thread that owns the buffer and track which function obtained the buffer
    std::unique_lock lock(owningThreadsMutex);
    ThreadOwnershipInfo info;
    fillThreadOwnershipInfo(info.threadName, info.callstack);
    owningThreads[std::this_thread::get_id()].emplace_back(info);
#endif
    if (referenceCounter.compare_exchange_strong(expected, 1)) {
        return true;
    }
    NES_ERROR("Invalid reference counter: " << expected);
    return false;
}

BufferControlBlock* BufferControlBlock::retain() {
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    // store the current thread that owns the buffer (shared) and track which function increased the coutner of the buffer
    std::unique_lock lock(owningThreadsMutex);
    ThreadOwnershipInfo info;
    fillThreadOwnershipInfo(info.threadName, info.callstack);
    owningThreads[std::this_thread::get_id()].emplace_back(info);
#endif
    referenceCounter++;
    return this;
}

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
void BufferControlBlock::dumpOwningThreadInfo() {
    std::unique_lock lock(owningThreadsMutex);
    NES_FATAL_ERROR("Buffer " << getOwner() << " has " << referenceCounter.load() << " live references");
    for (auto& item : owningThreads) {
        for (auto& v : item.second) {
            NES_FATAL_ERROR("Thread " << v.threadName << " has buffer " << getOwner()
                                      << " requested on callstack: " << v.callstack);
        }
    }
}
#endif

int32_t BufferControlBlock::getReferenceCount() const noexcept { return referenceCounter.load(); }

bool BufferControlBlock::release() {
    if (uint32_t const prevRefCnt = referenceCounter.fetch_sub(1); prevRefCnt == 1) {
        numberOfTuples = 0;
        recycleCallback(owner, owningBufferRecycler.load());
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
        {
            std::unique_lock lock(owningThreadsMutex);
            owningThreads.clear();
        }
#endif
        return true;
    } else if (prevRefCnt == 0) {
        NES_THROW_RUNTIME_ERROR("BufferControlBlock: releasing an already released buffer");
    }
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    {
        std::unique_lock lock(owningThreadsMutex);
        auto& v = owningThreads[std::this_thread::get_id()];
        if (!v.empty()) {
            v.pop_front();
        }
    }
#endif
    return false;
}

uint64_t BufferControlBlock::getNumberOfTuples() const noexcept { return numberOfTuples; }

void BufferControlBlock::setNumberOfTuples(uint64_t numberOfTuples) { this->numberOfTuples = numberOfTuples; }

uint64_t BufferControlBlock::getWatermark() const noexcept { return watermark; }

void BufferControlBlock::setWatermark(uint64_t watermark) { this->watermark = watermark; }

void BufferControlBlock::setCreationTimestamp(uint64_t ts) { this->creationTimestamp = ts; }

uint64_t BufferControlBlock::getCreationTimestamp() const noexcept { return creationTimestamp; }

uint64_t BufferControlBlock::getOriginId() const noexcept { return originId; }
void BufferControlBlock::setOriginId(uint64_t originId) { this->originId = originId; }

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
BufferControlBlock::ThreadOwnershipInfo::ThreadOwnershipInfo(std::string&& threadName, std::string&& callstack)
    : threadName(threadName), callstack(callstack) {
    // nop
}

BufferControlBlock::ThreadOwnershipInfo::ThreadOwnershipInfo() : threadName("NOT-SAMPLED"), callstack("NOT-SAMPLED") {
    // nop
}
#endif

// -----------------------------------------------------------------------------
// ------------------ Utility functions for TupleBuffer ------------------------
// -----------------------------------------------------------------------------

void zmqBufferRecyclingCallback(void* ptr, void* hint) {
    auto bufferSize = reinterpret_cast<uintptr_t>(hint);
    auto buffer = reinterpret_cast<uint8_t*>(ptr);
    auto controlBlock = reinterpret_cast<BufferControlBlock*>(buffer + bufferSize);
    controlBlock->release();
}

}// namespace detail

}// namespace NES::NodeEngine