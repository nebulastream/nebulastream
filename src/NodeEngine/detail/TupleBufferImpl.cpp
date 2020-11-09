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
#include <boost/endian/buffers.hpp>// see Synopsis below
#include <cstring>
#include <exception>
#include <iostream>
#include <sstream>

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
#include <NodeEngine/internal/backtrace.hpp>
#include <mutex>
#include <thread>
#endif

namespace NES {

namespace detail {

// -----------------------------------------------------------------------------
// ------------------ Core Mechanism for Buffer recycling ----------------------
// -----------------------------------------------------------------------------

MemorySegment::MemorySegment(const MemorySegment& other) : ptr(other.ptr), size(other.size),
                                                           controlBlock(other.controlBlock) {}

MemorySegment& MemorySegment::operator=(const MemorySegment& other) {
    ptr = other.ptr;
    size = other.size;
    controlBlock = other.controlBlock;
    return *this;
}

MemorySegment::MemorySegment() : ptr(nullptr), size(0), controlBlock(nullptr) {}

MemorySegment::MemorySegment(uint8_t* ptr, uint32_t size, std::function<void(MemorySegment*)>&& recycleFunction)
    : ptr(ptr), size(size) {
    controlBlock = new (ptr + size) BufferControlBlock(this, std::move(recycleFunction));
    if (!this->ptr) {
        NES_THROW_RUNTIME_ERROR("[MemorySegment] invalid pointer");
    }
    if (!this->size) {
        NES_THROW_RUNTIME_ERROR("[MemorySegment] invalid size");
    }
}

MemorySegment::~MemorySegment() {
    if (ptr) {
        auto refCnt = controlBlock->getReferenceCount();
        if (refCnt != 0) {
            NES_ERROR("Expected 0 as ref cnt but got " << refCnt);
            NES_THROW_RUNTIME_ERROR("[MemorySegment] invalid reference counter on mem segment dtor");
        }
        free(ptr);
        ptr = nullptr;
    }
}

BufferControlBlock::BufferControlBlock(MemorySegment* owner, std::function<void(MemorySegment*)>&& recycleCallback) : referenceCounter(0), numberOfTuples(0), owner(owner),
                                                                                                                      recycleCallback(recycleCallback), watermark(0), originId(0) {
}

BufferControlBlock::BufferControlBlock(const BufferControlBlock& that) {
    referenceCounter.store(that.referenceCounter.load());
    numberOfTuples.store(that.numberOfTuples.load());
    recycleCallback = that.recycleCallback;
    owner = that.owner;
    watermark.store(that.watermark.load());
    originId.store(that.originId.load());
}

BufferControlBlock& BufferControlBlock::operator=(const BufferControlBlock& that) {
    referenceCounter.store(that.referenceCounter.load());
    numberOfTuples.store(that.numberOfTuples.load());
    recycleCallback = that.recycleCallback;
    owner = that.owner;
    watermark.store(that.watermark.load());
    originId.store(that.originId.load());
    return *this;
}

MemorySegment* BufferControlBlock::getOwner() {
    return owner;
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
    uint32_t expected = 0;
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    // store the current thread that owns the buffer and track which function obtained the buffer
    std::unique_lock lock(owningThreadsMutex);
    ThreadOwnershipInfo info;
    fillThreadOwnershipInfo(info.threadName, info.callstack);
    owningThreads[std::this_thread::get_id()].emplace_back(info);
#endif
    return referenceCounter.compare_exchange_strong(expected, 1);
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
    NES_ERROR("Buffer " << getOwner() << " has " << referenceCounter.load() << " live references");
    for (auto& item : owningThreads) {
        for (auto& v : item.second) {
            NES_ERROR("Thread " << v.threadName << " has buffer " << getOwner()
                                << " requested on callstack: " << v.callstack);
        }
    }
}
#endif

uint32_t BufferControlBlock::getReferenceCount() {
    return referenceCounter.load();
}

bool BufferControlBlock::release() {
    uint32_t prevRefCnt;
    if ((prevRefCnt = referenceCounter.fetch_sub(1)) == 1) {
        numberOfTuples.store(0);
        recycleCallback(owner);
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

size_t BufferControlBlock::getNumberOfTuples() const {
    return numberOfTuples;
}

void BufferControlBlock::setNumberOfTuples(size_t numberOfTuples) {
    this->numberOfTuples = numberOfTuples;
}

int64_t BufferControlBlock::getWatermark() const {
    return watermark;
}

void BufferControlBlock::setWatermark(int64_t watermark) {
    this->watermark = watermark;
}
const uint64_t BufferControlBlock::getOriginId() const {
    return originId;
}
void BufferControlBlock::setOriginId(uint64_t originId) {
    this->originId = originId;
}

#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
BufferControlBlock::ThreadOwnershipInfo::ThreadOwnershipInfo(std::string&& threadName, std::string&& callstack)
    : threadName(threadName), callstack(callstack) {
    // nop
}

BufferControlBlock::ThreadOwnershipInfo::ThreadOwnershipInfo()
    : threadName("NOT-SAMPLED"), callstack("NOT-SAMPLED") {
    // nop
}
#endif

// -----------------------------------------------------------------------------
// ------------------ Utility functions for TupleBuffer ------------------------
// -----------------------------------------------------------------------------

/** possible types are:
 *  INT8,UINT8,INT16,UINT16,INT32,UINT32,INT64,FLOAT32,UINT64,FLOAT64,BOOLEAN,CHAR,DATE,VOID_TYPE
 *  Code for debugging:
 *  std::bitset<16> x(*orgVal);
 *  std::cout << "16-BEFORE biseq=" << x << " orgVal=" << *orgVal << endl;
 *  u_int16_t val = boost::endian::endian_reverse(*orgVal);
 *  std::bitset<16> x2(val);
 *  std::cout << "16-After biseq=" << x2 << " val=" << val << endl;
 *
 *  cout << "buffer=" << buffer << " offset=" << offset << " i=" << i
 *            << " tupleSize=" << tupleSize << " fieldSize=" << fieldSize
 *            << " res=" << (char*)buffer + offset + i * tupleSize << endl;
 */
void revertEndianness(TupleBuffer& tbuffer, SchemaPtr schema) {
    auto tupleSize = schema->getSchemaSizeInBytes();
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto buffer = tbuffer.getBufferAs<char>();
    auto physicalDataFactory = DefaultPhysicalTypeFactory();
    for (size_t i = 0; i < numberOfTuples; i++) {
        size_t offset = 0;
        for (size_t j = 0; j < schema->getSize(); j++) {
            auto field = schema->get(j);
            auto physicalField = physicalDataFactory.getPhysicalType(field->dataType);
            size_t fieldSize = physicalField->size();
            //TODO: add enum with switch for performance reasons
            if (physicalField->toString() == "UINT8") {
                u_int8_t* orgVal = (u_int8_t*) buffer + offset + i * tupleSize;
                memcpy((char*) buffer + offset + i * tupleSize, orgVal, fieldSize);
            } else if (physicalField->toString() == "UINT16") {
                u_int16_t* orgVal = (u_int16_t*) ((char*) buffer + offset
                                                  + i * tupleSize);
                u_int16_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
            } else if (physicalField->toString() == "UINT32") {
                uint32_t* orgVal = (uint32_t*) ((char*) buffer + offset + i * tupleSize);
                uint32_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
            } else if (physicalField->toString() == "UINT64") {
                uint64_t* orgVal = (uint64_t*) ((char*) buffer + offset + i * tupleSize);
                uint64_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
            } else if (physicalField->toString() == "INT8") {
                int8_t* orgVal = (int8_t*) buffer + offset + i * tupleSize;
                int8_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
            } else if (physicalField->toString() == "INT16") {
                int16_t* orgVal = (int16_t*) ((char*) buffer + offset + i * tupleSize);
                int16_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
            } else if (physicalField->toString() == "INT32") {
                int32_t* orgVal = (int32_t*) ((char*) buffer + offset + i * tupleSize);
                int32_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
            } else if (physicalField->toString() == "INT64") {
                int64_t* orgVal = (int64_t*) ((char*) buffer + offset + i * tupleSize);
                int64_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
            } else if (physicalField->toString() == "FLOAT32") {
                NES_WARNING(
                    "TupleBuffer::revertEndianness: float conversation is not totally supported, please check results");
                uint32_t* orgVal = (uint32_t*) ((char*) buffer + offset + i * tupleSize);
                uint32_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
            } else if (physicalField->toString() == "FLOAT64") {
                NES_WARNING(
                    "TupleBuffer::revertEndianness: double conversation is not totally supported, please check results");
                uint64_t* orgVal = (uint64_t*) ((char*) buffer + offset + i * tupleSize);
                uint64_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
            } else if (physicalField->toString() == "CHAR") {
                //TODO: I am not sure if we have to convert char at all because it is one byte only
                throw new Exception(
                    "Data type float is currently not supported for endian conversation");
            } else {
                throw new Exception(
                    "Data type " + field->getDataType()->toString()
                    + " is currently not supported for endian conversation");
            }
            offset += fieldSize;
        }
    }
}

void zmqBufferRecyclingCallback(void* ptr, void* hint) {
    auto bufferSize = reinterpret_cast<uintptr_t>(hint);
    auto buffer = reinterpret_cast<uint8_t*>(ptr);
    auto controlBlock = reinterpret_cast<BufferControlBlock*>(buffer + bufferSize);
    controlBlock->release();
}

}// namespace detail

}// namespace NES