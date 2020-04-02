#include <NodeEngine/BufferManager.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <utility>
#include <Util/Logger.hpp>
#include <NodeEngine/TupleBuffer.hpp>

namespace NES {
#ifndef USE_NEW_BUFFER_MANAGEMENT

BufferManager::BufferManager()
    :
    numberOfFreeVarSizeBuffers(0),
    changeBufferMutex(),
    resizeMutex() {
  size_t initalBufferCnt = 100;
  currentBufferSize = 4 * 1024;
  NES_DEBUG(
      "BufferManager: Set maximum number of buffer to " << initalBufferCnt << " and a bufferSize of KB:" << currentBufferSize / 1024)

  fixedSizeBufferPool = new BlockingQueue<TupleBufferPtr>(initalBufferCnt);
  NES_DEBUG("BufferManager: initialize buffers")
  for (size_t i = 0; i < initalBufferCnt; i++) {
    addOneBufferWithFixedSize();
  }
}

BufferManager::~BufferManager() {
  NES_DEBUG("BufferManager: Enter Destructor of BufferManager.")
  //Buffer themseleves are deallocated when shared pointer decrement to zero usage
  fixedSizeBufferPool->reset();
}

BufferManager& BufferManager::instance() {
  static BufferManager instance;
  return instance;
}

void BufferManager::clearFixBufferPool() {
//delete all existing buffers
  NES_DEBUG(
      "BufferManager: clearFixBufferPool of size" << fixedSizeBufferPool->size())
  fixedSizeBufferPool->reset();
  if (fixedSizeBufferPool->size() != 0) {
    NES_ERROR(
        "BufferManager::clearFixBufferPool: fixedSizeBufferPool still has elements after reset")
    throw new Exception("Error during clearFixBufferPool");
  }

  if (!fixedSizeBufferPool->empty()) {
    NES_ERROR(
        "BufferManager::clearFixBufferPool: fixedSizeBufferPool not empty after reset")
    throw new Exception("Error during clearFixBufferPool");
  }

}

void BufferManager::clearVarBufferPool() {
//delete all existing buffers
  NES_DEBUG(
      "BufferManager: clearVarBufferPool of size" << varSizeBufferPool.size())

  numberOfFreeVarSizeBuffers = 0;
  varSizeBufferPool.clear();
  if (varSizeBufferPool.size() != 0) {
    NES_ERROR(
        "BufferManager::clearVarBufferPool: fixedSizeBufferPool still has elements after reset")
    throw new Exception("Error during clearVarBufferPool");
  }

  if (!varSizeBufferPool.empty()) {
    NES_ERROR(
        "BufferManager::clearFixBufferPool: clearVarBufferPool not empty after reset")
    throw new Exception("Error during clearFixBclearVarBufferPoolufferPool");
  }
}

void BufferManager::reset() {
  NES_DEBUG("BufferManager: reset")
  clearFixBufferPool();
  clearVarBufferPool();
}

void BufferManager::resizeFixedBufferSize(size_t newBufferSizeInByte) {
  std::unique_lock<std::mutex> lock(resizeMutex);
  size_t bufferCnt = fixedSizeBufferPool->size();
  clearFixBufferPool();

  currentBufferSize = newBufferSizeInByte;
  for (size_t i = 0; i < bufferCnt; i++) {
    addOneBufferWithFixedSize();
  }
}

void BufferManager::resizeFixedBufferCnt(size_t newBufferCnt) {
  NES_DEBUG("BufferManager: resizeFixBufferCnt to size " << newBufferCnt)
  std::unique_lock<std::mutex> lock(resizeMutex);
  clearFixBufferPool();
  fixedSizeBufferPool->setCapacity(newBufferCnt);
  for (size_t i = 0; i < newBufferCnt; i++) {
    addOneBufferWithFixedSize();
  }
}

size_t BufferManager::getFixedBufferSize() {
  return currentBufferSize;
}

void BufferManager::addOneBufferWithFixedSize() {
  std::unique_lock<std::mutex> lock(changeBufferMutex);

  char* buffer = new char[currentBufferSize];
  TupleBufferPtr buff = std::make_shared<TupleBuffer>(buffer, currentBufferSize,
  /**tupleSizeBytes*/0, /**numTuples*/
                                                      0);
  fixedSizeBufferPool->push(buff);
}

TupleBufferPtr BufferManager::addOneBufferWithVarSize(size_t bufferSizeInByte) {
  std::unique_lock<std::mutex> lock(changeBufferMutex);

  char* buffer = new char[bufferSizeInByte];
  TupleBufferPtr buff = std::make_shared<TupleBuffer>(buffer, bufferSizeInByte,
  /**tupleSizeBytes*/0, /**numTuples*/
                                                      0);
  buff->incrementUseCnt();
  varSizeBufferPool.emplace(std::piecewise_construct,
                            std::forward_as_tuple(buff),
                            std::forward_as_tuple(false));

  return buff;
}

bool BufferManager::releaseFixedSizeBuffer(const TupleBufferPtr tupleBuffer) {
  std::unique_lock<std::mutex> lock(changeBufferMutex);
//TODO: do we really need this or can we solve it by a cas?

  if (!tupleBuffer) {
    NES_ERROR(
        "BufferManager::releaseFixedSizeBuffer: error release of nullptr buffer")
    throw new Exception("BufferManager failed");
  }

  //enqueue buffer back to queue
  NES_DEBUG("BufferManager::releaseBuffer: release buffer " << tupleBuffer)
  if (tupleBuffer->decrementUseCntAndTestForZero()) {
    NES_DEBUG("BufferManager: release buffer as useCnt gets zero")
    //reset buffer for next use
    tupleBuffer->setNumberOfTuples(0);
    tupleBuffer->setTupleSizeInBytes(0);
    //update statistics
    fixedSizeBufferPool->push(tupleBuffer);

    return true;
  } else {
    NES_DEBUG(
        "BufferManager: Dont release buffer as useCnt gets " << tupleBuffer->getUseCnt())
  }

}

bool BufferManager::releaseVarSizedBuffer(const TupleBufferPtr tupleBuffer) {
  std::unique_lock<std::mutex> lock(changeBufferMutex);
//TODO: do we really need this or can we solve it by a cas?

  if (!tupleBuffer) {
    NES_ERROR("BufferManager::releaseBuffer: error release of nullptr buffer")
    throw new Exception("BufferManager failed");
  }
  std::map<TupleBufferPtr, /**used*/std::atomic<bool>>::iterator it;
  std::map<TupleBufferPtr, /**used*/std::atomic<bool>>::iterator end;

  it = varSizeBufferPool.begin();
  end = varSizeBufferPool.end();

  for (; it != end; it++) {
    if (it->first.get() == tupleBuffer.get()) {  //found entry
      NES_DEBUG(
          "BufferManager: found buffer with useCnt " << it->first->getUseCnt())
      if (it->first->decrementUseCntAndTestForZero()) {

        NES_DEBUG("BufferManager: release buffer as useCnt gets zero")
        //reset buffer for next use
        it->first->setNumberOfTuples(0);
        it->first->setTupleSizeInBytes(0);
        it->second = false;
      } else {
        NES_DEBUG(
            "BufferManager: Dont release buffer as useCnt gets " << it->first->getUseCnt())
      }
      numberOfFreeVarSizeBuffers++;

      return true;
    }
  }
  NES_ERROR("BufferManager: buffer not found")
  return false;
}

TupleBufferPtr BufferManager::getFixedSizeBuffer() {
  //this call is blocking
  return fixedSizeBufferPool->pop();
}

TupleBufferPtr BufferManager::getFixedSizeBufferWithTimeout(size_t timeout_ms) {
  auto buff = fixedSizeBufferPool->popTimeout(timeout_ms);
  return buff.value_or(nullptr);
}

TupleBufferPtr BufferManager::getVarSizeBufferLargerThan(
    size_t bufferSizeInByte) {
  //find a free buffer
  for (auto& entry : varSizeBufferPool) {
    bool used = false;
    if (entry.second == false
        && entry.first->getBufferSizeInBytes() > bufferSizeInByte) {
      NES_DEBUG("BufferManager: found fitting var size buffer try to reserve")
      if (entry.second.compare_exchange_weak(used, true)) {
        NES_DEBUG(
            "BufferManager: getBuffer() provide free buffer" << entry.first)
        entry.first->incrementUseCnt();
        return entry.first;
      }
    }
  }  //end of for
//no fitting buffer found
  return nullptr;
}

TupleBufferPtr BufferManager::createVarSizeBuffer(size_t bufferSizeInByte) {
  return addOneBufferWithVarSize(bufferSizeInByte);
}

size_t BufferManager::getNumberOfFixedBuffers() {
  return fixedSizeBufferPool->getCapacity();
}

size_t BufferManager::getNumberOfFreeFixedBuffers() {
  return fixedSizeBufferPool->size();
}

size_t BufferManager::getNumberOfVarBuffers() {
  return varSizeBufferPool.size();
}

size_t BufferManager::getNumberOfFreeVarBuffers() {
  return numberOfFreeVarSizeBuffers;
}

void BufferManager::printStatistics() {
  NES_INFO("BufferManager Statistics:")
}
#else

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

#endif
}  // namespace NES
