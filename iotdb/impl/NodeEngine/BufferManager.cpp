#include <NodeEngine/BufferManager.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <utility>
#include <Util/Logger.hpp>

namespace NES {

BufferManager::BufferManager()
    :
    mutex(),
    noFreeBuffer(0),
    providedBuffer(0),
    releasedBuffer(0),
    maxNumberOfRetry(10000) {

  size_t initalBufferCnt = 1000;
  currentBufferSize = 4 * 1024;
  NES_DEBUG(
      "BufferManager: Set maximum number of buffer to " << initalBufferCnt << " and a bufferSize of KB:" << currentBufferSize / 1024)

  NES_DEBUG("BufferManager: initialize buffers")
  for (size_t i = 0; i < initalBufferCnt; i++) {
    addOneBufferWithFixSize();
  }
}

BufferManager::~BufferManager() {
  NES_DEBUG("BufferManager: Enter Destructor of BufferManager.")

  // Release memory.
  for (auto const& buffer_pool_entry : fixSizeBufferPool) {
    delete[] (char*) buffer_pool_entry.first->getBuffer();
  }
  fixSizeBufferPool.clear();
}

BufferManager& BufferManager::instance() {
  static BufferManager instance;
  return instance;
}

void BufferManager::clearFixBufferPool() {
  //delete all existing buffers
  for (auto& entry : fixSizeBufferPool) {
    //TODO: we have to make sure that no buffer is currently used
    assert(entry.second == false);
    delete[] (char*) entry.first->getBuffer();
  }
  fixSizeBufferPool.clear();
}

void BufferManager::resizeFixBufferSize(size_t newBufferSizeInByte) {
  std::unique_lock<std::mutex> lock(mutex);
  size_t bufferCnt = fixSizeBufferPool.size();
  clearFixBufferPool();

  currentBufferSize = newBufferSizeInByte;
  for (size_t i = 0; i < bufferCnt; i++) {
    addOneBufferWithFixSize();
  }
}

void BufferManager::resizeFixBufferCnt(size_t newBufferCnt) {
  std::unique_lock<std::mutex> lock(mutex);
  clearFixBufferPool();

  for (size_t i = 0; i < newBufferCnt; i++) {
    addOneBufferWithFixSize();
  }
}

size_t BufferManager::getFixBufferSize() {
  return currentBufferSize;
}

void BufferManager::addOneBufferWithFixSize() {
  std::unique_lock<std::mutex> lock(mutex);

  char* buffer = new char[currentBufferSize];
  TupleBufferPtr buff = std::make_shared<TupleBuffer>(buffer, currentBufferSize,
  /**tupleSizeBytes*/0, /**numTuples*/
                                                      0, /**fixSizeBuffer*/
                                                      true);
  fixSizeBufferPool.emplace(std::piecewise_construct,
                            std::forward_as_tuple(buff),
                            std::forward_as_tuple(false));

}

void BufferManager::addOneBufferWithVarSize(size_t bufferSizeInByte) {
  std::unique_lock<std::mutex> lock(mutex);

  char* buffer = new char[bufferSizeInByte];
  TupleBufferPtr buff = std::make_shared<TupleBuffer>(buffer, bufferSizeInByte,
  /**tupleSizeBytes*/0, /**numTuples*/
                                                      0, /**fixSizeBuffer*/
                                                      false);
  varSizeBufferPool.emplace(std::piecewise_construct,
                            std::forward_as_tuple(buff),
                            std::forward_as_tuple(false));
}

bool BufferManager::removeBuffer(TupleBufferPtr tupleBuffer) {
  std::unique_lock<std::mutex> lock(mutex);

  std::map<TupleBufferPtr, /**used*/std::atomic<bool>>* buffer;
  if (tupleBuffer->fixSizeBuffer == true) {
    buffer = fixSizeBufferPool;
  } else {
    buffer = varSizeBufferPool;
  }

  for (auto& entry : buffer) {
    if (entry.first.get() == tupleBuffer.get()) {
      if (entry.second == true) {
        NES_DEBUG(
            "BufferManager: could not remove Buffer buffer because it is in use" << tupleBuffer)
        return false;
      }

      delete (char*) entry.first->getBuffer();
      fixSizeBufferPool.erase(tupleBuffer);
      NES_DEBUG("BufferManager: found and remove Buffer buffer" << tupleBuffer)
      return true;
    }
  }
  NES_DEBUG(
      "BufferManager: could not remove buffer, buffer not found" << tupleBuffer)
  return false;
}

TupleBufferPtr BufferManager::getFixSizeBuffer() {

  size_t tryCnt = 0;
  while (true) {
    //find a free buffer
    for (auto& entry : fixSizeBufferPool) {
      bool used = false;
      if (entry.second.compare_exchange_weak(used, true)) {
        providedBuffer++;
        NES_DEBUG(
            "BufferManager: getBuffer() provide free buffer" << entry.first)
        entry.first->incrementUseCnt();
        return entry.first;
      }
    }
    //add wait
    noFreeBuffer++;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    NES_DEBUG("BufferManager: no buffer free yet --- retry " << tryCnt++)
    if (tryCnt >= maxNumberOfRetry) {
      NES_ERROR("BufferManager: break because no buffer could be found")
      throw new Exception("BufferManager failed");
    }
  }
  return nullptr;
}

TupleBufferPtr BufferManager::createVarSizeBuffer(size_t bufferSizeInByte) {
  addOneBufferWithVarSize(bufferSizeInByte);
}

size_t BufferManager::getNumberOfBuffers() {
  return fixSizeBufferPool.size();
}

size_t BufferManager::getNumberOfFreeBuffers() {
  size_t result = 0;
  for (auto& entry : fixSizeBufferPool) {
    if (!entry.second)
      result++;
  }
  return result;
}

bool BufferManager::releaseBuffer(const TupleBufferPtr tupleBuffer) {
  std::unique_lock<std::mutex> lock(mutex);
  //TODO: do we really need this or can we solve it by a cas?

  for (auto& entry : fixSizeBufferPool) {
    if (entry.first.get() == tupleBuffer.get()) {  //found entry
      entry.second = false;
      NES_DEBUG(
          "BufferManager: found buffer with useCnt " << entry.first->getUseCnt())

      if (entry.first->decrementUseCntAndTestForZero()) {

        NES_DEBUG("BufferManager: release buffer as useCnt gets zero")
        //reset buffer for next use
        entry.first->setNumberOfTuples(0);
        entry.first->setTupleSizeInBytes(0);
        //update statistics
        releasedBuffer++;
      } else {
        NES_DEBUG(
            "BufferManager: Dont release buffer as useCnt gets " << entry.first->getUseCnt())
      }

      return true;
    }
  }
  NES_ERROR("BufferManager: buffer not found")
  return false;
}

void BufferManager::printStatistics() {
  NES_INFO("BufferManager Statistics:")
  NES_INFO("\t noFreeBuffer=" << noFreeBuffer)
  NES_INFO("\t providedBuffer=" << providedBuffer)
  NES_INFO("\t releasedBuffer=" << releasedBuffer)
}

}  // namespace NES
