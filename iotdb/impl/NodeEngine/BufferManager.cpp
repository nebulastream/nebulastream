#include <NodeEngine/BufferManager.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <utility>
#include <Util/Logger.hpp>

namespace NES {

BufferManager::BufferManager()
    :
    changeBufferMutex(),
    resizeMutex(),
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
  std::cout << "buffer size=" << fixSizeBufferPool.size() << std::endl;
  for (auto& entry : fixSizeBufferPool) {
    entry.second == false;
    delete[] (char*) entry.first->getBuffer();
  }
  fixSizeBufferPool.clear();
  assert(fixSizeBufferPool.size() == 0);
}

void BufferManager::reset() {
  clearFixBufferPool();
}

void BufferManager::resizeFixBufferSize(size_t newBufferSizeInByte) {
  std::unique_lock < std::mutex > lock(resizeMutex);
  size_t bufferCnt = fixSizeBufferPool.size();
  clearFixBufferPool();

  currentBufferSize = newBufferSizeInByte;
  for (size_t i = 0; i < bufferCnt; i++) {
    addOneBufferWithFixSize();
  }
}

void BufferManager::resizeFixBufferCnt(size_t newBufferCnt) {
  std::unique_lock < std::mutex > lock(resizeMutex);
  clearFixBufferPool();

  for (size_t i = 0; i < newBufferCnt; i++) {
    addOneBufferWithFixSize();
  }
}

size_t BufferManager::getFixBufferSize() {
  return currentBufferSize;
}

void BufferManager::addOneBufferWithFixSize() {
  std::unique_lock < std::mutex > lock(changeBufferMutex);

  char* buffer = new char[currentBufferSize];
  TupleBufferPtr buff = std::make_shared<TupleBuffer>(buffer, currentBufferSize,
  /**tupleSizeBytes*/0, /**numTuples*/
                                                      0, /**fixSizeBuffer*/
                                                      true);
  fixSizeBufferPool.emplace(std::piecewise_construct,
                            std::forward_as_tuple(buff),
                            std::forward_as_tuple(false));

}

TupleBufferPtr BufferManager::addOneBufferWithVarSize(size_t bufferSizeInByte) {
  std::unique_lock < std::mutex > lock(changeBufferMutex);

  char* buffer = new char[bufferSizeInByte];
  TupleBufferPtr buff = std::make_shared<TupleBuffer>(buffer, bufferSizeInByte,
  /**tupleSizeBytes*/0, /**numTuples*/
                                                      0, /**fixSizeBuffer*/
                                                      false);
  varSizeBufferPool.emplace(std::piecewise_construct,
                            std::forward_as_tuple(buff),
                            std::forward_as_tuple(false));

  return buff;
}

bool BufferManager::removeBuffer(TupleBufferPtr tupleBuffer) {
  std::unique_lock < std::mutex > lock(changeBufferMutex);

  std::map<TupleBufferPtr, /**used*/std::atomic<bool>>::iterator it;
  std::map<TupleBufferPtr, /**used*/std::atomic<bool>>::iterator end;

  if (tupleBuffer->getFixSizeBuffer()) {
    it = fixSizeBufferPool.begin();
    end = fixSizeBufferPool.end();
  } else {
    it = varSizeBufferPool.begin();
    end = varSizeBufferPool.end();
  }

  for (; it != end; it++) {
    if (it->first.get() == tupleBuffer.get()) {
      if (it->second == true) {
        NES_DEBUG(
            "BufferManager: could not remove Buffer buffer because it is in use" << tupleBuffer)
        return false;
      }

      delete (char*) it->first->getBuffer();
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

TupleBufferPtr BufferManager::getVarSizeBufferLargerThan(
    size_t bufferSizeInByte) {
  size_t tryCnt = 0;
  //find a free buffer
  for (auto& entry : varSizeBufferPool) {
    bool used = false;
    if (entry.second == false
        && entry.first->getBufferSizeInBytes() > bufferSizeInByte) {
      NES_ERROR("BufferManager: found fitting var size buffer try to reserve")
      if (entry.second.compare_exchange_weak(used, true)) {
        providedBuffer++;
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

size_t BufferManager::getNumberOfFixBuffers() {
  return fixSizeBufferPool.size();
}

size_t BufferManager::getNumberOfFreeFixBuffers() {
  size_t result = 0;
  for (auto& entry : fixSizeBufferPool) {
    if (!entry.second)
      result++;
  }
  return result;
}

size_t BufferManager::getNumberOfVarBuffers() {
  return varSizeBufferPool.size();
}

size_t BufferManager::getNumberOfFreeVarBuffers() {
  size_t result = 0;
  for (auto& entry : varSizeBufferPool) {
    if (!entry.second)
      result++;
  }
  return result;
}

bool BufferManager::releaseBuffer(const TupleBufferPtr tupleBuffer) {
  std::unique_lock < std::mutex > lock(changeBufferMutex);
  //TODO: do we really need this or can we solve it by a cas?

  std::map<TupleBufferPtr, /**used*/std::atomic<bool>>::iterator it;
  std::map<TupleBufferPtr, /**used*/std::atomic<bool>>::iterator end;

  if (tupleBuffer->getFixSizeBuffer()) {
    it = fixSizeBufferPool.begin();
    end = fixSizeBufferPool.end();
  } else {
    it = varSizeBufferPool.begin();
    end = varSizeBufferPool.end();
  }

  for (; it != end; it++) {
    if (it->first.get() == tupleBuffer.get()) {  //found entry
      it->second = false;
      NES_DEBUG(
          "BufferManager: found buffer with useCnt " << it->first->getUseCnt())
      if (it->first->decrementUseCntAndTestForZero()) {

        NES_DEBUG("BufferManager: release buffer as useCnt gets zero")
        //reset buffer for next use
        it->first->setNumberOfTuples(0);
        it->first->setTupleSizeInBytes(0);
        //update statistics
        releasedBuffer++;
      } else {
        NES_DEBUG(
            "BufferManager: Dont release buffer as useCnt gets " << it->first->getUseCnt())
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
