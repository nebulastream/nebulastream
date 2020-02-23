#include <NodeEngine/BufferManager.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <utility>
#include <Util/Logger.hpp>

namespace NES {

BufferManager::BufferManager()
    : mutex(),
      noFreeBuffer(0),
      providedBuffer(0),
      releasedBuffer(0),
      maxNumberOfRetry(10){

  size_t initalBufferCnt = 1000;
  bufferSizeInByte = 4 * 1024;
  NES_DEBUG(
      "BufferManager: Set maximum number of buffer to " << initalBufferCnt << " and a bufferSize of KB:" << bufferSizeInByte / 1024)

  NES_DEBUG("BufferManager: initialize buffers")
  for (size_t i = 0; i < initalBufferCnt; i++) {
    addOneBufferWithDefaultSize();
  }
}

BufferManager::~BufferManager() {
  NES_DEBUG("BufferManager: Enter Destructor of BufferManager.")

  // Release memory.
  for (auto const &buffer_pool_entry : buffer_pool) {
    delete[] (char *) buffer_pool_entry.first->getBuffer();
  }
  buffer_pool.clear();
}

BufferManager &BufferManager::instance() {
  static BufferManager instance;
  return instance;
}

void BufferManager::setNumberOfBuffers(size_t n) {
//TODO: this should be somehow be protected

//delete all existing buffers
  for (auto &entry : buffer_pool) {
    delete[] (char *) entry.first->getBuffer();
  }
  buffer_pool.clear();

  //add n new buffer
  for (size_t i = 0; i < n; i++) {
    addOneBufferWithDefaultSize();
  }
}

void BufferManager::setBufferSize(size_t size) {
  //TODO: this should be somehow be protected
  size_t tmpBufferCnt = buffer_pool.size();
  //delete all existing buffers
  for (auto &entry : buffer_pool) {
    delete[] (char *) entry.first->getBuffer();
  }
  buffer_pool.clear();

  //recreate buffer with new size
  bufferSizeInByte = size;
  for (size_t i = 0; i < tmpBufferCnt; i++) {
    addOneBufferWithDefaultSize();
  }
}

size_t BufferManager::getBufferSize() {
  return bufferSizeInByte;
}

void BufferManager::addOneBufferWithDefaultSize() {
  std::unique_lock<std::mutex> lock(mutex);

  char* buffer = new char[bufferSizeInByte];
  TupleBufferPtr buff = std::make_shared<TupleBuffer>(buffer, bufferSizeInByte,
                                                      0, 0);
  buffer_pool.emplace(std::piecewise_construct, std::forward_as_tuple(buff),
                      std::forward_as_tuple(false));

}

bool BufferManager::removeBuffer(TupleBufferPtr tuple_buffer) {
  std::unique_lock<std::mutex> lock(mutex);

  for (auto &entry : buffer_pool) {
    if (entry.first.get() == tuple_buffer.get()) {
      if (entry.second == true) {
        NES_DEBUG(
            "BufferManager: could not remove Buffer buffer because it is in use" << tuple_buffer)
        return false;
      }

      delete (char *) entry.first->getBuffer();
      buffer_pool.erase(tuple_buffer);
      NES_DEBUG(
          "BufferManager: found and remove Buffer buffer" << tuple_buffer)
      return true;
    }
  }
  NES_DEBUG(
      "BufferManager: could not remove buffer, buffer not found" << tuple_buffer)
  return false;
}

TupleBufferPtr BufferManager::getBuffer() {

  size_t tryCnt = 0;
  while (true) {
    //find a free buffer
    for (auto &entry : buffer_pool) {
      bool used = false;
      if (entry.second.compare_exchange_weak(used, true)) {
        providedBuffer++;
        NES_DEBUG(
            "BufferManager: getBuffer() provide free buffer" << entry.first)
        return entry.first;
      }
    }
    //add wait
    noFreeBuffer++;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    NES_DEBUG("BufferManager: no buffer free yet --- retry " << tryCnt++)
    if(tryCnt >= maxNumberOfRetry)
    {
      NES_ERROR("BufferManager: break because no buffer could be found")
          break;
    }
  }
}

size_t BufferManager::getNumberOfBuffers() {
  return buffer_pool.size();
}

size_t BufferManager::getNumberOfFreeBuffers() {
  size_t result = 0;
  for (auto &entry : buffer_pool) {
    if (!entry.second)
      result++;
  }
  return result;
}

bool BufferManager::releaseBuffer(const TupleBufferPtr tuple_buffer) {
  std::unique_lock<std::mutex> lock(mutex);
  //TODO: do we really need this or can we solve it by a cas?

  for (auto &entry : buffer_pool) {
    if (entry.first.get() == tuple_buffer.get()) {  //found entry
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
      }
      else
      {
        NES_DEBUG("BufferManager: Dont release buffer as useCnt gets " << entry.first->getUseCnt())
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
