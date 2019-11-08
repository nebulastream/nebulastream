#include <NodeEngine/BufferManager.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <utility>
#include <Util/Logger.hpp>

namespace iotdb {

BufferManager::BufferManager()
    : mutex(),
      noFreeBuffer(0),
      providedBuffer(0),
      releasedBuffer(0) {

  size_t initalBufferCnt = 1000;
  bufferSizeInByte = 4 * 1024;
  IOTDB_DEBUG(
      "BufferManager: Set maximum number of buffer to " << initalBufferCnt << " and a bufferSize of KB:" << bufferSizeInByte / 1024)

  IOTDB_DEBUG("BufferManager: initialize buffers")
  for (size_t i = 0; i < initalBufferCnt; i++) {
    addOneBufferWithDefaultSize();
  }
}

BufferManager::~BufferManager() {
  IOTDB_DEBUG("BufferManager: Enter Destructor of BufferManager.")

  // Release memory.
  for (auto const &buffer_pool_entry : buffer_pool) {
    delete[] (char *) buffer_pool_entry.first->buffer;
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
    delete[] (char *) entry.first->buffer;
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
    delete[] (char *) entry.first->buffer;
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
        IOTDB_DEBUG(
            "BufferManager: could not remove Buffer buffer because it is in use" << tuple_buffer)
        return false;
      }

      delete (char *) entry.first->buffer;
      buffer_pool.erase(tuple_buffer);
      IOTDB_DEBUG(
          "BufferManager: found and remove Buffer buffer" << tuple_buffer)
      return true;
    }
  }
  IOTDB_DEBUG(
      "BufferManager: could not remove buffer, buffer not found" << tuple_buffer)
  return false;
}

TupleBufferPtr BufferManager::getBuffer() {

  while (true) {
    //find a free buffer
    for (auto &entry : buffer_pool) {
      bool used = false;
      if (entry.second.compare_exchange_weak(used, true)) {
        providedBuffer++;
        IOTDB_DEBUG(
            "BufferManager: getBuffer() provide free buffer" << entry.first)
        return entry.first;
      }
    }
    //add wait
    noFreeBuffer++;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    IOTDB_DEBUG("BufferManager: no buffer free yet --- retry")
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
      IOTDB_DEBUG("BufferManager: found and release buffer")

      //reset buffer for next use
      entry.first->num_tuples = 0;
      entry.first->tuple_size_bytes = 0;

      //update statistics
      releasedBuffer++;

      return true;
    }
  }
  IOTDB_ERROR("BufferManager: buffer not found")
  return false;
}

void BufferManager::printStatistics() {
  IOTDB_INFO("BufferManager Statistics:")
  IOTDB_INFO("\t noFreeBuffer=" << noFreeBuffer)
  IOTDB_INFO("\t providedBuffer=" << providedBuffer)
  IOTDB_INFO("\t releasedBuffer=" << releasedBuffer)
}

}  // namespace iotdb
