#include <Runtime/BufferManager.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <utility>

namespace iotdb {

BufferManager::BufferManager() { std::cout << "Enter Constructor of BufferManager." << std::endl; }
BufferManager::~BufferManager() {
  std::cout << "Enter Destructor of BufferManager." << std::endl;

  // Release memory.
  for (auto const &buffer_pool_entry : buffer_pool) {
    for (size_t i = 0; i != buffer_pool_entry.second.size(); ++i) {
#if USE_LOCK_IMPL
      delete[] buffer_pool_entry.second.at(i);
#elif USE_CAS_IMPL
      delete[] buffer_pool_entry.second.at(i).buffer;
#endif
    }
  }

  buffer_pool.clear();
}

BufferManager &BufferManager::instance() {
  static BufferManager instance;
  return instance;
}

void BufferManager::addBuffers(const size_t number_of_buffers, const size_t buffer_size_bytes) {
#if USE_LOCK_IMPL
  addBuffersByLock(number_of_buffers, buffer_size_bytes);
#elif USE_CAS_IMPL
  addBuffersByCAS(number_of_buffers, buffer_size_bytes);
#endif
}

TupleBuffer BufferManager::getBuffer(const uint32_t number_of_tuples, const uint32_t tuple_size_bytes) {
#if USE_LOCK_IMPL
  return getBufferByLock(number_of_tuples, tuple_size_bytes);
#elif USE_CAS_IMPL
  return getBufferByCAS(number_of_tuples, tuple_size_bytes);
#endif
}

void BufferManager::releaseBuffer(TupleBuffer tuple_buffer) {
#if USE_LOCK_IMPL
  releaseBufferByLock(tuple_buffer);
#elif USE_CAS_IMPL
  releaseBufferByCAS(tuple_buffer);
#endif
}

#if USE_LOCK_IMPL

void BufferManager::addBuffersByLock(const size_t number_of_buffers, const size_t buffer_size_bytes) {
  std::vector<char *> tmp_buffers(number_of_buffers);
  for (size_t i = 0; i != number_of_buffers; ++i) {
    tmp_buffers[i] = new char[buffer_size_bytes];
  }
  buffer_pool.insert(std::make_pair(buffer_size_bytes, tmp_buffers));
}

TupleBuffer BufferManager::getBufferByLock(const uint32_t number_of_tuples, const uint32_t tuple_size_bytes) {

  // Find buffer of that size
  auto buffer_size_bytes = number_of_tuples * tuple_size_bytes;
  auto found = buffer_pool.find(buffer_size_bytes);
  if (found == buffer_pool.cend()) {
    std::cout << "No buffer of size " << buffer_size_bytes << " found." << std::endl;
    std::cout << "Create 10 new buffers of size " << buffer_size_bytes << " bytes." << std::endl;
    addBuffersByLock(10, buffer_size_bytes);
    found = buffer_pool.find(buffer_size_bytes);
  }
  assert(found != buffer_pool.cend());

  // Acquire lock and wait for buffer ready
  auto &buffers = found->second;
  std::unique_lock<std::mutex> lock(mutex);
  while (buffers.empty()) {
    cv.wait(lock);
  }
  assert(!buffers.empty());

  // Prepare TupleBuffer to return
  auto new_tuple_buffer = TupleBuffer((void *)buffers.back(), buffer_size_bytes, tuple_size_bytes, number_of_tuples);
  buffers.pop_back();

  cv.notify_all();
  return new_tuple_buffer;
}

void BufferManager::releaseBufferByLock(TupleBuffer tuple_buffer) {

  // Find buffer of that size
  auto found = buffer_pool.find(tuple_buffer.buffer_size);
  assert(found != buffer_pool.cend());

  // Add buffer again to manager
  auto &buffers = found->second;
  std::unique_lock<std::mutex> lock(mutex);
  buffers.push_back((char *)tuple_buffer.buffer);

  cv.notify_all();
}

#elif USE_CAS_IMPL

void BufferManager::addBuffersByCAS(const size_t number_of_buffers, const size_t buffer_size_bytes) {
  std::vector<BufferEntry> tmp_buffers(number_of_buffers);
  for (size_t i = 0; i != number_of_buffers; ++i) {
    tmp_buffers[i].buffer = new char[buffer_size_bytes];
  }
  buffer_pool.insert(std::make_pair(buffer_size_bytes, std::move(tmp_buffers)));
}

TupleBuffer BufferManager::getBufferByCAS(const uint32_t number_of_tuples, const uint32_t tuple_size_bytes) {

  // Find buffer of that size
  auto buffer_size_bytes = number_of_tuples * tuple_size_bytes;
  auto found = buffer_pool.find(buffer_size_bytes);
  if (found == buffer_pool.cend()) {
    std::cout << "No buffer of size " << buffer_size_bytes << " found." << std::endl;
    std::cout << "Create 10 new buffers of size " << buffer_size_bytes << " bytes." << std::endl;
    addBuffersByCAS(10, buffer_size_bytes);
    found = buffer_pool.find(buffer_size_bytes);
  }
  assert(found != buffer_pool.cend());

  bool found_buffer = false;
  do {
    for (auto &buffer : found->second) {
      int expected = 0;
      int desired = 1;
      if (buffer.used.compare_exchange_weak(expected, desired, std::memory_order_relaxed)) {
        return TupleBuffer((void *)buffer.buffer, buffer_size_bytes, tuple_size_bytes, number_of_tuples);
        found_buffer = true;
        break;
      }
    }
  } while (!found_buffer);
}

void BufferManager::releaseBufferByCAS(TupleBuffer tuple_buffer) {

  // Find buffer of that size
  auto found = buffer_pool.find(tuple_buffer.buffer_size);
  assert(found != buffer_pool.cend());

  // Add buffer again to manager
  int expected = 0;
  for (auto &buffer : found->second) {
    if (buffer.buffer == (char *)tuple_buffer.buffer) {
      buffer.used.exchange(expected);
      break;
    }
  }
}

#endif
} // namespace iotdb
