#include <iostream>
#include <utility>
#include <cassert>
#include <thread>
#include <Runtime/BufferManager.hpp>

namespace iotdb {

  BufferManager::BufferManager() : capacity(1000) {

    std::cout << "Init BufferManager" << std::endl;
    std::cout << "capacity: " << capacity << std::endl;
#if USE_LOCK_IMPL
    // 1. Lock Version
    std::cout << "USE LOCK IMPL" << std::endl;
    std::vector<TupleBufferPtr> buffers(capacity);
#elif USE_CAS_IMPL
    // 2. CAS Version
    std::cout << "USE CAS IMPL" << std::endl;
    std::vector<BufferEntry> buffers(capacity);
#endif

    // uint64_t buffer_size = 4096;
    // uint32_t tuple_size_bytes = sizeof(uint64_t);
    // uint32_t num_tuples = buffer_size / tuple_size_bytes;

    uint32_t tuple_size_bytes = sizeof(uint64_t);
    uint32_t num_tuples = 10;
    uint64_t buffer_size = num_tuples * tuple_size_bytes;
    std::cout << "buffer_size: " << buffer_size << std::endl;

    for (uint32_t i = 0; i < capacity; i ++) {
      // void *buffer = malloc(buffer_size);
      // https://stackoverflow.com/questions/43631415/using-shared-ptr-with-char
      std::shared_ptr<char> buffer(new char[buffer_size], std::default_delete<char []>());
      TupleBufferPtr buf = std::make_shared<TupleBuffer>((void *)(buffer.get()), buffer_size, tuple_size_bytes, num_tuples);

#if USE_LOCK_IMPL
      // 1. Lock Version
      buffers[i] = buf;
#elif USE_CAS_IMPL
      // 2. CAS Version
      buffers[i].buffer = buf;
#endif
    }

#if USE_LOCK_IMPL
    // 1. Lock Version
    buffer_pool.insert(std::make_pair(buffer_size, buffers));
#elif USE_CAS_IMPL
    // 2. CAS Version
    buffer_pool.insert(std::make_pair(buffer_size, std::move(buffers)));
#endif
  }

  BufferManager::~BufferManager() {
    std::cout << "Enter Destructor of BufferManager" << std::endl;
    buffer_pool.clear();
  }

  BufferManager &BufferManager::instance() {
    static BufferManager instance;
    // std::cout << "Buffer Manager address: " << &instance << std::endl;
    return instance;
  }

  TupleBufferPtr BufferManager::getBuffer(const uint64_t size) {

   //  auto found = buffer_pool.find(size);
   // assert(found != buffer_pool.cend() && (std::string("No buffers for size ") + std::to_string(size) + std::string(" in pool. bug?")).c_str());
   //  TupleBufferPtr buf = nullptr;
   //  auto &buffers = found->second;
    // 1. Lock Version
    // std::unique_lock<std::mutex> lock(mutex);
    // while (buffers.empty()) {
    //   cv.wait(lock);
    // }

    // assert(buffers.empty() == false && "must have available buffer. bug?");
    // buf = buffers.back();
    // buffers.pop_back();

    // std::cout << "Thread: " << std::this_thread::get_id() << " GOT Buffer: " << buf.get() << std::endl;

    // cv.notify_all();

    // 2. CAS Version
    // do {
    //   for (auto &buffer : buffers) {
    //     int expected = 0;
    //     int desired = 1;
    //     if (buffer.used.compare_exchange_weak(expected, desired, std::memory_order_relaxed)) {
    //       buf = buffer.buffer;
    //       // std::cout << "Thread: " << std::this_thread::get_id() << " GOT Buffer: " << buf.get() << std::endl;
    //       break;
    //     }
    //   }
    // } while (buf == nullptr);

    // return buf;
    #if USE_LOCK_IMPL
      return getBufferByLock(size);
    #elif USE_CAS_IMPL
      return getBufferByCAS(size);
    #endif
  }

  void BufferManager::releaseBuffer(TupleBufferPtr tupleBuffer) {

    // assert(tupleBuffer != nullptr && "tupleBuffer is nullptr");
    // auto found = buffer_pool.find(tupleBuffer->buffer_size);
    // assert(found != buffer_pool.cend() && (std::string("No buffers for size ") + std::to_string(tupleBuffer->buffer_size) + std::string(" in pool. bug?")).c_str());

    // auto &buffers = found->second;
    // 1. Lock Version
    // xxx: no need to add lock here
    // buffers.push_back(tupleBuffer);
    // // sanity checking
    // assert(buffers.size() <= capacity && "Buffer size is fixed, bug?");

    // std::cerr << "Thread: " << std::this_thread::get_id() << " release buffer" << std::endl;
    // cv.notify_all();

    // 2. CAS Version
    // int expected = 0;
    // for (auto & buffer : buffers) {
    //   if (buffer.buffer.get() == tupleBuffer.get()) {
    //     buffer.used.exchange(expected);
    //     // std::cout << "Thread: " << std::this_thread::get_id() << "Release Buffer: " << tupleBuffer.get() << std::endl;
    //     break;
    //   }
    #if USE_LOCK_IMPL
      releaseBufferByLock(tupleBuffer);
    #elif USE_CAS_IMPL
      releaseBufferByCAS(tupleBuffer);
    #endif
    }

#if USE_LOCK_IMPL
  TupleBufferPtr BufferManager::getBufferByLock(const uint64_t size) {
    // 1. Lock Version
    auto found = buffer_pool.find(size);
    assert(found != buffer_pool.cend() && (std::string("No buffers for size ") + std::to_string(size) + std::string(" in pool. bug?")).c_str());

    TupleBufferPtr buf = nullptr;
    auto &buffers = found->second;

    std::unique_lock<std::mutex> lock(mutex);
    while (buffers.empty()) {
      cv.wait(lock);
    }

    assert(buffers.empty() == false && "must have available buffer. bug?");
    buf = buffers.back();
    buffers.pop_back();

    cv.notify_all();
    return buf;
  }
  void BufferManager::releaseBufferByLock(TupleBufferPtr tupleBuffer) {
    assert(tupleBuffer != nullptr && "tupleBuffer is nullptr");
    auto found = buffer_pool.find(tupleBuffer->buffer_size);
    assert(found != buffer_pool.cend() && (std::string("No buffers for size ") + std::to_string(tupleBuffer->buffer_size) + std::string(" in pool. bug?")).c_str());

    auto &buffers = found->second;
    // 1. Lock Version
    // xxx: no need to add lock here
    std::unique_lock<std::mutex> lock(mutex);
    buffers.push_back(tupleBuffer);
    // sanity checking
    assert(buffers.size() <= capacity && "Buffer size is fixed, bug?");

    // std::cerr << "Thread: " << std::this_thread::get_id() << " release buffer" << std::endl;
    cv.notify_all();
  }

#elif USE_CAS_IMPL
  TupleBufferPtr BufferManager::getBufferByCAS(const uint64_t size) {
    auto found = buffer_pool.find(size);
    assert(found != buffer_pool.cend() && (std::string("No buffers for size ") + std::to_string(size) + std::string(" in pool. bug?")).c_str());
    TupleBufferPtr buf = nullptr;
    auto &buffers = found->second;

    do {
      for (auto &buffer : buffers) {
        int expected = 0;
        int desired = 1;
        if (buffer.used.compare_exchange_weak(expected, desired, std::memory_order_relaxed)) {
          buf = buffer.buffer;
          // std::cout << "Thread: " << std::this_thread::get_id() << " GOT Buffer: " << buf.get() << std::endl;
          break;
        }
      }
    } while (buf == nullptr);

    return buf;
  }
  void BufferManager::releaseBufferByCAS(TupleBufferPtr tupleBuffer) {
    assert(tupleBuffer != nullptr && "tupleBuffer is nullptr");
    auto found = buffer_pool.find(tupleBuffer->buffer_size);
    assert(found != buffer_pool.cend() && (std::string("No buffers for size ") + std::to_string(tupleBuffer->buffer_size) + std::string(" in pool. bug?")).c_str());

    auto &buffers = found->second;

    // 2. CAS Version
    int expected = 0;
    for (auto & buffer : buffers) {
      if (buffer.buffer.get() == tupleBuffer.get()) {
        buffer.used.exchange(expected);
        // std::cout << "Thread: " << std::this_thread::get_id() << "Release Buffer: " << tupleBuffer.get() << std::endl;
        break;
      }
    }
  }
#endif
}
