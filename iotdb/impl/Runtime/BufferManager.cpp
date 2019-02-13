#include <iostream>
#include <utility>
#include <cassert>

#include <Runtime/BufferManager.hpp>

namespace iotdb {

  BufferManager::BufferManager() {

    std::cout << "Init BufferManager" << std::endl;
    std::vector<TupleBufferPtr> buffers(capacity);

    uint64_t buffer_size = 4096;
    uint32_t tuple_size_bytes = sizeof(uint64_t);
    uint32_t num_tuples = buffer_size / tuple_size_bytes;

    std::cout << "buffer_size: " << buffer_size << std::endl;

    for (uint32_t i = 0; i < capacity; i ++) {
      void *buffer = malloc(buffer_size);
      TupleBufferPtr buf = std::make_shared<TupleBuffer>(buffer, buffer_size, tuple_size_bytes, num_tuples);
      buffers[i] = buf;
    }

    buffer_pool.insert(std::make_pair(buffer_size, buffers));
  }

  BufferManager::~BufferManager() {
    std::cout << "Enter Destructor of BufferManager" << std::endl;
  }

  BufferManager &BufferManager::instance() {
    static BufferManager instance;
    // std::cout << "Buffer Manager address: " << &instance << std::endl;
    return instance;
  }

  TupleBufferPtr BufferManager::getBuffer(uint64_t size) {
    std::unique_lock<std::mutex> lock(mutex);
    auto found = buffer_pool.find(size);

    TupleBufferPtr buf = nullptr;

    if (found == buffer_pool.cend()) {
      std::cout << "no buffers for size " << size << " in pool. bug? " << std::endl;
    } else {
      auto& buffers = found->second;
      if (! buffers.empty()) {
        // std::cout << "before return tuple buffer to consumer, currently available buffer pool size is " << buffers.size() << std::endl;
        buf = buffers.back();
        buffers.pop_back();

        // std::cout << "after return tuple buffer to consumer, currently available buffer pool size is " << buffers.size() << std::endl;
      } else {
        std::cout << "no buffers in buffer pool" << std::endl;
      }
    }
    cv.notify_all();
    std::cout << "buffer address: " << buf << std::endl;
    return buf;
  }

  void BufferManager::releaseBuffer(TupleBufferPtr tupleBuffer) {
    std::unique_lock<std::mutex> lock(mutex);
    assert(tupleBuffer != nullptr && "tupleBuffer is nullptr");
    auto found = buffer_pool.find(tupleBuffer->buffer_size);
    if (found == buffer_pool.cend()) {
      std::cout << "no buffers for size " << tupleBuffer->buffer_size << " in pool. bug? " << std::endl;
    } else {
      auto& buffers = found->second;
      buffers.push_back(tupleBuffer);
      // sanity checking
      assert(buffers.size() <= capacity);
      // if (buffers.size() > capacity) {
      //   std::cerr << "buffer size is " << buffers.size() << ", wheras maximum capacity is " << capacity << std::endl;
      //   abort();
      // }
    }
    cv.notify_all();
  }
}
