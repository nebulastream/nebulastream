#include <iostream>
#include <utility>
#include <cassert>
#include <thread>
#include <Runtime/BufferManager.hpp>

namespace iotdb {

  BufferManager::BufferManager() : capacity(100) {

    std::cout << "Init BufferManager" << std::endl;
    std::vector<TupleBufferPtr> buffers(capacity);

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
      buffers[i] = buf;
    }

    buffer_pool.insert(std::make_pair(buffer_size, buffers));
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

    auto found = buffer_pool.find(size);
    assert(found != buffer_pool.cend() && (std::string("No buffers for size ") + std::to_string(size) + std::string(" in pool. bug?")).c_str());
    TupleBufferPtr buf = nullptr;
    auto& buffers = found->second;
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

  void BufferManager::releaseBuffer(TupleBufferPtr tupleBuffer) {

    assert(tupleBuffer != nullptr && "tupleBuffer is nullptr");
    auto found = buffer_pool.find(tupleBuffer->buffer_size);
    assert(found != buffer_pool.cend() && (std::string("No buffers for size ") + std::to_string(tupleBuffer->buffer_size) + std::string(" in pool. bug?")).c_str());
    // {
    auto& buffers = found->second;
    std::unique_lock<std::mutex> lock(mutex);
    buffers.push_back(tupleBuffer);
    // sanity checking
    assert(buffers.size() <= capacity && "Buffer size is fixed, bug?");

    // std::cerr << "Thread: " << std::this_thread::get_id() << " release buffer" << std::endl;
    cv.notify_all();
  }
}
