#include <map>
#include <vector>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <thread>

#include <Runtime/BufferManager.hpp>

namespace iotdb {

class ThreadPoolForBufferManager {
public:
  ThreadPoolForBufferManager() : run(), threads() {}
  ~ThreadPoolForBufferManager() {
    std::cout << "Destroying Thread Pool For Buffer" << std::endl;
    stop();
  }
  void start() {
    int size = 80;
    const int capacity = 100;
    iotdb::BufferManager::instance().addBuffers(capacity, size);
    std::cout << "Start threads" << std::endl;

    if (run)
      return;
    run = true;

    /* spawn threads */
    auto num_threads = std::thread::hardware_concurrency();

    std::cout << "Spawning " << num_threads << " threads" << std::endl;
    for (uint64_t i = 0; i < num_threads; ++i) {
      threads.push_back(std::thread(std::bind(&ThreadPoolForBufferManager::worker_thread, this)));
    }
  }

  void worker_thread() {
    BufferManager &buffer_manager = BufferManager::instance();
    const uint64_t size = 80;
    while (run) {

      TupleBuffer buf = buffer_manager.getBuffer(1, size);

      int sleep1 = std::rand() % 3 + 1;
      std::this_thread::sleep_for(std::chrono::seconds(sleep1));

      buffer_manager.releaseBuffer(buf);
      int sleep2 = std::rand() % 3 + 1;
      std::this_thread::sleep_for(std::chrono::seconds(sleep2));
    }
  }
  void stop() {
    if (!run)
      return;
    run = false;
    /* wake up all threads in the dispatcher,
     * so they notice the change in the run variable */
    BufferManager::instance().unblockThreads();
    /* join all threads if possible */
    for (auto &thread : threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

private:
  bool run;
  std::vector<std::thread> threads;
};

void testSingleThread() {
  int size = 80;
  const int capacity = 10;
  const int num_to_release = 2;
  iotdb::BufferManager::instance().addBuffers(capacity, size);

  std::vector<TupleBuffer> buffers;
  std::cout << "==========Get Buffer==========" << std::endl;
  for (int i = 0; i < capacity; i++) {
    buffers.push_back(BufferManager::instance().getBuffer(1, size));
    std::cout << "buffer " << i << ": " << buffers[i].buffer << std::endl;
  }

  std::cout << "==========Release Buffer==========" << std::endl;
  for (int i = 0; i < num_to_release; i++) {
    std::cout << "buffer " << capacity - i - 1 << ": " << buffers[capacity - i - 1].buffer << std::endl;
    if (buffers[i].buffer) { // make sure not nullptr
      BufferManager::instance().releaseBuffer(buffers[i]);
    }
  }

  std::cout << "==========Get Buffer Again==========" << std::endl;
  for (int i = capacity - num_to_release; i < capacity; i++) {
    buffers.push_back(BufferManager::instance().getBuffer(1, size));
    std::cout << "buffer " << i << ": " << buffers[i].buffer << std::endl;
  }

  std::cout << "==========Release Buffer==========" << std::endl;
  for (int i = 0; i < capacity; i++) {
    std::cout << "buffer " << i << ": " << buffers[i].buffer << std::endl;
    if (buffers[i].buffer) { // make sure not nullptr
      BufferManager::instance().releaseBuffer(buffers[i]);
    }
  }
}

void testMultiThreads() {
  ThreadPoolForBufferManager thread_pool;
  thread_pool.start();
  int sleep = 5;
  std::cout << "Waiting " << sleep << " seconds " << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(sleep));
}
} // namespace iotdb

int main(int argc, const char *argv[]) {
  iotdb::BufferManager::instance();

  std::cout << "Test Single Thread" << std::endl;
  iotdb::testSingleThread();
  std::cout << "Test Multiple Thread" << std::endl;
  iotdb::testMultiThreads();

  return 0;
}
