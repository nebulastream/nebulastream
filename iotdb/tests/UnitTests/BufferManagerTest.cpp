#include <map>
#include <vector>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <cassert>
#include <cstdlib>
#include <deque>
#include <future>
#include <gtest/gtest.h>
#include <iostream>
#include <log4cxx/appender.h>
#include <random>
#include <thread>

namespace NES {
const size_t buffers_managed = 10;
const size_t buffer_size = 4*1024;

class BufferManagerTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup BufferManagerTest test class." << std::endl;
    }

    /* Will be called before a  test is executed. */
    void SetUp() {
        NES::setupLogging("BufferManagerTest.log", NES::LOG_DEBUG);
        std::cout << "Setup BufferManagerTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Tear down BufferManagerTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down BufferManagerTest test class." << std::endl;

    }
};

TEST_F(BufferManagerTest, initializedBufferManager) {
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(buffer_size, buffers_managed);
    size_t buffers_count = bufferManager->getNumOfPooledBuffers();
    size_t buffers_free = bufferManager->getAvailableBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    ASSERT_EQ(buffers_free, buffers_managed);
}

TEST_F(BufferManagerTest, testBufferManagerNoSingleton) {
    auto manager = std::make_unique<BufferManager>();
    manager->configure(1024, 1024);
    manager.reset();
}

TEST_F(BufferManagerTest, singleThreadedBufferRecycling) {
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(buffer_size, buffers_managed);

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);
    auto buffer0 = bufferManager->getBufferBlocking();
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed - 1);
    {
        auto buffer1 = bufferManager->getBufferBlocking();
        ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
        ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed - 2);
    }
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed - 1);
}

TEST_F(BufferManagerTest, singleThreadedBufferRecyclingUnpooled) {
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(buffer_size, buffers_managed);

    auto buffer0 = bufferManager->getUnpooledBuffer(16384);
    ASSERT_EQ(bufferManager->getNumOfUnpooledBuffers(), 1);
    {
        auto buffer0 = bufferManager->getUnpooledBuffer(16384);
        ASSERT_EQ(bufferManager->getNumOfUnpooledBuffers(), 2);
    }
    ASSERT_EQ(bufferManager->getNumOfUnpooledBuffers(), 2);
}

TEST_F(BufferManagerTest, singleThreadedManyBufferRecyclingUnpooled) {
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(1024, 1);
    for (int i = 0; i < 500; i++) {
        auto buffer0 = bufferManager->getUnpooledBuffer(16384);
        ASSERT_EQ(bufferManager->getNumOfUnpooledBuffers(), 1);
    }
    ASSERT_EQ(bufferManager->getNumOfUnpooledBuffers(), 1);
}

TEST_F(BufferManagerTest, getBufferAfterRelease) {
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(buffer_size, buffers_managed);

    std::vector<TupleBuffer> buffers;

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    // get all buffers
    for (size_t i = 0; i < buffers_managed; ++i) {
        buffers.push_back(bufferManager->getBufferBlocking());
    }
    std::promise<bool> promise0, promise1;
    auto f0 = promise0.get_future();
    // start a thread that is blocking waiting on the queue
    std::thread t1([&promise0, &promise1, this, &bufferManager]() {
      promise0.set_value(true);
      auto buf = bufferManager->getBufferBlocking();
      buf.release();
      promise1.set_value(true);
    });
    f0.get();
    auto& buffer = buffers.back();
    buffer.release();
    promise1.get_future().get();
    t1.join();
    buffers.clear();
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);
}

TEST_F(BufferManagerTest, bufferManagerMtAccess) {
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(buffer_size, buffers_managed);

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> threads;
    for (int i = 0; i < 4; i++) {
        threads.emplace_back([this, &bufferManager]() {
          for (int i = 0; i < 50; ++i) {
              auto buf = bufferManager->getBufferBlocking();
              std::this_thread::sleep_for(std::chrono::milliseconds(250));
          }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);
}

TEST_F(BufferManagerTest, bufferManagerMtProducerConsumer) {
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(buffer_size, buffers_managed);

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 250000;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    for (int i = 0; i < producer_threads; i++) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, this, &bufferManager]() {
          for (int j = 0; j < max_buffer; ++j) {
              std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
              auto buf = bufferManager->getBufferBlocking();
              for (uint32_t k = 0; k < (buffer_size/sizeof(uint32_t) - 1); ++k) {
                  buf.getBufferAs<uint32_t>()[k] = k;
              }
              buf.getBuffer<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = 0;
              lock.lock();
              workQueue.push_back(buf);
              cvar.notify_all();
              lock.unlock();
          }
        });
    }
    for (int i = 0; i < consumer_threads; i++) {
        con_threads.emplace_back([&workQueue, &mutex, &cvar]() {
          std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
          while (true) {
              lock.lock();
              while (workQueue.empty()) {
                  cvar.wait(lock);
              }
              auto buf = workQueue.front();
              workQueue.pop_front();
              lock.unlock();
              for (uint32_t k = 0; k < (buffer_size/sizeof(uint32_t) - 1); ++k) {
                  ASSERT_EQ(buf.getBufferAs<uint32_t>()[k], k);
              }
              if (buf.getBufferAs<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] == max_buffer) {
                  break;
              }
          }
        });
    }
    for (auto& t : prod_threads) {
        t.join();
    }
    for (int j = 0; j < consumer_threads; ++j) {
        std::unique_lock<std::mutex> lock(mutex);
        auto buf = bufferManager->getBufferBlocking();
        buf.getBufferAs<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = max_buffer;
        workQueue.push_back(buf);
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    workQueue.clear();
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);
}

TEST_F(BufferManagerTest, bufferManagerMtProducerConsumerNoSingleton) {
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(buffer_size, buffers_managed);

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 250000;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    for (int i = 0; i < producer_threads; i++) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, bufferManager]() {
          for (int j = 0; j < max_buffer; ++j) {
              std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
              auto buf = bufferManager->getBufferBlocking();
              for (uint32_t k = 0; k < (buffer_size/sizeof(uint32_t) - 1); ++k) {
                  buf.getBufferAs<uint32_t>()[k] = k;
              }
              buf.getBuffer<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = 0;
              lock.lock();
              workQueue.push_back(buf);
              cvar.notify_all();
              lock.unlock();
          }
        });
    }
    for (int i = 0; i < consumer_threads; i++) {
        con_threads.emplace_back([&workQueue, &mutex, &cvar]() {
          std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
          while (true) {
              lock.lock();
              while (workQueue.empty()) {
                  cvar.wait(lock);
              }
              auto buf = workQueue.front();
              workQueue.pop_front();
              lock.unlock();
              for (uint32_t k = 0; k < (buffer_size/sizeof(uint32_t) - 1); ++k) {
                  ASSERT_EQ(buf.getBufferAs<uint32_t>()[k], k);
              }
              if (buf.getBufferAs<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] == max_buffer) {
                  break;
              }
          }
        });
    }
    for (auto& t : prod_threads) {
        t.join();
    }
    for (int j = 0; j < consumer_threads; ++j) {
        std::unique_lock<std::mutex> lock(mutex);
        auto buf = bufferManager->getBufferBlocking();
        buf.getBufferAs<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = max_buffer;
        workQueue.push_back(buf);
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    workQueue.clear();
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);
}

TupleBuffer getBufferTimeout(std::shared_ptr<BufferManager> bufferManager, std::chrono::milliseconds&& timeout) {
    std::optional<TupleBuffer> opt;
    while (!(opt = bufferManager->getBufferTimeout(timeout)).has_value()) {
        // nop
    }
    return *opt;
}

TEST_F(BufferManagerTest, bufferManagerMtProducerConsumerTimeout) {
    using namespace std::chrono_literals;
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(buffer_size, buffers_managed);

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 250000;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    for (int i = 0; i < producer_threads; i++) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, bufferManager]() {
          for (int j = 0; j < max_buffer; ++j) {
              std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
              auto buf = getBufferTimeout(bufferManager, 100ms);
              for (uint32_t k = 0; k < (buffer_size/sizeof(uint32_t) - 1); ++k) {
                  buf.getBufferAs<uint32_t>()[k] = k;
              }
              buf.getBuffer<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = 0;
              lock.lock();
              workQueue.push_back(buf);
              cvar.notify_all();
              lock.unlock();
          }
        });
    }
    for (int i = 0; i < consumer_threads; i++) {
        con_threads.emplace_back([&workQueue, &mutex, &cvar]() {
          std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
          while (true) {
              lock.lock();
              while (workQueue.empty()) {
                  cvar.wait(lock);
              }
              auto buf = workQueue.front();
              workQueue.pop_front();
              lock.unlock();
              for (uint32_t k = 0; k < (buffer_size/sizeof(uint32_t) - 1); ++k) {
                  ASSERT_EQ(buf.getBufferAs<uint32_t>()[k], k);
              }
              auto ctrl_val = buf.getBuffer<uint32_t>()[buffer_size/sizeof(uint32_t) - 1];
              if (ctrl_val == max_buffer) {
                  break;
              }
          }
        });
    }
    for (auto& t : prod_threads) {
        t.join();
    }
    for (int j = 0; j < consumer_threads; ++j) {
        std::unique_lock<std::mutex> lock(mutex);
        auto buf = bufferManager->getBufferBlocking();
        buf.getBufferAs<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = max_buffer;
        workQueue.push_back(buf);
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    workQueue.clear();
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);
}

TupleBuffer getBufferNoBlocking(BufferManager& bufferManager) {
    std::optional<TupleBuffer> opt;
    while (!(opt = bufferManager.getBufferNoBlocking())) {
        usleep(100*1000);
    }
    return *opt;
}

TEST_F(BufferManagerTest, bufferManagerMtProducerConsumerNoblocking) {
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(buffer_size, buffers_managed);

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 250;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    for (int i = 0; i < producer_threads; i++) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, bufferManager]() {
          for (int j = 0; j < max_buffer; ++j) {
              std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
              auto buf = getBufferNoBlocking(*bufferManager);
              for (uint32_t k = 0; k < (buffer_size/sizeof(uint32_t) - 1); ++k) {
                  buf.getBufferAs<uint32_t>()[k] = k;
              }
              buf.getBuffer<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = 0;
              lock.lock();
              workQueue.push_back(buf);
              cvar.notify_all();
              lock.unlock();
          }
        });
    }
    for (int i = 0; i < consumer_threads; i++) {
        con_threads.emplace_back([&workQueue, &mutex, &cvar]() {
          std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
          while (true) {
              lock.lock();
              while (workQueue.empty()) {
                  cvar.wait(lock);
              }
              auto buf = workQueue.front();
              workQueue.pop_front();
              lock.unlock();
              for (uint32_t k = 0; k < (buffer_size/sizeof(uint32_t) - 1); ++k) {
                  ASSERT_EQ(buf.getBufferAs<uint32_t>()[k], k);
              }
              if (buf.getBufferAs<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] == max_buffer) {
                  break;
              }
          }
        });
    }
    for (auto& t : prod_threads) {
        t.join();
    }
    for (int j = 0; j < consumer_threads; ++j) {
        std::unique_lock<std::mutex> lock(mutex);
        auto buf = bufferManager->getBufferBlocking();
        buf.getBufferAs<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = max_buffer;
        workQueue.push_back(buf);
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    workQueue.clear();
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);
}

} // namespace NES
