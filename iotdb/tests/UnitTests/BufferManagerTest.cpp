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
    BufferManagerPtr buffMgnr;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup BufferManagerTest test class." << std::endl;
    }

    /* Will be called before a  test is executed. */
    void SetUp() {
        NES::setupLogging("BufferManagerTest.log", NES::LOG_DEBUG);
        std::cout << "Setup BufferManagerTest test case." << std::endl;
        buffMgnr = std::make_shared<BufferManager>(buffer_size, buffers_managed);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        ASSERT_EQ(buffMgnr->getNumOfPooledBuffers(), buffers_managed);
        ASSERT_EQ(buffMgnr->getAvailableBuffers(), buffers_managed);
        std::cout << "Tear down BufferManagerTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down BufferManagerTest test class." << std::endl;

    }
};

TEST_F(BufferManagerTest, initialized_buffer_manager) {
    size_t buffers_count = buffMgnr->getNumOfPooledBuffers();
    size_t buffers_free = buffMgnr->getAvailableBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    ASSERT_EQ(buffers_free, buffers_managed);
}

TEST_F(BufferManagerTest, test_buffer_manager_no_singleton) {
    auto manager = std::make_unique<BufferManager>();
    manager->configure(1024, 1024);
    manager.reset();
}

TEST_F(BufferManagerTest, single_threaded_buffer_recycling) {
    ASSERT_EQ(buffMgnr->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(buffMgnr->getAvailableBuffers(), buffers_managed);
    auto buffer0 = buffMgnr->getBufferBlocking();
    ASSERT_EQ(buffMgnr->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(buffMgnr->getAvailableBuffers(), buffers_managed - 1);
    {
        auto buffer1 = buffMgnr->getBufferBlocking();
        ASSERT_EQ(buffMgnr->getNumOfPooledBuffers(), buffers_managed);
        ASSERT_EQ(buffMgnr->getAvailableBuffers(), buffers_managed - 2);
    }
    ASSERT_EQ(buffMgnr->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(buffMgnr->getAvailableBuffers(), buffers_managed - 1);
}

TEST_F(BufferManagerTest, single_threaded_buffer_recycling_unpooled) {
    auto buffer0 = buffMgnr->getUnpooledBuffer(16384);
    ASSERT_EQ(buffMgnr->getNumOfUnpooledBuffers(), 1);
    {
        auto buffer0 = buffMgnr->getUnpooledBuffer(16384);
        ASSERT_EQ(buffMgnr->getNumOfUnpooledBuffers(), 2);
    }
    ASSERT_EQ(buffMgnr->getNumOfUnpooledBuffers(), 2);
}

TEST_F(BufferManagerTest, single_threaded_many_buffer_recycling_unpooled) {
    auto pool = std::make_unique<BufferManager>();
    pool->configure(1024, 1);
    for (int i = 0; i < 500; i++) {
        auto buffer0 = pool->getUnpooledBuffer(16384);
        ASSERT_EQ(pool->getNumOfUnpooledBuffers(), 1);
    }
    ASSERT_EQ(pool->getNumOfUnpooledBuffers(), 1);
}

TEST_F(BufferManagerTest, getBuffer_afterRelease) {
    std::vector<TupleBuffer> buffers;

    ASSERT_EQ(buffMgnr->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(buffMgnr->getAvailableBuffers(), buffers_managed);

    // get all buffers
    for (size_t i = 0; i < buffers_managed; ++i) {
        buffers.push_back(buffMgnr->getBufferBlocking());
    }
    std::promise<bool> promise0, promise1;
    auto f0 = promise0.get_future();
    // start a thread that is blocking waiting on the queue
    std::thread t1([&promise0, &promise1, &buffMgnr]() {
      promise0.set_value(true);
      auto buf = buffMgnr->getBufferBlocking();
      buf.release();
      promise1.set_value(true);
    });
    f0.get();
    auto& buffer = buffers.back();
    buffer.release();
    promise1.get_future().get();
    t1.join();
    buffers.clear();
    ASSERT_EQ(buffMgnr->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(buffMgnr->getAvailableBuffers(), buffers_managed);
}

TEST_F(BufferManagerTest, buffer_manager_mt_access) {
    ASSERT_EQ(buffMgnr->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(buffMgnr->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> threads;
    for (int i = 0; i < 4; i++) {
        threads.emplace_back([buffMgnr]() {
          for (int i = 0; i < 50; ++i) {
              auto buf = buffMgnr->getBufferBlocking();
              std::this_thread::sleep_for(std::chrono::milliseconds(250));
          }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    ASSERT_EQ(buffMgnr->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(buffMgnr->getAvailableBuffers(), buffers_managed);
}

TEST_F(BufferManagerTest, buffer_manager_mt_producer_consumer) {
    ASSERT_EQ(buffMgnr->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(buffMgnr->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 250000;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    for (int i = 0; i < producer_threads; i++) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, &buffMgnr]() {
          for (int j = 0; j < max_buffer; ++j) {
              std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
              auto buf = buffMgnr->getBufferBlocking();
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
        auto buf = buffMgnr->getBufferBlocking();
        buf.getBufferAs<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = max_buffer;
        workQueue.push_back(buf);
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    workQueue.clear();
    ASSERT_EQ(buffMgnr->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(buffMgnr->getAvailableBuffers(), buffers_managed);
}

TEST_F(BufferManagerTest, buffer_manager_mt_producer_consumer_no_singleton) {
    auto pool = std::make_shared<BufferManager>();
    pool->configure(buffer_size, buffers_managed);

    ASSERT_EQ(pool->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 250000;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    for (int i = 0; i < producer_threads; i++) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, pool]() {
          for (int j = 0; j < max_buffer; ++j) {
              std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
              auto buf = pool->getBufferBlocking();
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
        auto buf = pool->getBufferBlocking();
        buf.getBufferAs<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = max_buffer;
        workQueue.push_back(buf);
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    workQueue.clear();
    ASSERT_EQ(pool->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool->getAvailableBuffers(), buffers_managed);
}

TupleBuffer getBufferTimeout(std::shared_ptr<BufferManager> bufferManager, std::chrono::milliseconds&& timeout) {
    std::optional<TupleBuffer> opt;
    while (!(opt = bufferManager->getBufferTimeout(timeout)).has_value()) {
        // nop
    }
    return *opt;
}


TEST_F(BufferManagerTest, buffer_manager_mt_producer_consumer_timeout) {
    using namespace std::chrono_literals;
    auto pool = std::make_shared<BufferManager>();
    pool->configure(buffer_size, buffers_managed);

    ASSERT_EQ(pool->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 250000;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    for (int i = 0; i < producer_threads; i++) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, pool]() {
          for (int j = 0; j < max_buffer; ++j) {
              std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
              auto buf = getBufferTimeout(pool, 100ms);
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
        auto buf = pool->getBufferBlocking();
        buf.getBufferAs<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = max_buffer;
        workQueue.push_back(buf);
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    workQueue.clear();
    ASSERT_EQ(pool->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool->getAvailableBuffers(), buffers_managed);
}

TupleBuffer getBufferNoBlocking(BufferManager& bufferManager) {
    std::optional<TupleBuffer> opt;
    while (!(opt = bufferManager.getBufferNoBlocking())) {
        usleep(100 * 1000);
    }
    return *opt;
}

TEST_F(BufferManagerTest, buffer_manager_mt_producer_consumer_noblocking) {
    auto pool = std::make_shared<BufferManager>();
    pool->configure(buffer_size, buffers_managed);

    ASSERT_EQ(pool->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 250;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    for (int i = 0; i < producer_threads; i++) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, pool]() {
          for (int j = 0; j < max_buffer; ++j) {
              std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
              auto buf = getBufferNoBlocking(*pool);
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
        auto buf = pool->getBufferBlocking();
        buf.getBufferAs<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = max_buffer;
        workQueue.push_back(buf);
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    workQueue.clear();
    ASSERT_EQ(pool->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool->getAvailableBuffers(), buffers_managed);
}


} // namespace NES
