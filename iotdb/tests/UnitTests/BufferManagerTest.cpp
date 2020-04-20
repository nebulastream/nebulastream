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
        BufferManager::instance().configure(buffer_size, buffers_managed);
    }

    /* Will be called before a  test is executed. */
    void SetUp() {
        NES::setupLogging("BufferManagerTest.log", NES::LOG_DEBUG);
        std::cout << "Setup BufferManagerTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        //    BufferManager::instance().resizeFixedBufferCnt(10);

        std::cout << "Tear down BufferManagerTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down BufferManagerTest test class." << std::endl;
        ASSERT_EQ(BufferManager::instance().getNumOfPooledBuffers(), buffers_managed);
        ASSERT_EQ(BufferManager::instance().getAvailableBuffers(), buffers_managed);
    }
};

TEST_F(BufferManagerTest, initialized_buffer_manager) {
    auto& mgr = BufferManager::instance();
    size_t buffers_count = mgr.getNumOfPooledBuffers();
    size_t buffers_free = mgr.getAvailableBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    ASSERT_EQ(buffers_free, buffers_managed);
}

TEST_F(BufferManagerTest, test_buffer_manager_no_singleton) {
    auto manager = std::make_unique<BufferManager>();
    manager->configure(1024, 1024);
    manager.reset();
}

TEST_F(BufferManagerTest, single_threaded_buffer_recycling) {
    auto& pool = BufferManager::instance();
    ASSERT_EQ(pool.getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool.getAvailableBuffers(), buffers_managed);
    auto buffer0 = pool.getBufferBlocking();
    ASSERT_EQ(pool.getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool.getAvailableBuffers(), buffers_managed - 1);
    {
        auto buffer1 = pool.getBufferBlocking();
        ASSERT_EQ(pool.getNumOfPooledBuffers(), buffers_managed);
        ASSERT_EQ(pool.getAvailableBuffers(), buffers_managed - 2);
    }
    ASSERT_EQ(pool.getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool.getAvailableBuffers(), buffers_managed - 1);
}

TEST_F(BufferManagerTest, single_threaded_buffer_recycling_unpooled) {
    auto& pool = BufferManager::instance();
    auto buffer0 = pool.getUnpooledBuffer(16384);
    ASSERT_EQ(pool.getNumOfUnpooledBuffers(), 1);
    {
        auto buffer0 = pool.getUnpooledBuffer(16384);
        ASSERT_EQ(pool.getNumOfUnpooledBuffers(), 2);
    }
    ASSERT_EQ(pool.getNumOfUnpooledBuffers(), 2);
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

//
// TEST_F(BufferManagerTest, resize_buffer_pool) {
//  size_t buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
//  size_t buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
//  ASSERT_EQ(buffers_count, buffers_managed);
//  ASSERT_EQ(buffers_free, buffers_managed);
//
//  BufferManager::instance().resizeFixedBufferCnt(5);
//  buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
//  buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
//  size_t expected = 5;
//  ASSERT_EQ(buffers_count, expected);
//  ASSERT_EQ(buffers_free, expected);
//
//  BufferManager::instance().resizeFixedBufferCnt(buffers_managed);
//  buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
//  buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
//  ASSERT_EQ(buffers_count, buffers_managed);
//  ASSERT_EQ(buffers_free, buffers_managed);
//  BufferManager::instance().reset();
//}
//
// TEST_F(BufferManagerTest, resize_buffer_size) {
//  size_t buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
//  size_t buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
//  ASSERT_EQ(buffers_count, buffers_managed);
//  ASSERT_EQ(buffers_free, buffers_managed);
//
//  BufferManager::instance().resizeFixedBufferSize(buffer_size * 2);
//  TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
//  ASSERT_EQ(buf->getBufferSizeInBytes(), 2 * buffer_size);
//  bool retRel = BufferManager::instance().releaseFixedSizeBuffer(buf);
//  ASSERT_TRUE(retRel);
//
//  BufferManager::instance().resizeFixedBufferSize(buffer_size);
//  buf = BufferManager::instance().getFixedSizeBuffer();
//  ASSERT_EQ(buf->getBufferSizeInBytes(), buffer_size);
//  bool retRel2 = BufferManager::instance().releaseFixedSizeBuffer(buf);
//  ASSERT_TRUE(retRel2);
//
//  buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
//  buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
//  ASSERT_EQ(buffers_count, buffers_managed);
//  ASSERT_EQ(buffers_free, buffers_managed);
//  BufferManager::instance().reset();
//
//}
//
// void run(TupleBufferPtr* ptr) {
//  *ptr = BufferManager::instance().getFixedSizeBuffer();
//  std::cout << "unreleased blocking buffer" << std::endl;
//}

// void run_and_release(size_t id, size_t sleeptime) {
//  TupleBufferPtr ptr = BufferManager::instance().getFixedSizeBuffer();
//  std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime));
//  bool retRel = BufferManager::instance().releaseFixedSizeBuffer(ptr);
//  ASSERT_TRUE(retRel);
//}
//
TEST_F(BufferManagerTest, getBuffer_afterRelease) {
    std::vector<TupleBuffer> buffers;
    auto& pool = BufferManager::instance();

    ASSERT_EQ(pool.getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool.getAvailableBuffers(), buffers_managed);

    // get all buffers
    for (size_t i = 0; i < buffers_managed; ++i) {
        buffers.push_back(BufferManager::instance().getBufferBlocking());
    }
    std::promise<bool> promise0, promise1;
    auto f0 = promise0.get_future();
    // start a thread that is blocking waiting on the queue
    std::thread t1([&promise0, &promise1]() {
      promise0.set_value(true);
      auto buf = BufferManager::instance().getBufferBlocking();
      buf.release();
      promise1.set_value(true);
    });
    f0.get();
    auto& buffer = buffers.back();
    buffer.release();
    promise1.get_future().get();
    t1.join();
    buffers.clear();
    ASSERT_EQ(pool.getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool.getAvailableBuffers(), buffers_managed);
}

TEST_F(BufferManagerTest, buffer_manager_mt_access) {
    auto& pool = BufferManager::instance();

    ASSERT_EQ(pool.getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool.getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> threads;
    for (int i = 0; i < 4; i++) {
        threads.emplace_back([]() {
          for (int i = 0; i < 50; ++i) {
              auto buf = BufferManager::instance().getBufferBlocking();
              std::this_thread::sleep_for(std::chrono::milliseconds(250));
          }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    ASSERT_EQ(pool.getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool.getAvailableBuffers(), buffers_managed);
}

TEST_F(BufferManagerTest, buffer_manager_mt_producer_consumer) {
    auto& pool = BufferManager::instance();

    ASSERT_EQ(pool.getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool.getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 250000;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    for (int i = 0; i < producer_threads; i++) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar]() {
          for (int j = 0; j < max_buffer; ++j) {
              std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
              auto buf = BufferManager::instance().getBufferBlocking();
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
        auto buf = BufferManager::instance().getBufferBlocking();
        buf.getBufferAs<uint32_t>()[buffer_size/sizeof(uint32_t) - 1] = max_buffer;
        workQueue.push_back(buf);
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    workQueue.clear();
    ASSERT_EQ(pool.getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(pool.getAvailableBuffers(), buffers_managed);
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


//
// TEST_F(BufferManagerTest, get_and_release) {
//  size_t buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
//  size_t buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
//  ASSERT_EQ(buffers_count, buffers_managed);
//  ASSERT_EQ(buffers_free, buffers_managed);
//
//  std::vector<std::thread> threads;
//  BufferManager::instance().printStatistics();
//  std::random_device rd;
//  std::mt19937 mt(rd());
//  std::uniform_int_distribution<size_t> sleeptime(1, 100);
//
//  for (size_t i = 0; i < 100; i++) {
//    threads.emplace_back(run_and_release, i, sleeptime(mt));
//  }
//
//  for (auto& thread : threads) {
//    thread.join();
//  }
//
//  buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
//  buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
//  ASSERT_EQ(buffers_count, buffers_managed);
//  ASSERT_EQ(buffers_free, buffers_managed);
//  BufferManager::instance().printStatistics();
//}
//
//#ifndef NO_RACE_CHECK
// TEST_F(BufferManagerTest, getBuffer_race) {
//  for (int i = 0; i < 100; ++i) {
//    std::cout << "Run getBuffer_race " << i << std::endl;
//
//    std::vector<TupleBufferPtr> buffers;
//
//    size_t buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
//    size_t buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
//    ASSERT_EQ(buffers_count, buffers_managed);
//    ASSERT_EQ(buffers_free, buffers_managed);
//
//    for (size_t j = 0; j < buffers_count; ++j) {
//      buffers.push_back(BufferManager::instance().getFixedSizeBuffer());
//    }
//
//    TupleBufferPtr buffer_threads[buffers_managed];
//    std::thread threads[buffers_managed];
//    for (size_t j = 0; j < buffers_count; ++j) {
//      threads[j] = std::thread(run, &buffer_threads[j]);
//    }
//
//    std::this_thread::sleep_for(std::chrono::milliseconds(1));
//
//    for (size_t j = 0; j < buffers_managed; ++j) {
//      bool retRel = BufferManager::instance().releaseFixedSizeBuffer(buffers.back());
//           ASSERT_TRUE(retRel);
//      buffers.pop_back();
//    }
//
//    for (auto& thread : threads) {
//      thread.join();
//    }
//
//    std::set<TupleBufferPtr> setOfTupleBufferPtr;
//    for (auto& b : buffer_threads) {
//      setOfTupleBufferPtr.insert(b);
//    }
//    ASSERT_EQ(10, setOfTupleBufferPtr.size());
//
//    for (auto& b : buffer_threads) {
//      bool retRel = BufferManager::instance().releaseFixedSizeBuffer(b);
//           ASSERT_TRUE(retRel);
//    }
//    buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
//    buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
//    ASSERT_EQ(buffers_count, buffers_managed);
//    ASSERT_EQ(buffers_free, buffers_managed);
//  }
//}
//#endif
///**
// * Var Size Buffer tests
// */
// TEST_F(BufferManagerTest, add_and_remove_Var_Buffer_simple) {
//  size_t buffers_count = BufferManager::instance().getNumberOfVarBuffers();
//  size_t buffers_free = BufferManager::instance().getNumberOfFreeVarBuffers();
//  ASSERT_EQ(buffers_count, 0);
//  ASSERT_EQ(buffers_free, 0);
//
//  TupleBufferPtr buffer = BufferManager::instance().createVarSizeBuffer(100);
//  ASSERT_EQ(buffer->getBufferSizeInBytes(), 100);
//
//  buffers_count = BufferManager::instance().getNumberOfVarBuffers();
//  buffers_free = BufferManager::instance().getNumberOfFreeVarBuffers();
//  size_t expected = 1;
//  ASSERT_EQ(buffers_count, expected);
//  ASSERT_EQ(buffers_free, expected - 1);
//
//  bool retRelease = BufferManager::instance().releaseVarSizedBuffer(buffer);
//  ASSERT_TRUE(retRelease);
////  bool retRemove = BufferManager::instance().removeBuffer(buffer);
////  ASSERT_TRUE(retRemove);
//
//  buffers_count = BufferManager::instance().getNumberOfVarBuffers();
//  buffers_free = BufferManager::instance().getNumberOfFreeVarBuffers();
//  ASSERT_EQ(buffers_count, 1);
//  ASSERT_EQ(buffers_free, 1);
//  BufferManager::instance().reset();
//}
//
// TEST_F(BufferManagerTest, get_Existing_Var_Buffer) {
//  size_t buffers_count = BufferManager::instance().getNumberOfVarBuffers();
//  size_t buffers_free = BufferManager::instance().getNumberOfFreeVarBuffers();
//  ASSERT_EQ(buffers_count, 0);
//  ASSERT_EQ(buffers_free, 0);
//
//  TupleBufferPtr buffer = BufferManager::instance().getVarSizeBufferLargerThan(
//      100);
//  ASSERT_EQ(buffer, nullptr);
//
//  TupleBufferPtr buffer1 = BufferManager::instance().createVarSizeBuffer(80);
//  ASSERT_EQ(buffer1->getBufferSizeInBytes(), 80);
//
//  TupleBufferPtr buffer2 = BufferManager::instance().getVarSizeBufferLargerThan(
//      100);
//  ASSERT_EQ(buffer2, nullptr);
//
//  TupleBufferPtr buffer3 = BufferManager::instance().getVarSizeBufferLargerThan(
//      60);
//  ASSERT_NE(buffer3->getBufferSizeInBytes(), 60);
//
//  buffers_count = BufferManager::instance().getNumberOfVarBuffers();
//  buffers_free = BufferManager::instance().getNumberOfFreeVarBuffers();
//  size_t expected = 1;
//  ASSERT_EQ(buffers_count, expected);
//  ASSERT_EQ(buffers_free, 0);
//
//  bool retRel = BufferManager::instance().releaseVarSizedBuffer(buffer3);
//       ASSERT_TRUE(retRel);
//
//  buffers_count = BufferManager::instance().getNumberOfVarBuffers();
//  buffers_free = BufferManager::instance().getNumberOfFreeVarBuffers();
//  ASSERT_EQ(buffers_count, 1);
//  ASSERT_EQ(buffers_free, 1);
//}

} // namespace NES
