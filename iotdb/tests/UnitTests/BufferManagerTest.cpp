#include <map>
#include <vector>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <log4cxx/appender.h>
#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <random>

namespace NES {
class BufferManagerTest : public testing::Test {
 public:
  static void SetUpTestCase() {
    NES::setupLogging("BufferManagerTest.log", NES::LOG_DEBUG);
    NES_INFO("Setup BufferMangerTest test class.");
    BufferManager::instance().resizeFixBufferCnt(10);
  }
  static void TearDownTestCase() {
    std::cout << "Tear down BufferManager test class." << std::endl;
  }

  const size_t buffers_managed = 10;
  const size_t buffer_size = 4 * 1024;
};

TEST_F(BufferManagerTest, add_and_remove_Buffer_simple) {
  size_t buffers_count = BufferManager::instance().getNumberOfBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);

  BufferManager::instance().resizeFixBufferCnt(11);
  TupleBufferPtr buffer = BufferManager::instance().getFixSizeBuffer();

  buffers_count = BufferManager::instance().getNumberOfBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  size_t expected = buffers_managed + 1;
  ASSERT_EQ(buffers_count, expected);
  ASSERT_EQ(buffers_free, buffers_managed);

  BufferManager::instance().releaseBuffer(buffer);
  BufferManager::instance().removeBuffer(buffer);

  buffers_count = BufferManager::instance().getNumberOfBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);
}

TEST_F(BufferManagerTest, get_and_release_Buffer_simple) {
  std::vector<TupleBufferPtr> buffers;

  size_t buffers_count = BufferManager::instance().getNumberOfBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);

  for (size_t i = 1; i <= BufferManager::instance().getNumberOfBuffers(); ++i) {
    TupleBufferPtr buf = BufferManager::instance().getFixSizeBuffer();
    size_t expected = 0;
    ASSERT_TRUE(buf->getBuffer() != nullptr);
    ASSERT_EQ(buf->getBufferSizeInBytes(), buffer_size);
    ASSERT_EQ(buf->getNumberOfTuples(), expected);
    ASSERT_EQ(buf->getTupleSizeInBytes(), expected);

    buffers_count = BufferManager::instance().getNumberOfBuffers();
    buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    expected = buffers_managed - i;
    ASSERT_EQ(buffers_free, expected);

    buffers.push_back(buf);
  }

  size_t i = 1;
  for (auto& buf : buffers) {

    BufferManager::instance().releaseBuffer(buf);

    buffers_count = BufferManager::instance().getNumberOfBuffers();
    buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    ASSERT_EQ(buffers_free, i);
    i++;
  }

  buffers_count = BufferManager::instance().getNumberOfBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);
}

TEST_F(BufferManagerTest, resize_buffer_pool) {
  size_t buffers_count = BufferManager::instance().getNumberOfBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);

  BufferManager::instance().resizeFixBufferCnt(5);
  buffers_count = BufferManager::instance().getNumberOfBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  size_t expected = 5;
  ASSERT_EQ(buffers_count, expected);
  ASSERT_EQ(buffers_free, expected);

  BufferManager::instance().resizeFixBufferCnt(buffers_managed);
  buffers_count = BufferManager::instance().getNumberOfBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);
}

TEST_F(BufferManagerTest, resize_buffer_size) {
  size_t buffers_count = BufferManager::instance().getNumberOfBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);

  BufferManager::instance().resizeFixBufferSize(buffer_size * 2);
  TupleBufferPtr buf = BufferManager::instance().getFixSizeBuffer();
  ASSERT_EQ(buf->getBufferSizeInBytes(), 2 * buffer_size);
  BufferManager::instance().releaseBuffer(buf);

  BufferManager::instance().resizeFixBufferSize(buffer_size);
  buf = BufferManager::instance().getFixSizeBuffer();
  ASSERT_EQ(buf->getBufferSizeInBytes(), buffer_size);
  BufferManager::instance().releaseBuffer(buf);

  buffers_count = BufferManager::instance().getNumberOfBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);

}

void run(TupleBufferPtr *ptr) {
  *ptr = BufferManager::instance().getFixSizeBuffer();
}

void run_and_release(size_t id, size_t sleeptime) {
  TupleBufferPtr ptr = BufferManager::instance().getFixSizeBuffer();
  std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime));
  BufferManager::instance().releaseBuffer(ptr);
}

TEST_F(BufferManagerTest, getBuffer_afterRelease) {
  std::vector<TupleBufferPtr> buffers;

  size_t buffers_count = BufferManager::instance().getNumberOfBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);

  for (size_t i = 0; i < buffers_count; ++i) {
    buffers.push_back(BufferManager::instance().getFixSizeBuffer());
  }

  TupleBufferPtr t;
  std::thread t1(run, &t);

  std::this_thread::sleep_for(std::chrono::seconds(1));

  BufferManager::instance().releaseBuffer(buffers.back());
  buffers.pop_back();
  t1.join();

  buffers.push_back(t);

  for (auto& buf : buffers) {
    BufferManager::instance().releaseBuffer(buf);
  }

  buffers_count = BufferManager::instance().getNumberOfBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);
}

TEST_F(BufferManagerTest, get_and_release) {
  size_t buffers_count = BufferManager::instance().getNumberOfBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);

  std::vector<std::thread> threads;
  BufferManager::instance().printStatistics();
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<size_t> sleeptime(1, 100);

  for (size_t i = 0; i < 100; i++) {
    threads.emplace_back(run_and_release, i, sleeptime(mt));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  buffers_count = BufferManager::instance().getNumberOfBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);
  BufferManager::instance().printStatistics();
}

#ifndef NO_RACE_CHECK
TEST_F(BufferManagerTest, getBuffer_race) {
  for (int i = 0; i < 100; ++i) {
    std::cout << "Run getBuffer_race " << i << std::endl;

    std::vector<TupleBufferPtr> buffers;

    size_t buffers_count = BufferManager::instance().getNumberOfBuffers();
    size_t buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    ASSERT_EQ(buffers_free, buffers_managed);

    for (size_t j = 0; j < buffers_count; ++j) {
      buffers.push_back(BufferManager::instance().getFixSizeBuffer());
    }

    TupleBufferPtr buffer_threads[buffers_managed];
    std::thread threads[buffers_managed];
    for (size_t j = 0; j < buffers_count; ++j) {
      threads[j] = std::thread(run, &buffer_threads[j]);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    for (size_t j = 0; j < buffers_managed; ++j) {
      BufferManager::instance().releaseBuffer(buffers.back());
      buffers.pop_back();
    }

    for (auto &thread : threads) {
      thread.join();
    }

    std::set<TupleBufferPtr> setOfTupleBufferPtr;
    for (auto &b : buffer_threads) {
      setOfTupleBufferPtr.insert(b);
    }
    ASSERT_EQ(10, setOfTupleBufferPtr.size());

    for (auto &b : buffer_threads) {
      BufferManager::instance().releaseBuffer(b);
    }
    buffers_count = BufferManager::instance().getNumberOfBuffers();
    buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    ASSERT_EQ(buffers_free, buffers_managed);
  }
}
#endif
}
