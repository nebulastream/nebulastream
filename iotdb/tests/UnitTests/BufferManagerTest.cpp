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
#include <iostream>

namespace NES {
const size_t buffers_managed = 10;
const size_t buffer_size = 4 * 1024;

class BufferManagerTest : public testing::Test {
 public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    std::cout << "Setup BufferManagerTest test class." << std::endl;
  }

  /* Will be called before a  test is executed. */
  void SetUp() {
    NES::setupLogging("BufferManagerTest.log", NES::LOG_DEBUG);
    BufferManager::instance().resizeFixedBufferCnt(buffers_managed);
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
  }
};

#if 0
TEST_F(BufferManagerTest, add_and_remove_Buffer_simple) {
  size_t buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);

  BufferManager::instance().resizeFixedBufferCnt(buffers_managed + 1);
  TupleBufferPtr buffer = BufferManager::instance().getFixedSizeBuffer();

  buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  size_t expected = buffers_managed + 1;
  ASSERT_EQ(buffers_count, expected);
  ASSERT_EQ(buffers_free, buffers_managed);

  bool retRel = BufferManager::instance().releaseBuffer(buffer);
  ASSERT_TRUE(retRel);

  buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  ASSERT_EQ(buffers_count, buffers_managed + 1);
  ASSERT_EQ(buffers_free, buffers_managed + 1);
  BufferManager::instance().reset();
}

TEST_F(BufferManagerTest, get_and_release_Buffer_simple) {
  std::vector<TupleBufferPtr> buffers;

  size_t buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);

  for (size_t i = 1; i <= BufferManager::instance().getNumberOfFixedBuffers();
      ++i) {
    TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
//    std::cout << "useCnt after get=" << (size_t) buf.use_count() << " addr=" << buf.get( )<< std::endl;
    size_t expected = 0;
    ASSERT_TRUE(buf->getBuffer() != nullptr);
    ASSERT_EQ(buf->getBufferSizeInBytes(), buffer_size);
    ASSERT_EQ(buf->getNumberOfTuples(), expected);
    ASSERT_EQ(buf->getTupleSizeInBytes(), expected);

    buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
    buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    expected = buffers_managed - i;
    ASSERT_EQ(buffers_free, expected);

    buffers.push_back(buf);
//    std::cout << "useCnt after push_back=" << buf.use_count() << " addr=" << buf.get( )<< std::endl;
  }

  size_t i = 1;
  for (auto& buf : buffers) {
//    std::cout << "useCnt after beforeRel=" << buf.use_count() << " addr=" << buf.get( )<< std::endl;
    bool retRel = BufferManager::instance().releaseBuffer(buf);
    ASSERT_TRUE(retRel);
//    std::cout << "useCnt after afterRel=" << buf.use_count() << " addr=" << buf.get( )<< std::endl;

    buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
    buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    ASSERT_EQ(buffers_free, i);
    i++;
  }

  buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);
  BufferManager::instance().reset();
}

TEST_F(BufferManagerTest, resize_buffer_pool) {
  size_t buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);

  BufferManager::instance().resizeFixedBufferCnt(5);
  buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  size_t expected = 5;
  ASSERT_EQ(buffers_count, expected);
  ASSERT_EQ(buffers_free, expected);

  BufferManager::instance().resizeFixedBufferCnt(buffers_managed);
  buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);
  BufferManager::instance().reset();
}

TEST_F(BufferManagerTest, resize_buffer_size) {
  size_t buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);

  BufferManager::instance().resizeFixedBufferSize(buffer_size * 2);
  TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
  ASSERT_EQ(buf->getBufferSizeInBytes(), 2 * buffer_size);
  bool retRel = BufferManager::instance().releaseBuffer(buf);
  ASSERT_TRUE(retRel);

  BufferManager::instance().resizeFixedBufferSize(buffer_size);
  buf = BufferManager::instance().getFixedSizeBuffer();
  ASSERT_EQ(buf->getBufferSizeInBytes(), buffer_size);
  bool retRel2 = BufferManager::instance().releaseBuffer(buf);
  ASSERT_TRUE(retRel2);

  buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);
  BufferManager::instance().reset();

}

void run(TupleBufferPtr* ptr) {
  *ptr = BufferManager::instance().getFixedSizeBuffer();
  std::cout << "unreleased blocking buffer" << std::endl;
}

void run_and_release(size_t id, size_t sleeptime) {
  TupleBufferPtr ptr = BufferManager::instance().getFixedSizeBuffer();
  std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime));
  bool retRel = BufferManager::instance().releaseBuffer(ptr);
  ASSERT_TRUE(retRel);
}

TEST_F(BufferManagerTest, getBuffer_afterRelease) {
  std::vector<TupleBufferPtr> buffers;

  size_t buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);

  //get all buffers
  for (size_t i = 0; i < buffers_count; ++i) {
    buffers.push_back(BufferManager::instance().getFixedSizeBuffer());
  }

  //start a thread that is blocking waiting on the queue
  TupleBufferPtr t;
  std::thread t1(run, &t);

  std::this_thread::sleep_for(std::chrono::seconds(1));

  bool retRel = BufferManager::instance().releaseBuffer(buffers.back());
  ASSERT_TRUE(retRel);

  buffers.pop_back();
  t1.join();

  buffers.push_back(t);
  size_t i = 0;
  for (auto& buf : buffers) {
    std::cout << "iteration =" << i++ << std::endl;
    bool retRel = BufferManager::instance().releaseBuffer(buf);
     ASSERT_TRUE(retRel);

  }

  buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);
}

TEST_F(BufferManagerTest, get_and_release) {
  size_t buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
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

  buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
  ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(buffers_free, buffers_managed);
  BufferManager::instance().printStatistics();
}

#ifndef NO_RACE_CHECK
TEST_F(BufferManagerTest, getBuffer_race) {
  for (int i = 0; i < 100; ++i) {
    std::cout << "Run getBuffer_race " << i << std::endl;

    std::vector<TupleBufferPtr> buffers;

    size_t buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
    size_t buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    ASSERT_EQ(buffers_free, buffers_managed);

    for (size_t j = 0; j < buffers_count; ++j) {
      buffers.push_back(BufferManager::instance().getFixedSizeBuffer());
    }

    TupleBufferPtr buffer_threads[buffers_managed];
    std::thread threads[buffers_managed];
    for (size_t j = 0; j < buffers_count; ++j) {
      threads[j] = std::thread(run, &buffer_threads[j]);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    for (size_t j = 0; j < buffers_managed; ++j) {
      bool retRel = BufferManager::instance().releaseBuffer(buffers.back());
           ASSERT_TRUE(retRel);
      buffers.pop_back();
    }

    for (auto& thread : threads) {
      thread.join();
    }

    std::set<TupleBufferPtr> setOfTupleBufferPtr;
    for (auto& b : buffer_threads) {
      setOfTupleBufferPtr.insert(b);
    }
    ASSERT_EQ(10, setOfTupleBufferPtr.size());

    for (auto& b : buffer_threads) {
      bool retRel = BufferManager::instance().releaseBuffer(b);
           ASSERT_TRUE(retRel);
    }
    buffers_count = BufferManager::instance().getNumberOfFixedBuffers();
    buffers_free = BufferManager::instance().getNumberOfFreeFixedBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    ASSERT_EQ(buffers_free, buffers_managed);
  }
}
#endif
#endif
/**
 * Var Size Buffer tests
 */
TEST_F(BufferManagerTest, add_and_remove_Var_Buffer_simple) {
  size_t buffers_count = BufferManager::instance().getNumberOfVarBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeVarBuffers();
  ASSERT_EQ(buffers_count, 0);
  ASSERT_EQ(buffers_free, 0);

  TupleBufferPtr buffer = BufferManager::instance().createVarSizeBuffer(100);
  ASSERT_EQ(buffer->getBufferSizeInBytes(), 100);

  buffers_count = BufferManager::instance().getNumberOfVarBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeVarBuffers();
  size_t expected = 1;
  ASSERT_EQ(buffers_count, expected);
  ASSERT_EQ(buffers_free, expected);

  bool retRelease = BufferManager::instance().releaseBuffer(buffer);
  ASSERT_TRUE(retRelease);
//  bool retRemove = BufferManager::instance().removeBuffer(buffer);
//  ASSERT_TRUE(retRemove);

  buffers_count = BufferManager::instance().getNumberOfVarBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeVarBuffers();
  ASSERT_EQ(buffers_count, 1);
  ASSERT_EQ(buffers_free, 1);
  BufferManager::instance().reset();
}

TEST_F(BufferManagerTest, get_Existing_Var_Buffer) {
  size_t buffers_count = BufferManager::instance().getNumberOfVarBuffers();
  size_t buffers_free = BufferManager::instance().getNumberOfFreeVarBuffers();
  ASSERT_EQ(buffers_count, 0);
  ASSERT_EQ(buffers_free, 0);

  TupleBufferPtr buffer = BufferManager::instance().getVarSizeBufferLargerThan(
      100);
  ASSERT_EQ(buffer, nullptr);

  TupleBufferPtr buffer1 = BufferManager::instance().createVarSizeBuffer(80);
  ASSERT_EQ(buffer1->getBufferSizeInBytes(), 80);

  TupleBufferPtr buffer2 = BufferManager::instance().getVarSizeBufferLargerThan(
      100);
  ASSERT_EQ(buffer2, nullptr);

  TupleBufferPtr buffer3 = BufferManager::instance().getVarSizeBufferLargerThan(
      60);
  ASSERT_NE(buffer3->getBufferSizeInBytes(), 60);

  buffers_count = BufferManager::instance().getNumberOfVarBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeVarBuffers();
  size_t expected = 1;
  ASSERT_EQ(buffers_count, expected);
  ASSERT_EQ(buffers_free, 0);

  bool retRel = BufferManager::instance().releaseBuffer(buffer3);
       ASSERT_TRUE(retRel);

  buffers_count = BufferManager::instance().getNumberOfVarBuffers();
  buffers_free = BufferManager::instance().getNumberOfFreeVarBuffers();
  ASSERT_EQ(buffers_count, 1);
  ASSERT_EQ(buffers_free, 1);
}
}
