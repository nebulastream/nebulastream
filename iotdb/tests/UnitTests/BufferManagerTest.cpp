#include <map>
#include <vector>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <log4cxx/appender.h>
#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <Runtime/BufferManager.hpp>


namespace iotdb {
    class BufferManagerTest : public testing::Test {
    public:
        static void SetUpTestCase() {
            setupLogging();
            IOTDB_INFO("Setup BufferMangerTest test class.");
            BufferManager::instance().setNumberOfBuffers(10);
        }
        static void TearDownTestCase() { std::cout << "Tear down BufferManager test class." << std::endl; }


        const size_t buffers_managed = 10;
        const size_t buffer_size = 4 * 1024;
    protected:
        static void setupLogging()
        {
            // create PatternLayout
            log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

            // create FileAppender
            LOG4CXX_DECODE_CHAR(fileName, "BufferManagerTest.log");
            log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

            // create ConsoleAppender
            log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

            // set log level
            // logger->setLevel(log4cxx::Level::getDebug());
            logger->setLevel(log4cxx::Level::getInfo());

            // add appenders and other will inherit the settings
            logger->addAppender(file);
            logger->addAppender(console);
        }

    };

    TEST_F(BufferManagerTest, add_and_remove_Buffer_simple) {
        size_t buffers_count = BufferManager::instance().getNumberOfBuffers();
        size_t buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
        ASSERT_EQ(buffers_count, buffers_managed);
        ASSERT_EQ(buffers_free, buffers_managed);

        BufferManager::instance().addBuffer();
        TupleBufferPtr buffer = BufferManager::instance().getBuffer();

        buffers_count = BufferManager::instance().getNumberOfBuffers();
        buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
        size_t expected = buffers_managed + 1;
        ASSERT_EQ(buffers_count, expected);
        ASSERT_EQ(buffers_free, buffers_managed);

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
            TupleBufferPtr buf = BufferManager::instance().getBuffer();
            size_t expected = 0;
            ASSERT_TRUE(buf->buffer != nullptr);
            ASSERT_EQ(buf->buffer_size, buffer_size);
            ASSERT_EQ(buf->num_tuples, expected);
            ASSERT_EQ(buf->tuple_size_bytes, expected);

            buffers_count = BufferManager::instance().getNumberOfBuffers();
            buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
            ASSERT_EQ(buffers_count, buffers_managed);
            expected = buffers_managed - i;
            ASSERT_EQ(buffers_free, expected );

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

        BufferManager::instance().setNumberOfBuffers(5);
        buffers_count = BufferManager::instance().getNumberOfBuffers();
        buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
        size_t expected = 5;
        ASSERT_EQ(buffers_count, expected);
        ASSERT_EQ(buffers_free, expected);

        BufferManager::instance().setNumberOfBuffers(buffers_managed);
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

        BufferManager::instance().setBufferSize(buffer_size * 2);
        TupleBufferPtr buf = BufferManager::instance().getBuffer();
        ASSERT_EQ(buf->buffer_size, 2 * buffer_size);
        BufferManager::instance().releaseBuffer(buf);

        BufferManager::instance().setBufferSize(buffer_size);
        buf = BufferManager::instance().getBuffer();
        ASSERT_EQ(buf->buffer_size, buffer_size);
        BufferManager::instance().releaseBuffer(buf);


        buffers_count = BufferManager::instance().getNumberOfBuffers();
        buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
        ASSERT_EQ(buffers_count, buffers_managed);
        ASSERT_EQ(buffers_free, buffers_managed);

    }

    void run(TupleBufferPtr *ptr) {
        *ptr = BufferManager::instance().getBuffer();
    }

    void run_and_release(size_t id, size_t sleeptime) {
        TupleBufferPtr ptr = BufferManager::instance().getBuffer();
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
            buffers.push_back(BufferManager::instance().getBuffer());
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

        for(size_t i = 0; i < 1000; i++) {
            threads.emplace_back(run_and_release, i, sleeptime(mt));
        }

        for(auto& thread :threads) {
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
                buffers.push_back(BufferManager::instance().getBuffer());
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