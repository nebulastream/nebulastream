#include <map>
#include <vector>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <thread>

#include <gtest/gtest.h>

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
            iotdb::BufferManager::instance();
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
            while (run) {

                TupleBufferPtr buf = buffer_manager.getBuffer();

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
        int size = 3 * 4 * 1024;
        const int capacity = 3;
        const int num_to_release = 2;
        iotdb::BufferManager::instance();

        std::vector<TupleBufferPtr> buffers;
        std::cout << "==========Get Buffer==========" << std::endl;
        for (int i = 0; i < capacity; i++) {
            buffers.push_back(BufferManager::instance().getBuffer());
            std::cout << "buffer " << i << ": " << buffers[i]->buffer << std::endl;
        }

        std::cout << "==========Release Buffer==========" << std::endl;
        for (int i = 0; i < capacity; i++) {
            if (buffers[i]->buffer) { // make sure not nullptr
                std::cout << "try to release buffer " << buffers[i]->buffer << std::endl;
                BufferManager::instance().releaseBuffer(buffers[i]);
            } else {
                std::cout << "empty buffer found!!!" << std::endl;
            }
        }

        std::cout << "==========Get Buffer Again==========" << std::endl;
        for (int i = capacity - num_to_release; i < capacity; i++) {
            buffers.push_back(BufferManager::instance().getBuffer());
            std::cout << "buffer " << i << ": " << buffers[i]->buffer << std::endl;
        }

        std::cout << "==========Release Buffer==========" << std::endl;
        for (int i = 0; i < capacity; i++) {
            std::cout << "buffer " << i << ": " << buffers[i]->buffer << std::endl;
            if (buffers[i]->buffer) { // make sure not nullptr
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

/*
int main(int argc, const char *argv[]) {
    iotdb::BufferManager::instance();

    std::cout << "Test Single Thread" << std::endl;
    iotdb::testSingleThread();
    std::cout << "Test Multiple Thread" << std::endl;
//  iotdb::testMultiThreads();

    return 0;
}
*/

namespace iotdb {
    class BufferManagerTest : public testing::Test {
    public:
        static void SetUpTestCase() { std::cout << "Setup BufferMangerTest test class." << std::endl; }


        static void TearDownTestCase() { std::cout << "Tear down BufferManager test class." << std::endl; }

        const size_t buffers_managed = 10;
        const size_t buffer_size = 4 * 1024;

    };

    TEST_F(BufferManagerTest, addBuffer_simple) {
        size_t buffers_count = BufferManager::instance().getNumberOfBuffers();
        ASSERT_EQ(buffers_count, buffers_managed);

        BufferManager::instance().addBuffer();
        buffers_count = BufferManager::instance().getNumberOfBuffers();
        ASSERT_EQ(buffers_count, buffers_managed + 1);
    }

    TEST_F(BufferManagerTest, getBuffer_simple) {
        for (int i = 0; i < BufferManager::instance().getNumberOfBuffers(); ++i) {
            TupleBufferPtr buf = BufferManager::instance().getBuffer();

            ASSERT_TRUE(buf->buffer != nullptr);
            ASSERT_EQ(buf->buffer_size, buffer_size);
            ASSERT_EQ(buf->num_tuples, 0);
            ASSERT_EQ(buf->tuple_size_bytes, 0);
        }
    }

    TEST_F(BufferManagerTest, release_Buffer) {
        size_t size = BufferManager::instance().getNumberOfBuffers();
        std::vector<TupleBufferPtr> buffers;

        ASSERT_EQ(size, BufferManager::instance().getNumberOfFreeBuffers());

        for (size_t i = 0; i < size; ++i) {
            buffers.push_back(BufferManager::instance().getBuffer());
        }
        ASSERT_EQ(0, BufferManager::instance().getNumberOfFreeBuffers());

        for (auto &ptr : buffers) {
            BufferManager::instance().releaseBuffer(ptr);
        }

        ASSERT_EQ(size, BufferManager::instance().getNumberOfFreeBuffers());
    }

    void run(TupleBufferPtr *ptr) {
        *ptr = BufferManager::instance().getBuffer();
        ASSERT_TRUE(ptr != nullptr);
    }

    TEST_F(BufferManagerTest, getBuffer_afterRelease) {
        size_t size = BufferManager::instance().getNumberOfBuffers();
        std::vector<TupleBufferPtr> buffers;

        for (size_t i = 0; i < size; ++i) {
            buffers.push_back(BufferManager::instance().getBuffer());
        }

        TupleBufferPtr t;
        std::thread t1(run, &t);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        BufferManager::instance().releaseBuffer(buffers.front());
        t1.join();
    }


    TEST_F(BufferManagerTest, getBuffer_race) {
        size_t size = BufferManager::instance().getNumberOfBuffers();
        std::vector<TupleBufferPtr> buffers;

        for (size_t i = 0; i < size - 2; ++i) {
            buffers.push_back(BufferManager::instance().getBuffer());
        }

        TupleBufferPtr buffer_thread1;
        TupleBufferPtr buffer_thread2;
        std::thread t1(run, &buffer_thread1);
        std::thread t2(run, &buffer_thread2);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        BufferManager::instance().releaseBuffer(buffers.front());
        std::this_thread::sleep_for(std::chrono::seconds(1));
        BufferManager::instance().releaseBuffer(buffers.front());
        t1.join();
        t2.join();

        ASSERT_NE(buffer_thread1.get(), buffer_thread2.get());
    }
}