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

    TEST_F(BufferManagerTest, add_and_remove_Buffer_simple) {
        size_t buffers_count = BufferManager::instance().getNumberOfBuffers();
        size_t buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
        ASSERT_EQ(buffers_count, buffers_managed);
        ASSERT_EQ(buffers_free, buffers_managed);

        BufferManager::instance().addBuffer();
        TupleBufferPtr buffer = BufferManager::instance().getBuffer();

        buffers_count = BufferManager::instance().getNumberOfBuffers();
        buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
        ASSERT_EQ(buffers_count, buffers_managed + 1);
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

        for (int i = 1; i <= BufferManager::instance().getNumberOfBuffers(); ++i) {
            TupleBufferPtr buf = BufferManager::instance().getBuffer();

            ASSERT_TRUE(buf->buffer != nullptr);
            ASSERT_EQ(buf->buffer_size, buffer_size);
            ASSERT_EQ(buf->num_tuples, 0);
            ASSERT_EQ(buf->tuple_size_bytes, 0);

            buffers_count = BufferManager::instance().getNumberOfBuffers();
            buffers_free = BufferManager::instance().getNumberOfFreeBuffers();
            ASSERT_EQ(buffers_count, buffers_managed);
            ASSERT_EQ(buffers_free, buffers_managed - i);

            buffers.push_back(buf);
        }

        int i = 1;
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

    void run(TupleBufferPtr *ptr) {
        *ptr = BufferManager::instance().getBuffer();
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


    TEST_F(BufferManagerTest, getBuffer_race) {
        for (int i = 0; i < 100; ++i) {

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
}