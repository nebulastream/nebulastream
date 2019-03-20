#include <map>
#include <vector>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <thread>

#include <gtest/gtest.h>

#include <Runtime/BufferManager.hpp>

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
}