/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <map>
#include <vector>

#include <Runtime/BufferManager.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <cstdlib>
#include <deque>
#include <future>
#include <gtest/gtest.h>
#include <iostream>
#include <thread>

namespace NES {
using Runtime::TupleBuffer;
const size_t buffers_managed = 1024;
const size_t buffer_size = 32 * 1024;

class BufferManagerTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::setupLogging("BufferManagerTest.log", NES::LOG_DEBUG); }
};

TEST_F(BufferManagerTest, initializedBufferManager) {
    auto hardwareManager = std::make_shared<Runtime::HardwareManager>();
    auto bufferManager =
        std::make_shared<Runtime::BufferManager>(buffer_size, buffers_managed, hardwareManager->getGlobalAllocator());
    size_t buffers_count = bufferManager->getNumOfPooledBuffers();
    size_t buffers_free = bufferManager->getAvailableBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    ASSERT_EQ(buffers_free, buffers_managed);
}

#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
TEST_F(BufferManagerTest, initializedBufferManagerWithNuma) {
    auto hardwareManager = std::make_shared<Runtime::HardwareManager>();
    auto bufferManager =
        std::make_shared<Runtime::BufferManager>(buffer_size, buffers_managed, hardwareManager->getNumaAllactor(0));
    size_t buffers_count = bufferManager->getNumOfPooledBuffers();
    size_t buffers_free = bufferManager->getAvailableBuffers();
    ASSERT_EQ(buffers_count, buffers_managed);
    ASSERT_EQ(buffers_free, buffers_managed);
}
#endif

TEST_F(BufferManagerTest, initializedBufferManagerAlignment16) {
    auto bufferManager = std::make_shared<Runtime::BufferManager>(buffer_size,
                                                                  buffers_managed,
                                                                  std::make_shared<Runtime::NesDefaultMemoryAllocator>(),
                                                                  16);
    auto buffer = bufferManager->getBufferBlocking();
    ASSERT_TRUE(reinterpret_cast<uintptr_t>(buffer.getBuffer()) % 16 == 0);
}

TEST_F(BufferManagerTest, initializedBufferManagerAlignment64) {
    auto bufferManager = std::make_shared<Runtime::BufferManager>(buffer_size,
                                                                  buffers_managed,
                                                                  std::make_shared<Runtime::NesDefaultMemoryAllocator>(),
                                                                  64);
    auto buffer = bufferManager->getBufferBlocking();
    ASSERT_TRUE(reinterpret_cast<uintptr_t>(buffer.getBuffer()) % 64 == 0);
}

TEST_F(BufferManagerTest, singleThreadedBufferRecycling) {
    auto bufferManager = std::make_shared<Runtime::BufferManager>(buffer_size, buffers_managed);
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
    auto bufferManager = std::make_shared<Runtime::BufferManager>(buffer_size, buffers_managed);
    auto buffer0 = bufferManager->getUnpooledBuffer(16384);
    ASSERT_EQ(bufferManager->getNumOfUnpooledBuffers(), 1u);
    {
        auto buffer0 = bufferManager->getUnpooledBuffer(16384);
        ASSERT_EQ(bufferManager->getNumOfUnpooledBuffers(), 2UL);
    }
    ASSERT_EQ(bufferManager->getNumOfUnpooledBuffers(), 2u);
}

TEST_F(BufferManagerTest, singleThreadedManyBufferRecyclingUnpooled) {
    auto bufferManager = std::make_shared<Runtime::BufferManager>(1024, 1);
    for (int i = 0; i < 500; i++) {
        auto buffer0 = bufferManager->getUnpooledBuffer(16384);
        ASSERT_EQ(bufferManager->getNumOfUnpooledBuffers(), 1U);
    }
    ASSERT_EQ(bufferManager->getNumOfUnpooledBuffers(), 1u);
}

TEST_F(BufferManagerTest, getBufferAfterRelease) {
    auto bufferManager = std::make_shared<Runtime::BufferManager>(buffer_size, buffers_managed);

    std::vector<TupleBuffer> buffers;

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    // get all buffers
    for (size_t i = 0; i < buffers_managed; ++i) {
        buffers.push_back(bufferManager->getBufferBlocking());
    }
    std::promise<bool> promise0;
    std::promise<bool> promise1;
    auto f0 = promise0.get_future();
    // start a thread that is blocking waiting on the queue
    std::thread t1([&promise0, &promise1, &bufferManager]() {
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
    auto bufferManager = std::make_shared<Runtime::BufferManager>(buffer_size, buffers_managed);

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> threads;
    threads.reserve(4);
    for (uint32_t i = 0ul; i < 4ul; ++i) {
        threads.emplace_back([&bufferManager]() {
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
    auto bufferManager = std::make_shared<Runtime::BufferManager>(buffer_size, buffers_managed);
    std::atomic<size_t> numBuffers = buffers_managed;

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 5'000;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    prod_threads.reserve(producer_threads);
    for (uint32_t i = 0ul; i < producer_threads; ++i) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, &bufferManager, &numBuffers]() {
            for (uint32_t j = 0ul; j < max_buffer; ++j) {
                std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
                auto buf = bufferManager->getBufferBlocking();
                numBuffers.fetch_sub(1);
                for (uint32_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
                    buf.getBuffer<uint32_t>()[k] = k;
                }
                buf.getBuffer<uint32_t>()[buffer_size / sizeof(uint32_t) - 1] = 0;
                lock.lock();
                workQueue.emplace_back(std::move(buf));
                cvar.notify_all();
                lock.unlock();
            }
        });
    }
    con_threads.reserve(consumer_threads);
    for (uint32_t i = 0ul; i < consumer_threads; ++i) {
        con_threads.emplace_back([&workQueue, &mutex, &cvar, &numBuffers]() {
            std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
            while (true) {
                lock.lock();
                while (workQueue.empty()) {
                    cvar.wait(lock);
                }
                auto buf = workQueue.front();
                workQueue.pop_front();
                lock.unlock();
                if (buf.getBuffer<uint32_t>()[buffer_size / sizeof(uint32_t) - 1] == max_buffer) {
                    numBuffers.fetch_add(1);
                    break;
                }
                for (uint32_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
                    ASSERT_EQ(buf.getBuffer<uint32_t>()[k], k);
                }
                numBuffers.fetch_add(1);
            }
        });
    }
    for (auto& t : prod_threads) {
        t.join();
    }
    for (uint32_t j = 0ul; j < consumer_threads; ++j) {
        std::unique_lock<std::mutex> lock(mutex);
        auto buf = bufferManager->getBufferBlocking();
        numBuffers.fetch_sub(1);
        buf.getBuffer<uint32_t>()[buffer_size / sizeof(uint32_t) - 1] = max_buffer;
        workQueue.emplace_back(std::move(buf));
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    std::unique_lock<std::mutex> lock(mutex);
    workQueue.clear();
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(numBuffers.load(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);
}

TupleBuffer getBufferTimeout(const std::shared_ptr<Runtime::BufferManager>& bufferManager, std::chrono::milliseconds&& timeout) {
    std::optional<TupleBuffer> opt;
    while (!(opt = bufferManager->getBufferTimeout(timeout)).has_value()) {
        // nop
    }
    return *opt;
}

TEST_F(BufferManagerTest, bufferManagerMtProducerConsumerTimeout) {
    using namespace std::chrono_literals;
    auto bufferManager = std::make_shared<Runtime::BufferManager>(buffer_size, buffers_managed);

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 25'000;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    prod_threads.reserve(producer_threads);
    for (auto i = 0UL; i < producer_threads; ++i) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, bufferManager]() {
            for (uint32_t j = 0ul; j < max_buffer; ++j) {
                std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
                auto buf = getBufferTimeout(bufferManager, 100ms);
                for (uint32_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
                    buf.getBuffer<uint32_t>()[k] = k;
                }
                buf.getBuffer<uint32_t>()[buffer_size / sizeof(uint32_t) - 1] = 0;
                lock.lock();
                workQueue.emplace_back(std::move(buf));
                cvar.notify_all();
                lock.unlock();
            }
        });
    }
    con_threads.reserve(consumer_threads);
    for (auto i = 0UL; i < consumer_threads; ++i) {
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
                auto ctrl_val = buf.getBuffer<uint32_t>()[buffer_size / sizeof(uint32_t) - 1];
                if (ctrl_val == max_buffer) {
                    break;
                }
                for (uint32_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
                    ASSERT_EQ(buf.getBuffer<uint32_t>()[k], k);
                }
            }
        });
    }
    for (auto& t : prod_threads) {
        t.join();
    }
    for (uint32_t j = 0ul; j < consumer_threads; ++j) {
        std::unique_lock<std::mutex> lock(mutex);
        auto buf = bufferManager->getBufferBlocking();
        auto* writer = buf.getBuffer<uint32_t>();
        for (uint32_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
            writer[k] = k;
        }
        writer[buffer_size / sizeof(uint32_t) - 1] = max_buffer;
        workQueue.emplace_back(std::move(buf));
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    std::unique_lock<std::mutex> lock(mutex);
    workQueue.clear();
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);
}

std::optional<TupleBuffer> getBufferNoBlocking(Runtime::BufferManager& bufferManager) {
    size_t retries = 0;
    while (true) {
        auto optBuffer = bufferManager.getBufferNoBlocking();
        if (optBuffer.has_value()) {
            return optBuffer;
        }
        usleep(100 * 1000);
        if (++retries == 1'000'000'000) {
            NES_WARNING("Too many retries");
            return std::nullopt;
        }
    }
    return std::nullopt;
}

TEST_F(BufferManagerTest, bufferManagerMtProducerConsumerNoblocking) {
    auto bufferManager = std::make_shared<Runtime::BufferManager>(buffer_size, buffers_managed);

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 1'000;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    prod_threads.reserve(producer_threads);
    for (uint32_t i = 0ul; i < producer_threads; ++i) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, bufferManager]() {
            for (auto j = 0L; j < max_buffer; ++j) {
                std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
                auto bufOpt = getBufferNoBlocking(*bufferManager);
                EXPECT_TRUE(bufOpt.has_value());
                auto buf = *bufOpt;
                auto* data = buf.getBuffer<uint32_t>();
                for (size_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
                    data[k] = k;
                }
                data[buffer_size / sizeof(uint32_t) - 1] = 0;
                lock.lock();
                workQueue.emplace_back(std::move(buf));
                cvar.notify_all();
                lock.unlock();
            }
        });
    }
    con_threads.reserve(consumer_threads);
    for (uint32_t i = 0ul; i < consumer_threads; ++i) {
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
                auto* data = buf.getBuffer<uint32_t>();
                for (size_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
                    ASSERT_EQ(data[k], k);
                }
                if (data[buffer_size / sizeof(uint32_t) - 1] == max_buffer) {
                    break;
                }
            }
        });
    }
    for (auto& t : prod_threads) {
        t.join();
    }
    for (auto j = 0UL; j < consumer_threads; ++j) {
        std::unique_lock<std::mutex> lock(mutex);
        auto buf = bufferManager->getBufferBlocking();
        auto* data = buf.getBuffer<uint32_t>();
        for (size_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
            data[k] = k;
        }
        data[buffer_size / sizeof(uint32_t) - 1] = max_buffer;
        workQueue.emplace_back(std::move(buf));
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    std::unique_lock<std::mutex> lock(mutex);
    workQueue.clear();
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);
}

TEST_F(BufferManagerTest, bufferManagerMtProducerConsumerLocalPool) {
    auto bufferManager = std::make_shared<Runtime::BufferManager>(buffer_size, buffers_managed);

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 10'000;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    auto localPool = bufferManager->createLocalBufferPool(32);
    prod_threads.reserve(producer_threads);
    for (uint32_t i = 0ul; i < producer_threads; i++) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, localPool]() {
            for (auto j = 0UL; j < max_buffer; ++j) {
                std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
                auto buf = localPool->getBufferBlocking();
                auto* writer = buf.getBuffer<uint32_t>();
                for (uint32_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
                    writer[k] = k;
                }
                writer[buffer_size / sizeof(uint32_t) - 1] = 0;
                lock.lock();
                workQueue.emplace_back(std::move(buf));
                cvar.notify_all();
                lock.unlock();
            }
        });
    }
    con_threads.reserve(consumer_threads);
    for (uint32_t i = 0ul; i < consumer_threads; i++) {
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
                auto* reader = buf.getBuffer<uint32_t>();
                for (uint32_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
                    ASSERT_EQ(reader[k], k);
                }
                if (reader[buffer_size / sizeof(uint32_t) - 1] == max_buffer) {
                    break;
                }
            }
        });
    }
    for (auto& t : prod_threads) {
        t.join();
    }
    for (uint32_t j = 0ul; j < consumer_threads; ++j) {
        std::unique_lock<std::mutex> lock(mutex);
        auto buf = bufferManager->getBufferBlocking();
        auto* writer = buf.getBuffer<uint32_t>();
        for (uint32_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
            writer[k] = k;
        }
        writer[buffer_size / sizeof(uint32_t) - 1] = max_buffer;
        workQueue.emplace_back(std::move(buf));
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    std::unique_lock<std::mutex> lock(mutex);
    localPool->destroy();
    workQueue.clear();
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);
}

TEST_F(BufferManagerTest, bufferManagerMtProducerConsumerLocalPoolWithExtraAllocation) {
    auto bufferManager = std::make_shared<Runtime::BufferManager>(buffer_size, buffers_managed);

    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> con_threads;
    std::mutex mutex;
    std::deque<TupleBuffer> workQueue;
    std::condition_variable cvar;

    constexpr uint32_t max_buffer = 2;
    constexpr uint32_t producer_threads = 3;
    constexpr uint32_t consumer_threads = 4;

    prod_threads.reserve(producer_threads);
    for (uint32_t i = 0ul; i < producer_threads; ++i) {
        prod_threads.emplace_back([&workQueue, &mutex, &cvar, &bufferManager]() {
            for (uint32_t j = 0ul; j < max_buffer; ++j) {
                std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
                auto buf = bufferManager->getBufferBlocking();
                auto* writer = buf.getBuffer<uint32_t>();
                for (uint32_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
                    writer[k] = k;
                }
                writer[buffer_size / sizeof(uint32_t) - 1] = 0;
                lock.lock();
                workQueue.emplace_back(std::move(buf));
                cvar.notify_all();
                lock.unlock();
            }
        });
    }
    auto localPool = bufferManager->createLocalBufferPool(32);
    con_threads.reserve(consumer_threads);
    for (uint32_t i = 0ul; i < consumer_threads; ++i) {
        con_threads.emplace_back([&workQueue, &mutex, &cvar, localPool]() {
            std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
            while (true) {
                lock.lock();
                while (workQueue.empty()) {
                    cvar.wait(lock);
                }
                TupleBuffer buf = workQueue.front();
                workQueue.pop_front();
                lock.unlock();
                auto copied_buf = localPool->getBufferBlocking();
                memcpy(copied_buf.getBuffer(), buf.getBuffer(), buf.getBufferSize());
                auto* reader = copied_buf.getBuffer<uint32_t>();
                for (uint32_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
                    ASSERT_EQ(reader[k], k);
                }
                if (reader[buffer_size / sizeof(uint32_t) - 1] == max_buffer) {
                    break;
                }
            }
        });
    }
    for (auto& t : prod_threads) {
        t.join();
    }
    for (uint32_t j = 0ul; j < consumer_threads; ++j) {
        std::unique_lock<std::mutex> lock(mutex);
        auto buf = bufferManager->getBufferBlocking();
        auto* writer = buf.getBuffer<uint32_t>();
        for (uint32_t k = 0; k < (buffer_size / sizeof(uint32_t) - 1); ++k) {
            writer[k] = k;
        }
        writer[buffer_size / sizeof(uint32_t) - 1] = max_buffer;
        workQueue.emplace_back(std::move(buf));
        cvar.notify_all();
    }
    for (auto& t : con_threads) {
        t.join();
    }
    std::unique_lock<std::mutex> lock(mutex);
    workQueue.clear();
    localPool->destroy();
    ASSERT_EQ(bufferManager->getNumOfPooledBuffers(), buffers_managed);
    ASSERT_EQ(bufferManager->getAvailableBuffers(), buffers_managed);
}

}// namespace NES
