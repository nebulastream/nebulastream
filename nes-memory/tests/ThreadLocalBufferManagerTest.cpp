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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <latch>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/ThreadLocalBufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>

namespace NES
{

/// The buffers are tiny, so that a pool with thread local caches (which has to be large) needs little memory.
class ThreadLocalBufferManagerTest : public ::testing::Test
{
protected:
    static constexpr uint32_t BUFFER_SIZE = 64;
    static constexpr uint32_t NUM_BUFFERS = 65536;
    static constexpr size_t TOTAL_MEMORY_IN_BYTES = 2 * static_cast<size_t>(NUM_BUFFERS) * BUFFER_SIZE;

    static std::shared_ptr<BufferManager> createPool(const bool threadLocalCaches = true)
    {
        return BufferManager::create(
            TOTAL_MEMORY_IN_BYTES,
            0.5,
            NES::BufferAlignment{64},
            BUFFER_SIZE,
            std::make_shared<NesDefaultMemoryAllocator>(),
            threadLocalCaches);
    }
};

/// A pool with thread local caches can also be used directly, like any other BufferManager.
TEST_F(ThreadLocalBufferManagerTest, PoolWithCachesCanBeUsedDirectly)
{
    auto pool = createPool();
    ASSERT_TRUE(pool->usesThreadLocalCaches());
    ASSERT_EQ(pool->getNumOfPooledBuffers(), NUM_BUFFERS);
    EXPECT_EQ(pool->getNumberOfAvailableBuffers(), NUM_BUFFERS);
    {
        auto buffer = pool->getBufferBlocking();
        EXPECT_EQ(buffer.getBufferSize(), BUFFER_SIZE);
        EXPECT_EQ(pool->getNumberOfAvailableBuffers(), NUM_BUFFERS - 1);
    }
    EXPECT_EQ(pool->getNumberOfAvailableBuffers(), NUM_BUFFERS);
    pool->destroy();
}

TEST_F(ThreadLocalBufferManagerTest, PoolWithoutCachesDoesNotUseThem)
{
    auto pool = createPool(false);
    EXPECT_FALSE(pool->usesThreadLocalCaches());
    {
        auto buffer = pool->getBufferBlocking();
    }
    EXPECT_EQ(pool->getNumberOfAvailableBuffers(), NUM_BUFFERS);
    pool->destroy();
}

/// One thread can take every buffer of the pool through its provider, and the pool is exhausted afterwards.
TEST_F(ThreadLocalBufferManagerTest, SingleThreadCanTakeEveryBuffer)
{
    auto pool = createPool();
    {
        ThreadLocalBufferManager provider(pool);
        EXPECT_EQ(provider.getBufferManagerType(), BufferManagerType::LOCAL);
        EXPECT_EQ(provider.getBufferSize(), BUFFER_SIZE);
        std::vector<TupleBuffer> held;
        held.reserve(NUM_BUFFERS);
        for (uint32_t i = 0; i < NUM_BUFFERS; ++i)
        {
            auto buffer = provider.getBufferNoBlocking();
            ASSERT_TRUE(buffer.has_value()) << "buffer " << i << " of " << NUM_BUFFERS << " is missing";
            held.emplace_back(std::move(buffer.value()));
        }
        EXPECT_FALSE(provider.getBufferNoBlocking().has_value());
        EXPECT_FALSE(pool->getBufferNoBlocking().has_value());
        EXPECT_EQ(pool->getNumberOfAvailableBuffers(), 0U);
        held.clear();
        EXPECT_EQ(pool->getNumberOfAvailableBuffers(), NUM_BUFFERS);
    }
    EXPECT_EQ(pool->getNumberOfAvailableBuffers(), NUM_BUFFERS);
    pool->destroy();
}

/// Buffers that a thread without a provider returns go back to the pool, and the provider's buffers are still usable by others.
TEST_F(ThreadLocalBufferManagerTest, BuffersAreReturnedByThreadsWithoutProvider)
{
    auto pool = createPool();
    std::vector<TupleBuffer> held;
    {
        ThreadLocalBufferManager provider(pool);
        for (uint32_t i = 0; i < 1000; ++i)
        {
            held.emplace_back(provider.getBufferBlocking());
        }
    }
    held.clear();
    EXPECT_EQ(pool->getNumberOfAvailableBuffers(), NUM_BUFFERS);
    /// All of them can be taken again, also by the pool itself.
    std::vector<TupleBuffer> all;
    for (uint32_t i = 0; i < NUM_BUFFERS; ++i)
    {
        auto buffer = pool->getBufferNoBlocking();
        ASSERT_TRUE(buffer.has_value()) << "buffer " << i << " of " << NUM_BUFFERS << " is missing";
        all.emplace_back(std::move(buffer.value()));
    }
    all.clear();
    pool->destroy();
}

/// Producer threads only take buffers and consumer threads only return them, each with its own provider, so every buffer is returned by
/// another thread than the one that took it. Every buffer carries a tag that must survive until it is returned, which fails if a
/// buffer is handed out twice.
TEST_F(ThreadLocalBufferManagerTest, ProducersAndConsumersExchangeBuffers)
{
    auto pool = createPool();
    constexpr size_t numProducers = 4;
    constexpr size_t numConsumers = 4;
    constexpr size_t buffersPerProducer = 300000;
    constexpr size_t maxInFlight = 20000;

    std::mutex queueMutex;
    std::vector<std::pair<TupleBuffer, uint64_t>> inFlight;
    std::atomic<size_t> producersDone{0};
    std::atomic<uint64_t> corrupted{0};
    std::atomic<uint64_t> returned{0};
    std::latch allThreadsStarted(numProducers + numConsumers);

    {
        std::vector<std::jthread> threads;
        for (size_t producer = 0; producer < numProducers; ++producer)
        {
            threads.emplace_back(
                [&, producer]
                {
                    ThreadLocalBufferManager provider(pool);
                    allThreadsStarted.arrive_and_wait();
                    for (size_t i = 0; i < buffersPerProducer; ++i)
                    {
                        auto buffer = provider.getBufferNoBlocking();
                        while (!buffer.has_value())
                        {
                            /// All buffers are in flight, wait for the consumers to return some.
                            std::this_thread::yield();
                            buffer = provider.getBufferNoBlocking();
                        }
                        const uint64_t tag = (static_cast<uint64_t>(producer) << 48) | i;
                        std::memcpy(buffer->getAvailableMemoryArea<std::byte>().data(), &tag, sizeof(tag));
                        while (true)
                        {
                            const std::scoped_lock lock(queueMutex);
                            if (inFlight.size() < maxInFlight)
                            {
                                inFlight.emplace_back(std::move(buffer.value()), tag);
                                break;
                            }
                        }
                    }
                    ++producersDone;
                });
        }
        for (size_t consumer = 0; consumer < numConsumers; ++consumer)
        {
            threads.emplace_back(
                [&]
                {
                    ThreadLocalBufferManager provider(pool);
                    allThreadsStarted.arrive_and_wait();
                    while (true)
                    {
                        std::vector<std::pair<TupleBuffer, uint64_t>> taken;
                        {
                            const std::scoped_lock lock(queueMutex);
                            taken.swap(inFlight);
                        }
                        if (taken.empty() && producersDone.load() == numProducers)
                        {
                            const std::scoped_lock lock(queueMutex);
                            if (inFlight.empty())
                            {
                                return;
                            }
                        }
                        for (auto& [buffer, tag] : taken)
                        {
                            uint64_t stored = 0;
                            std::memcpy(&stored, buffer.getAvailableMemoryArea<std::byte>().data(), sizeof(stored));
                            if (stored != tag)
                            {
                                ++corrupted;
                            }
                            ++returned;
                        }
                        /// The buffers are returned to the cache of this thread here.
                    }
                });
        }
    }
    EXPECT_EQ(corrupted.load(), 0U);
    EXPECT_EQ(returned.load(), numProducers * buffersPerProducer);
    EXPECT_EQ(pool->getNumberOfAvailableBuffers(), NUM_BUFFERS);
    EXPECT_NO_FATAL_FAILURE(pool->destroy());
}

}
