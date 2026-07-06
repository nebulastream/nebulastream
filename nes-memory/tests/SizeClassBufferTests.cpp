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

/// NOLINTBEGIN(readability-magic-numbers,readability-identifier-length,misc-include-cleaner)
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <thread>
#include <vector>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h> /// NOLINT(misc-include-cleaner): consumed via macros expanded from rapidcheck/gtest.h
#include <rapidcheck/gtest.h>

namespace NES
{

namespace
{
SizeClassConfig eagerConfig(const size_t minClass, const size_t maxClass, const size_t buffersPerClass)
{
    SizeClassConfig config{.minClassSize = minClass, .maxClassSize = maxClass};
    config.policy = BufferProvisioningPolicy::EagerPerClass;
    config.buffersPerClass = buffersPerClass;
    return config;
}

SizeClassConfig budgetConfig(const size_t minClass, const size_t maxClass, const size_t totalBudgetBytes)
{
    SizeClassConfig config{.minClassSize = minClass, .maxClassSize = maxClass};
    config.policy = BufferProvisioningPolicy::TotalBudgetSplit;
    config.totalBudgetBytes = totalBudgetBytes;
    return config;
}

SizeClassConfig lazyConfig(const size_t minClass, const size_t maxClass, const size_t floor, const size_t growth, const size_t ceiling)
{
    SizeClassConfig config{.minClassSize = minClass, .maxClassSize = maxClass};
    config.policy = BufferProvisioningPolicy::LazyElastic;
    config.floorBuffersPerClass = floor;
    config.growthChunkBuffers = growth;
    config.maxBuffersPerClass = ceiling;
    return config;
}
}

class SizeClassBufferTest : public ::testing::Test
{
};

/// getBuffer(size) rounds the request up to the smallest fitting power-of-two size class.
TEST_F(SizeClassBufferTest, RoundsUpToSmallestFittingClass)
{
    auto bm = BufferManager::create(4096, 8, std::make_shared<NesDefaultMemoryAllocator>(), eagerConfig(1024, 16384, 4));

    EXPECT_EQ(bm->getBuffer(1).getBufferSize(), 1024U);
    EXPECT_EQ(bm->getBuffer(1024).getBufferSize(), 1024U);
    EXPECT_EQ(bm->getBuffer(1025).getBufferSize(), 2048U);
    EXPECT_EQ(bm->getBuffer(4096).getBufferSize(), 4096U); /// the default class
    EXPECT_EQ(bm->getBuffer(5000).getBufferSize(), 8192U);
    EXPECT_EQ(bm->getBuffer(16384).getBufferSize(), 16384U);

    /// Larger than the largest class -> unpooled buffer that still satisfies the >= contract.
    auto unpooled = bm->getBuffer(20000);
    EXPECT_GE(unpooled.getBufferSize(), 20000U);
}

/// Without a SizeClassConfig the manager has exactly one class (the default), preserving legacy behavior,
/// but getBuffer(size) still serves small requests from the pooled default class and large ones unpooled.
TEST_F(SizeClassBufferTest, BackwardCompatibleSingleClass)
{
    auto bm = BufferManager::create(4096, 8);

    EXPECT_EQ(bm->getMaxBufferSize(), 4096U);
    EXPECT_EQ(bm->getNumOfPooledBuffers(), 8U);
    EXPECT_EQ(bm->getBuffer(2000).getBufferSize(), 4096U);
    EXPECT_EQ(bm->getBuffer(4096).getBufferSize(), 4096U);
    EXPECT_GE(bm->getBuffer(9000).getBufferSize(), 9000U); /// unpooled
}

/// EagerPerClass preallocates the configured number of buffers per class up front.
TEST_F(SizeClassBufferTest, EagerPerClassPreallocatesCounts)
{
    /// Classes: 1024, 2048, 4096(default,8), 8192 -> 3 extra classes * 4 + default 8 = 20.
    auto bm = BufferManager::create(4096, 8, std::make_shared<NesDefaultMemoryAllocator>(), eagerConfig(1024, 8192, 4));
    EXPECT_EQ(bm->getNumOfPooledBuffers(), 20U);
    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), 20U);
}

/// TotalBudgetSplit distributes a byte budget across the classes; each class gets at least one buffer.
TEST_F(SizeClassBufferTest, TotalBudgetSplitDistributesBudget)
{
    auto bm = BufferManager::create(4096, 8, std::make_shared<NesDefaultMemoryAllocator>(), budgetConfig(1024, 8192, 1U << 20U));
    /// Default class (8) plus a positive number of buffers in each extra class.
    EXPECT_GT(bm->getNumOfPooledBuffers(), 8U);
    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), bm->getNumOfPooledBuffers());
}

/// A released pooled buffer is recycled back into its own size class and can be obtained again.
TEST_F(SizeClassBufferTest, RecyclesBackIntoCorrectClass)
{
    auto bm = BufferManager::create(4096, 8, std::make_shared<NesDefaultMemoryAllocator>(), eagerConfig(1024, 8192, 2));
    const auto totalAvailable = bm->getNumberOfAvailableBuffers();

    {
        auto buffer = bm->getBuffer(1500); /// 2048 class
        EXPECT_EQ(buffer.getBufferSize(), 2048U);
        EXPECT_EQ(bm->getNumberOfAvailableBuffers(), totalAvailable - 1);
    }
    /// After release the buffer is back, and a new same-sized request reuses the 2048 class.
    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), totalAvailable);
    EXPECT_EQ(bm->getBuffer(1500).getBufferSize(), 2048U);
}

/// When the best-fit class is momentarily exhausted, getBuffer promotes to a larger pooled class.
TEST_F(SizeClassBufferTest, PromotesToLargerClassWhenExhausted)
{
    auto bm = BufferManager::create(4096, 8, std::make_shared<NesDefaultMemoryAllocator>(), eagerConfig(1024, 8192, 1));

    auto first = bm->getBuffer(1000); /// drains the single 1024-class buffer
    EXPECT_EQ(first.getBufferSize(), 1024U);
    auto promoted = bm->getBuffer(1000); /// 1024 empty -> next class
    EXPECT_GE(promoted.getBufferSize(), 1024U);
    EXPECT_GT(promoted.getBufferSize(), 1024U);
}

/// LazyElastic starts near-empty and grows a class on demand up to its ceiling.
TEST_F(SizeClassBufferTest, LazyElasticGrowsOnDemand)
{
    auto bm = BufferManager::create(4096, 8, std::make_shared<NesDefaultMemoryAllocator>(), lazyConfig(2048, 8192, 0, 4, 64));

    /// The lazy classes contribute nothing until first use.
    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), 8U);

    std::vector<TupleBuffer> held;
    held.reserve(10);
    for (int i = 0; i < 10; ++i)
    {
        auto buffer = bm->getBuffer(2048);
        EXPECT_EQ(buffer.getBufferSize(), 2048U);
        held.push_back(std::move(buffer));
    }
    /// Grew past the initial growth chunk of 4 to satisfy 10 concurrent live buffers.
    held.clear();
    EXPECT_NO_FATAL_FAILURE(bm->destroy());
}

/// Requests larger than the largest size class are served from the unpooled path.
TEST_F(SizeClassBufferTest, LargerThanMaxClassUsesUnpooled)
{
    auto bm = BufferManager::create(4096, 8, std::make_shared<NesDefaultMemoryAllocator>(), eagerConfig(1024, 4096, 2));
    const auto pooledBefore = bm->getNumberOfAvailableBuffers();

    auto buffer = bm->getBuffer(100000);
    EXPECT_GE(buffer.getBufferSize(), 100000U);
    /// Pooled availability is untouched; the buffer came from the unpooled manager.
    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), pooledBefore);
    EXPECT_GE(bm->getNumOfUnpooledBuffers(), 1U);
}

/// getBufferNoBlocking(size) returns an empty optional once the best-fit class and every larger
/// class are drained (so it cannot promote), without blocking.
TEST_F(SizeClassBufferTest, NoBlockingReturnsNulloptWhenExhausted)
{
    /// Classes >= 1000 bytes are 1024(1), 2048(1) and the 4096 default(1) -> three buffers total.
    auto bm = BufferManager::create(4096, 1, std::make_shared<NesDefaultMemoryAllocator>(), eagerConfig(1024, 2048, 1));
    std::vector<TupleBuffer> held;
    held.push_back(bm->getBuffer(1000));
    held.push_back(bm->getBuffer(1000));
    held.push_back(bm->getBuffer(1000));
    EXPECT_FALSE(bm->getBufferNoBlocking(1000).has_value());
    held.clear();
    EXPECT_TRUE(bm->getBufferNoBlocking(1000).has_value());
}

/// TotalBudgetSplit provisions every class and serves the exact class for a request.
TEST_F(SizeClassBufferTest, BudgetPolicyServesEveryClass)
{
    auto bm = BufferManager::create(4096, 8, std::make_shared<NesDefaultMemoryAllocator>(), budgetConfig(1024, 8192, 1U << 20U));
    EXPECT_EQ(bm->getBuffer(1024).getBufferSize(), 1024U);
    EXPECT_EQ(bm->getBuffer(8192).getBufferSize(), 8192U);
}

/// LazyElastic grows to its ceiling and then promotes / falls back for further demand, leak-free.
TEST_F(SizeClassBufferTest, LazyElasticPastCeiling)
{
    auto bm = BufferManager::create(4096, 8, std::make_shared<NesDefaultMemoryAllocator>(), lazyConfig(2048, 4096, 0, 2, 4));
    std::vector<TupleBuffer> held;
    held.reserve(12);
    for (int i = 0; i < 12; ++i)
    {
        auto buffer = bm->getBuffer(2048);
        EXPECT_GE(buffer.getBufferSize(), 2048U);
        held.push_back(std::move(buffer));
    }
    held.clear();
    EXPECT_NO_FATAL_FAILURE(bm->destroy());
}

/// deepCopyBuffer replicates the raw bytes, size and metadata into a fresh buffer.
TEST_F(SizeClassBufferTest, DeepCopyReplicatesDataAndMetadata)
{
    auto bm = BufferManager::create(4096, 8, std::make_shared<NesDefaultMemoryAllocator>(), eagerConfig(1024, 8192, 4));
    auto source = bm->getBuffer(2000); /// 2048 class
    auto bytes = source.getAvailableMemoryArea<uint8_t>();
    for (size_t i = 0; i < bytes.size(); ++i)
    {
        bytes[i] = static_cast<uint8_t>(i & 0xFFU);
    }
    source.setNumberOfTuples(7);

    auto copy = deepCopyBuffer(source, *bm);
    EXPECT_GE(copy.getBufferSize(), source.getBufferSize());
    EXPECT_EQ(copy.getNumberOfTuples(), 7U);
    const auto copyBytes = copy.getAvailableMemoryArea<uint8_t>();
    for (size_t i = 0; i < source.getBufferSize(); ++i)
    {
        EXPECT_EQ(copyBytes[i], static_cast<uint8_t>(i & 0xFFU));
    }
}

/// Exhausting the default pool makes the blocking no-arg path throw after the timeout.
TEST_F(SizeClassBufferTest, BlockingThrowsWhenDefaultPoolExhausted)
{
    auto bm = BufferManager::create(4096, 1);
    auto held = bm->getBufferBlocking();
    EXPECT_ANY_THROW([[maybe_unused]] auto buffer = bm->getBufferBlocking());
}

/// Many threads hammering mixed sizes across all classes must never leak: after everyone returns
/// their buffers, every pooled buffer is available again and destroy() finds no live buffer.
TEST_F(SizeClassBufferTest, ConcurrentMixedSizeAllocationsAreLeakFree)
{
    auto bm = BufferManager::create(8192, 64, std::make_shared<NesDefaultMemoryAllocator>(), eagerConfig(512, 65536, 64));
    const auto totalPooled = bm->getNumOfPooledBuffers();

    constexpr size_t numThreads = 8;
    constexpr size_t iterationsPerThread = 2000;
    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    for (size_t t = 0; t < numThreads; ++t)
    {
        threads.emplace_back(
            [&bm, t]
            {
                std::mt19937 generator(static_cast<unsigned>(t + 1));
                std::uniform_int_distribution<size_t> distribution(1, 70000);
                for (size_t i = 0; i < iterationsPerThread; ++i)
                {
                    const size_t size = distribution(generator);
                    auto buffer = bm->getBuffer(size);
                    ASSERT_GE(buffer.getBufferSize(), size);
                    /// buffer released at end of iteration
                }
            });
    }
    for (auto& thread : threads)
    {
        thread.join();
    }

    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), totalPooled);
    EXPECT_NO_FATAL_FAILURE(bm->destroy());
}

/// Property: every getBuffer(size) returns a buffer of at least the requested size, and once the
/// buffer is released the pool returns to full availability (no buffers lost or duplicated).
RC_GTEST_FIXTURE_PROP(SizeClassBufferTest, ReturnedBufferFitsAndRecycles, (const std::vector<uint32_t>& rawSizes))
{
    auto bm = BufferManager::create(4096, 16, std::make_shared<NesDefaultMemoryAllocator>(), eagerConfig(512, 16384, 8));
    const auto totalPooled = bm->getNumOfPooledBuffers();

    for (const auto rawSize : rawSizes)
    {
        const size_t size = (rawSize % 32768U) + 1U; /// 1 .. 32768
        auto buffer = bm->getBuffer(size);
        RC_ASSERT(buffer.getBufferSize() >= size);
        /// buffer released here, so at most one buffer is ever live -> no class can be exhausted
    }

    RC_ASSERT(bm->getNumberOfAvailableBuffers() == totalPooled);
}

}

/// NOLINTEND(readability-magic-numbers,readability-identifier-length,misc-include-cleaner)
