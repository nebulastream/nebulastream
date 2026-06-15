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

#include <chrono>
#include <cstdint>
#include <vector>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>

namespace NES
{

/// Tests for the liveness reserve in BufferManager: a subset of the pool is held back and only handed out via the
/// reserved acquisition API, so the drain/emit path can always make forward progress and break the buffer-exhaustion
/// deadlock. The normal (non-reserved) acquisition path must never consume the reserve.
class BufferManagerReserveTest : public ::testing::Test
{
protected:
    static constexpr uint32_t BUFFER_SIZE = 4096;
    static constexpr uint32_t NUM_BUFFERS = 8;
    static constexpr uint32_t NUM_RESERVED = 3;
    static constexpr uint32_t NUM_NORMAL = NUM_BUFFERS - NUM_RESERVED;
    static constexpr auto SHORT_TIMEOUT = std::chrono::milliseconds(10);

    /// Helper: create a buffer manager with a reserve of numReserved buffers.
    static std::shared_ptr<BufferManager> makeBm(uint32_t numReserved)
    {
        return BufferManager::create(
            BUFFER_SIZE, NUM_BUFFERS, std::make_shared<NesDefaultMemoryAllocator>(), BufferManager::DEFAULT_ALIGNMENT, numReserved);
    }
};

/// All buffers (reserve + normal) are counted as available right after construction.
TEST_F(BufferManagerReserveTest, ReserveSeededAtConstruction)
{
    auto bm = makeBm(NUM_RESERVED);
    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), NUM_BUFFERS);
    EXPECT_EQ(bm->getNumOfPooledBuffers(), NUM_BUFFERS);
}

/// The normal acquisition path may only consume the non-reserved buffers; once those are gone it must fail rather
/// than dip into the reserve.
TEST_F(BufferManagerReserveTest, NormalAcquireCannotDrainReserve)
{
    auto bm = makeBm(NUM_RESERVED);
    std::vector<TupleBuffer> held;

    /// Drain exactly the normal portion of the pool.
    for (uint32_t i = 0; i < NUM_NORMAL; ++i)
    {
        auto buffer = bm->getBufferNoBlocking();
        ASSERT_TRUE(buffer.has_value()) << "normal buffer " << i << " should be available";
        held.push_back(std::move(buffer.value()));
    }

    /// The reserve is now all that's left; normal acquisition must not touch it.
    EXPECT_FALSE(bm->getBufferNoBlocking().has_value());
    EXPECT_FALSE(bm->getBufferWithTimeout(SHORT_TIMEOUT).has_value());
    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), NUM_RESERVED);

    held.clear();
}

/// The reserved acquisition path can use the reserve once the normal pool is exhausted.
TEST_F(BufferManagerReserveTest, ReservedAcquireCanUseReserve)
{
    auto bm = makeBm(NUM_RESERVED);
    std::vector<TupleBuffer> held;

    /// Drain the normal portion via the normal path.
    for (uint32_t i = 0; i < NUM_NORMAL; ++i)
    {
        held.push_back(bm->getBufferBlocking());
    }

    /// The reserved path now serves the remaining reserve buffers.
    for (uint32_t i = 0; i < NUM_RESERVED; ++i)
    {
        auto buffer = bm->tryGetReservedBuffer();
        ASSERT_TRUE(buffer.has_value()) << "reserved buffer " << i << " should be available";
        held.push_back(std::move(buffer.value()));
    }

    /// Whole pool drained: even the reserved path now returns empty.
    EXPECT_FALSE(bm->tryGetReservedBuffer().has_value());
    EXPECT_FALSE(bm->getReservedBufferWithTimeout(SHORT_TIMEOUT).has_value());
    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), 0u);

    held.clear();
}

/// A recycled buffer tops up the reserve before the normal pool, preserving the liveness guarantee.
TEST_F(BufferManagerReserveTest, RecycleTopsUpReserveFirst)
{
    auto bm = makeBm(NUM_RESERVED);
    std::vector<TupleBuffer> held;

    /// Drain the whole pool (normal + reserve).
    for (uint32_t i = 0; i < NUM_NORMAL; ++i)
    {
        held.push_back(bm->getBufferBlocking());
    }
    for (uint32_t i = 0; i < NUM_RESERVED; ++i)
    {
        held.push_back(bm->tryGetReservedBuffer().value());
    }
    ASSERT_EQ(bm->getNumberOfAvailableBuffers(), 0u);

    /// Release one buffer. Since the reserve is below its target, the recycled buffer must go to the reserve, so the
    /// normal path stays empty while the reserved path can hand it out.
    held.pop_back();
    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), 1u);
    EXPECT_FALSE(bm->getBufferNoBlocking().has_value());

    auto reserved = bm->tryGetReservedBuffer();
    ASSERT_TRUE(reserved.has_value());
    held.push_back(std::move(reserved.value()));

    held.clear();
}

/// With a zero-sized reserve the manager behaves exactly as before: the entire pool is available to the normal path,
/// and the reserved API simply falls back to it.
TEST_F(BufferManagerReserveTest, ZeroReserveIsUnchangedBehaviour)
{
    auto bm = makeBm(0);
    std::vector<TupleBuffer> held;

    for (uint32_t i = 0; i < NUM_BUFFERS; ++i)
    {
        auto buffer = bm->getBufferNoBlocking();
        ASSERT_TRUE(buffer.has_value()) << "buffer " << i << " should be available with no reserve";
        held.push_back(std::move(buffer.value()));
    }
    EXPECT_FALSE(bm->getBufferNoBlocking().has_value());
    EXPECT_FALSE(bm->tryGetReservedBuffer().has_value());
    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), 0u);

    held.clear();
}

/// The reserve target is clamped to the pool size: requesting more reserved buffers than exist puts the whole pool in
/// the reserve, reachable only via the reserved path.
TEST_F(BufferManagerReserveTest, ReserveClampedToPoolSize)
{
    auto bm = makeBm(NUM_BUFFERS * 4);
    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), NUM_BUFFERS);
    /// Entire pool is reserved, so the normal path gets nothing.
    EXPECT_FALSE(bm->getBufferNoBlocking().has_value());

    std::vector<TupleBuffer> held;
    for (uint32_t i = 0; i < NUM_BUFFERS; ++i)
    {
        auto buffer = bm->tryGetReservedBuffer();
        ASSERT_TRUE(buffer.has_value());
        held.push_back(std::move(buffer.value()));
    }
    held.clear();
}

}
