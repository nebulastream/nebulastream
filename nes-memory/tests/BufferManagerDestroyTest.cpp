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

#include <cstdint>
#include <memory>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>

namespace NES
{

class BufferManagerDestroyTest : public ::testing::Test
{
protected:
    static constexpr uint32_t BUFFER_SIZE = 4096;
    static constexpr uint32_t NUM_BUFFERS = 8;
    static constexpr NES::BufferAlignment BUFFER_ALIGNMENT{64};
    static constexpr double UNPOOLED_MEMORY_FRACTION = 0.9;
    static constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUM_BUFFERS) * BUFFER_SIZE;
};

/// Happy path: get a buffer, release it, then destroy() succeeds without error.
TEST_F(BufferManagerDestroyTest, DestroyAfterAllBuffersReturned)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
    {
        auto buffer = bm->getBufferBlocking();
        /// buffer goes out of scope and is returned
    }
    EXPECT_EQ(bm->getNumberOfAvailableBuffers(), NUM_BUFFERS);
    EXPECT_NO_FATAL_FAILURE(bm->destroy());
}

/// Calling destroy() twice is safe (idempotent via isDestroyed flag).
TEST_F(BufferManagerDestroyTest, DestroyIsIdempotent)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
    EXPECT_NO_FATAL_FAILURE(bm->destroy());
    EXPECT_NO_FATAL_FAILURE(bm->destroy());
}

/// Destroy on a fresh BufferManager with no buffers ever acquired.
TEST_F(BufferManagerDestroyTest, DestroyWithoutAcquiringBuffers)
{
    auto bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
    EXPECT_NO_FATAL_FAILURE(bm->destroy());
}

/// INVARIANT macros are compiled out in Release and Benchmark builds (via NO_ASSERT),
/// so the ASSERT_DEATH tests below would pass vacuously (no abort). Guard them so they
/// only run in Debug/RelWithDebInfo where leak detection is actually active.
#ifndef NO_ASSERT
/// Leak detection: get a buffer, do NOT release it, call destroy() -> INVARIANT fires (terminate).
TEST_F(BufferManagerDestroyTest, DestroyDetectsSingleLeak)
{
    ASSERT_DEATH(
        {
            auto bm = BufferManager::create(
                TOTAL_MEMORY_IN_BYTES,
                UNPOOLED_MEMORY_FRACTION,
                BUFFER_ALIGNMENT,
                BUFFER_SIZE,
                std::make_shared<NesDefaultMemoryAllocator>());
            [[maybe_unused]] auto leakedBuffer = bm->getBufferBlocking();
            bm->destroy();
        },
        "");
}

/// Multiple leaks: acquire multiple buffers, release none, call destroy() -> INVARIANT fires.
TEST_F(BufferManagerDestroyTest, DestroyDetectsMultipleLeaks)
{
    ASSERT_DEATH(
        {
            auto bm = BufferManager::create(
                TOTAL_MEMORY_IN_BYTES,
                UNPOOLED_MEMORY_FRACTION,
                BUFFER_ALIGNMENT,
                BUFFER_SIZE,
                std::make_shared<NesDefaultMemoryAllocator>());
            [[maybe_unused]] auto leaked1 = bm->getBufferBlocking();
            [[maybe_unused]] auto leaked2 = bm->getBufferBlocking();
            [[maybe_unused]] auto leaked3 = bm->getBufferBlocking();
            bm->destroy();
        },
        "");
}
#endif

}
