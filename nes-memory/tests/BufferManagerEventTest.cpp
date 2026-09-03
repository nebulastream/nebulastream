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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/BufferProviderStatisticListener.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>

namespace NES
{

namespace
{
/// Generous enough that it never expires on a healthy pool, so a timeout means a real failure.
constexpr std::chrono::milliseconds ACQUIRE_TIMEOUT{100};
/// Small, because the test wants the timeout to actually elapse on a drained pool.
constexpr std::chrono::milliseconds EXHAUSTED_TIMEOUT{20};
/// A budget of a fraction of a single buffer, so any unpooled request of substance is refused.
constexpr double NEGLIGIBLE_UNPOOLED_FRACTION = 0.001;
/// Comfortably above the pooled buffer size, so it is served from an unpooled chunk.
constexpr size_t UNPOOLED_REQUEST_BYTES = 1024;
/// Far beyond NEGLIGIBLE_UNPOOLED_FRACTION of the budget, so the reservation is certain to be refused.
constexpr size_t OVERSIZED_UNPOOLED_REQUEST_BYTES = size_t{1024} * 1024;
constexpr size_t NUM_THREADS = 4;
constexpr size_t ITERATIONS_PER_THREAD = 200;

/// Keeps every event so a test can assert on order and payload. Events arrive from arbitrary threads, hence
/// the mutex; a production listener would not be allowed to take one.
class RecordingListener final : public BufferProviderStatisticListener
{
public:
    void onEvent(BufferEvent event) override
    {
        const std::lock_guard lock(mutex);
        events.push_back(std::move(event));
    }

    template <typename EventType>
    size_t count() const
    {
        const std::lock_guard lock(mutex);
        size_t matches = 0;
        for (const auto& event : events)
        {
            matches += static_cast<size_t>(std::holds_alternative<EventType>(event));
        }
        return matches;
    }

    /// The most recent event of the requested type, or nullopt if none was seen.
    template <typename EventType>
    std::optional<EventType> last() const
    {
        const std::lock_guard lock(mutex);
        for (auto event = events.rbegin(); event != events.rend(); ++event)
        {
            if (const auto* match = std::get_if<EventType>(&*event))
            {
                return *match;
            }
        }
        return std::nullopt;
    }

    size_t total() const
    {
        const std::lock_guard lock(mutex);
        return events.size();
    }

private:
    mutable std::mutex mutex;
    std::vector<BufferEvent> events;
};
}

class BufferManagerEventTest : public ::testing::Test
{
protected:
    static constexpr uint32_t BUFFER_SIZE = 4096;
    static constexpr uint32_t NUM_BUFFERS = 8;
    static constexpr NES::BufferAlignment BUFFER_ALIGNMENT{64};
    /// Reserving 90% for unpooled memory leaves exactly NUM_BUFFERS pooled buffers.
    static constexpr double UNPOOLED_MEMORY_FRACTION = 0.9;
    static constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUM_BUFFERS) * BUFFER_SIZE;

    /// NOLINTNEXTLINE(fuchsia-default-arguments-declarations): only one test overrides the fraction.
    static std::shared_ptr<BufferManager>
    makeBufferManager(std::shared_ptr<BufferProviderStatisticListener> listener, const double unpooledFraction = UNPOOLED_MEMORY_FRACTION)
    {
        return BufferManager::create(
            TOTAL_MEMORY_IN_BYTES,
            unpooledFraction,
            BUFFER_ALIGNMENT,
            BUFFER_SIZE,
            std::make_shared<NesDefaultMemoryAllocator>(),
            std::move(listener));
    }
};

/// clang tidy doesn't recognize the ASSERT_TRUE guarding the optional accesses below
/// NOLINTBEGIN(bugprone-unchecked-optional-access)

/// The pool describes itself when it comes up and reports its final state when it goes away, so a consumer
/// never has to be handed the configuration separately.
TEST_F(BufferManagerEventTest, ReportsItsOwnLifecycle)
{
    auto recorder = std::make_shared<RecordingListener>();
    auto bufferManager = makeBufferManager(recorder);

    const auto created = recorder->last<BufferPoolCreated>();
    ASSERT_TRUE(created.has_value());
    EXPECT_EQ(created->numberOfBuffers, NUM_BUFFERS);
    EXPECT_EQ(created->bufferSize, BUFFER_SIZE);
    EXPECT_EQ(created->unpooledBudgetBytes, static_cast<size_t>(TOTAL_MEMORY_IN_BYTES * UNPOOLED_MEMORY_FRACTION));
    EXPECT_EQ(recorder->count<BufferPoolDestroyed>(), 0) << "The pool is still alive";

    bufferManager->destroy();

    const auto destroyed = recorder->last<BufferPoolDestroyed>();
    ASSERT_TRUE(destroyed.has_value());
    EXPECT_EQ(destroyed->leakedBuffers, 0);
}

/// The acquire and the recycle of a pooled buffer are both reported, and each carries the fill level observed
/// right afterwards.
TEST_F(BufferManagerEventTest, ReportsPooledAcquireAndRecycle)
{
    auto recorder = std::make_shared<RecordingListener>();
    auto bufferManager = makeBufferManager(recorder);

    {
        auto buffer = bufferManager->getBufferBlocking();
        ASSERT_EQ(recorder->count<PooledBufferAcquired>(), 1);
        EXPECT_EQ(recorder->last<PooledBufferAcquired>()->availableAfter, NUM_BUFFERS - 1);
        EXPECT_EQ(recorder->count<PooledBufferRecycled>(), 0) << "The buffer is still held";
    }

    ASSERT_EQ(recorder->count<PooledBufferRecycled>(), 1);
    EXPECT_EQ(recorder->last<PooledBufferRecycled>()->availableAfter, NUM_BUFFERS);
}

/// All three entry points report an acquire, so a consumer does not have to know which one a component uses.
TEST_F(BufferManagerEventTest, ReportsAcquireFromEveryEntryPoint)
{
    auto recorder = std::make_shared<RecordingListener>();
    auto bufferManager = makeBufferManager(recorder);

    {
        auto blocking = bufferManager->getBufferBlocking();
        auto nonBlocking = bufferManager->getBufferNoBlocking();
        auto withTimeout = bufferManager->getBufferWithTimeout(ACQUIRE_TIMEOUT);
        ASSERT_TRUE(nonBlocking.has_value());
        ASSERT_TRUE(withTimeout.has_value());
        EXPECT_EQ(recorder->count<PooledBufferAcquired>(), 3);
    }
    EXPECT_EQ(recorder->count<PooledBufferRecycled>(), 3);
}

/// An empty pool is the interesting signal, and the granted timeout distinguishes a non-blocking probe from a
/// real starvation stall.
TEST_F(BufferManagerEventTest, ReportsExhaustionWithTheGrantedTimeout)
{
    auto recorder = std::make_shared<RecordingListener>();
    auto bufferManager = makeBufferManager(recorder);

    std::vector<TupleBuffer> drained;
    drained.reserve(NUM_BUFFERS);
    for (size_t i = 0; i < NUM_BUFFERS; ++i)
    {
        drained.push_back(bufferManager->getBufferBlocking());
    }
    ASSERT_EQ(recorder->count<PooledBufferRequestFailed>(), 0);

    EXPECT_FALSE(bufferManager->getBufferNoBlocking().has_value());
    ASSERT_EQ(recorder->count<PooledBufferRequestFailed>(), 1);
    EXPECT_EQ(recorder->last<PooledBufferRequestFailed>()->waited, std::chrono::milliseconds{0})
        << "A non-blocking probe did not wait for anything";

    EXPECT_FALSE(bufferManager->getBufferWithTimeout(EXHAUSTED_TIMEOUT).has_value());
    ASSERT_EQ(recorder->count<PooledBufferRequestFailed>(), 2);
    EXPECT_EQ(recorder->last<PooledBufferRequestFailed>()->waited, EXHAUSTED_TIMEOUT);

    drained.clear();
}

/// An unpooled buffer is reported together with the chunk it was carved out of, and the chunk is reported
/// again when its last buffer goes away.
TEST_F(BufferManagerEventTest, ReportsUnpooledAllocationAndChunkRelease)
{
    auto recorder = std::make_shared<RecordingListener>();
    auto bufferManager = makeBufferManager(recorder);

    {
        auto unpooled = bufferManager->getUnpooledBuffer(UNPOOLED_REQUEST_BYTES);
        ASSERT_TRUE(unpooled.has_value());

        ASSERT_EQ(recorder->count<UnpooledBufferAllocated>(), 1);
        const auto allocated = recorder->last<UnpooledBufferAllocated>();
        EXPECT_GE(allocated->requestedBytes, UNPOOLED_REQUEST_BYTES) << "The request is padded by the control block and the alignment";
        EXPECT_GT(allocated->unpooledBytesInUse, 0);

        ASSERT_EQ(recorder->count<UnpooledChunkAllocated>(), 1);
        EXPECT_GE(recorder->last<UnpooledChunkAllocated>()->chunkBytes, UNPOOLED_REQUEST_BYTES);
        EXPECT_EQ(recorder->count<UnpooledChunkReleased>(), 0) << "The chunk still has a live buffer";
    }

    ASSERT_EQ(recorder->count<UnpooledChunkReleased>(), 1);
    const auto released = recorder->last<UnpooledChunkReleased>();
    EXPECT_EQ(released->chunkBytes, recorder->last<UnpooledChunkAllocated>()->chunkBytes);
    EXPECT_EQ(released->unpooledBytesInUse, 0) << "The only chunk went back to the allocator";
}

/// A breach of the unpooled budget is currently only an NES_WARNING, so this event is the one way a consumer
/// can see it happen.
TEST_F(BufferManagerEventTest, ReportsUnpooledBudgetRefusal)
{
    auto recorder = std::make_shared<RecordingListener>();
    auto bufferManager = makeBufferManager(recorder, NEGLIGIBLE_UNPOOLED_FRACTION);

    EXPECT_FALSE(bufferManager->getUnpooledBuffer(OVERSIZED_UNPOOLED_REQUEST_BYTES).has_value());

    ASSERT_EQ(recorder->count<UnpooledBufferRequestFailed>(), 1);
    EXPECT_EQ(recorder->last<UnpooledBufferRequestFailed>()->unpooledBytesInUse, 0) << "The refused reservation was rolled back";
    EXPECT_EQ(recorder->count<UnpooledBufferAllocated>(), 0);
    EXPECT_EQ(recorder->count<UnpooledChunkAllocated>(), 0);
}

/// Emission happens on whichever thread touches a buffer, so the counts have to add up under contention.
TEST_F(BufferManagerEventTest, CountsAddUpUnderContention)
{
    auto recorder = std::make_shared<RecordingListener>();
    auto bufferManager = makeBufferManager(recorder);

    {
        std::vector<std::jthread> threads;
        threads.reserve(NUM_THREADS);
        for (size_t thread = 0; thread < NUM_THREADS; ++thread)
        {
            threads.emplace_back(
                [&bufferManager]
                {
                    for (size_t iteration = 0; iteration < ITERATIONS_PER_THREAD; ++iteration)
                    {
                        /// Acquired and released immediately, so NUM_BUFFERS is never the limit.
                        [[maybe_unused]] auto buffer = bufferManager->getBufferBlocking();
                    }
                });
        }
    }

    EXPECT_EQ(recorder->count<PooledBufferAcquired>(), NUM_THREADS * ITERATIONS_PER_THREAD);
    EXPECT_EQ(recorder->count<PooledBufferRecycled>(), NUM_THREADS * ITERATIONS_PER_THREAD);
    EXPECT_EQ(recorder->count<PooledBufferRequestFailed>(), 0);
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), NUM_BUFFERS);
}

/// The uninstrumented path is the default and has to stay untouched.
TEST_F(BufferManagerEventTest, WorksWithoutAListener)
{
    auto bufferManager = makeBufferManager(nullptr);
    {
        auto buffer = bufferManager->getBufferBlocking();
        auto unpooled = bufferManager->getUnpooledBuffer(UNPOOLED_REQUEST_BYTES);
        EXPECT_TRUE(unpooled.has_value());
    }
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), NUM_BUFFERS);
    EXPECT_NO_FATAL_FAILURE(bufferManager->destroy());
}

/// NOLINTEND(bugprone-unchecked-optional-access)

}
