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

#include <RangeCoalescer.hpp>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <ranges>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Interface/BufferRef/BufferMerge.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
using namespace std::chrono_literals;

namespace
{
using Clock = RangeCoalescer::Clock;

/// Small, so the full and half-full rules need few tuples.
constexpr uint64_t CAPACITY = 10;
constexpr uint64_t TUPLE_SIZE = sizeof(uint64_t);
/// Larger than any layout of this test, as pooled buffers may be.
constexpr uint32_t BUFFER_SIZE = 256;
constexpr std::chrono::microseconds MAX_DELAY{1000};
constexpr size_t MAX_HELD_RUNS = 64;
constexpr auto START = Clock::time_point{} + 1h;

BufferLayout keyLayout()
{
    return {.segments = {{.offset = 0, .stride = TUPLE_SIZE}}, .references = {}, .capacity = CAPACITY, .bufferSize = CAPACITY * TUPLE_SIZE};
}

/// Records emitted buffers and scheduled callbacks.
struct RecordingContext final : PipelineExecutionContext
{
    struct ScheduledCallback
    {
        std::chrono::microseconds delay;
        Clock::time_point dueAt;
        std::function<void(PipelineExecutionContext&)> callback;
    };

    explicit RecordingContext(std::shared_ptr<BufferManager> bufferManager) : bufferManager(std::move(bufferManager)) { }

    bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
    {
        const std::scoped_lock lock(mutex);
        emitted.push_back(buffer);
        return true;
    }

    void scheduleCallback(const std::chrono::microseconds delay, std::function<void(PipelineExecutionContext&)> callback) override
    {
        const std::scoped_lock lock(mutex);
        callbacks.push_back({.delay = delay, .dueAt = Clock::now() + delay, .callback = std::move(callback)});
    }

    /// Runs the callbacks whose delay has passed by `now` and returns how many are scheduled afterwards.
    size_t runDueCallbacks(const Clock::time_point now)
    {
        std::vector<std::function<void(PipelineExecutionContext&)>> due;
        {
            const std::scoped_lock lock(mutex);
            for (auto scheduled = callbacks.begin(); scheduled != callbacks.end();)
            {
                if (scheduled->dueAt > now)
                {
                    ++scheduled;
                    continue;
                }
                due.push_back(std::move(scheduled->callback));
                scheduled = callbacks.erase(scheduled);
            }
        }
        for (auto& callback : due)
        {
            callback(*this);
        }
        const std::scoped_lock lock(mutex);
        return callbacks.size();
    }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { INVARIANT(false, "This function should not be called"); }

    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return INITIAL<WorkerThreadId>; }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override
    {
        INVARIANT(false, "This function should not be called");
        std::unreachable();
    }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>&) override
    {
        INVARIANT(false, "This function should not be called");
    }

    std::shared_ptr<BufferManager> bufferManager;
    std::mutex mutex;
    std::vector<TupleBuffer> emitted;
    std::vector<ScheduledCallback> callbacks;
};

/// The key of the tuple at `index` of the output for the range ending at `last`, unique across all outputs of a test.
uint64_t keyOf(const SequenceNumber::Underlying last, const uint64_t index)
{
    return (last * CAPACITY) + index;
}

std::vector<uint64_t> keysIn(const TupleBuffer& buffer)
{
    std::vector<uint64_t> keys(buffer.getNumberOfTuples());
    std::memcpy(keys.data(), buffer.getAvailableMemoryArea<uint8_t>().data(), keys.size() * TUPLE_SIZE);
    return keys;
}

using Outputs = std::vector<std::pair<SequenceNumber::Underlying, uint64_t>>;

/// The tuple counts of `numberOfSequences` consecutive outputs, in shuffled order. Adds their sum to `totalTuples`.
Outputs shuffledOutputs(const SequenceNumber::Underlying numberOfSequences, uint64_t& totalTuples)
{
    std::mt19937_64 random{42}; /// NOLINT(cert-msc51-cpp) a fixed seed reproduces failures with the same standard library
    std::uniform_int_distribution<uint64_t> tuplesPerOutput(0, CAPACITY);
    Outputs outputs;
    for (SequenceNumber::Underlying sequence = SequenceNumber::INITIAL; sequence < SequenceNumber::INITIAL + numberOfSequences; ++sequence)
    {
        /// Every seventh output holds up to CAPACITY tuples, the others at most two.
        const auto tuples = tuplesPerOutput(random) % (sequence % 7 == 0 ? CAPACITY + 1 : 3);
        outputs.emplace_back(sequence, tuples);
        totalTuples += tuples;
    }
    /// Shuffled in windows, so that ranges both merge and leave gaps.
    for (auto window = outputs.begin(); window < outputs.end(); window += 64)
    {
        std::shuffle(window, std::min(window + 64, outputs.end()), random);
    }
    return outputs;
}
}

class RangeCoalescerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("RangeCoalescerTest.log", LogLevel::LOG_DEBUG); }

    /// The complete output of the input range [first, last] of `origin`.
    ///NOLINTBEGIN(fuchsia-default-arguments-declarations)
    [[nodiscard]] TupleBuffer output(
        const SequenceNumber::Underlying first,
        const SequenceNumber::Underlying last,
        const uint64_t tuples,
        const OriginId origin = OriginId(1),
        const Timestamp watermark = Timestamp(0),
        const Timestamp creation = Timestamp(0)) const
    {
        auto buffer = bufferManager->getBufferBlocking();
        for (uint64_t i = 0; i < tuples; ++i)
        {
            const auto key = keyOf(last, i);
            std::memcpy(buffer.getAvailableMemoryArea<uint8_t>().data() + (i * TUPLE_SIZE), &key, TUPLE_SIZE);
        }
        buffer.setNumberOfTuples(tuples);
        buffer.setSequenceRange(SequenceNumber(last), static_cast<uint32_t>(last - first));
        buffer.setOriginId(origin);
        buffer.setWatermark(watermark);
        buffer.setCreationTimestampInMS(creation);
        return buffer;
    }

    void offer(const SequenceNumber::Underlying sequence, const uint64_t tuples, const Clock::time_point now = START)
    {
        coalescer.offer(output(sequence, sequence, tuples), pec, now);
    }

    ///NOLINTEND(fuchsia-default-arguments-declarations)

    /// Asserts that the emitted buffer at `index` covers [first, last] with `tuples` tuples as one complete chunk.
    void expectRange(
        const size_t index, const SequenceNumber::Underlying first, const SequenceNumber::Underlying last, const uint64_t tuples) const
    {
        ASSERT_LT(index, pec.emitted.size());
        const auto& buffer = pec.emitted[index];
        EXPECT_EQ(buffer.getSequenceNumber(), SequenceNumber(last));
        EXPECT_EQ(buffer.getSequenceRangeOffset(), last - first);
        EXPECT_EQ(buffer.getNumberOfTuples(), tuples);
        EXPECT_EQ(buffer.getChunkNumber(), INITIAL_CHUNK_NUMBER);
        EXPECT_TRUE(buffer.isLastChunk());
    }

    /// Offers `outputs` from several threads while one more runs `serve` in a loop.
    void offerConcurrently(RangeCoalescer& concurrent, const Outputs& outputs, const std::function<void()>& serve)
    {
        constexpr size_t numberOfThreads = 4;
        std::atomic_bool offering{true};
        std::atomic_size_t next{0};
        std::barrier start(static_cast<std::ptrdiff_t>(numberOfThreads) + 1);
        std::vector<std::jthread> threads;
        threads.reserve(numberOfThreads);
        for (size_t thread = 0; thread < numberOfThreads; ++thread)
        {
            threads.emplace_back(
                [&]
                {
                    start.arrive_and_wait();
                    for (auto i = next++; i < outputs.size(); i = next++)
                    {
                        const auto [sequence, tuples] = outputs[i];
                        concurrent.offer(output(sequence, sequence, tuples, OriginId(1), Timestamp(sequence)), pec, Clock::now());
                    }
                });
        }
        const std::jthread server(
            [&]
            {
                start.arrive_and_wait();
                while (offering)
                {
                    serve();
                    std::this_thread::yield();
                }
            });
        threads.clear();
        offering = false;
    }

    /// Asserts that the emitted buffers cover the first `numberOfSequences` input ranges exactly once, keep all `totalTuples` tuples and
    /// carry the maximum watermark of what they cover.
    void expectExactCover(const SequenceNumber::Underlying numberOfSequences, const uint64_t totalTuples)
    {
        auto ranges = pec.emitted
            | std::views::transform(
                          [](const TupleBuffer& buffer)
                          {
                              const auto last = buffer.getSequenceNumber().getRawValue();
                              return std::tuple{last - buffer.getSequenceRangeOffset(), last, &buffer};
                          })
            | std::ranges::to<std::vector>();
        std::ranges::sort(ranges);
        SequenceNumber::Underlying expectedFirst = SequenceNumber::INITIAL;
        uint64_t emittedTuples = 0;
        std::vector<uint64_t> keys;
        for (const auto& [first, last, buffer] : ranges)
        {
            ASSERT_EQ(first, expectedFirst) << "ranges must neither overlap nor leave gaps";
            EXPECT_EQ(buffer->getWatermark(), Timestamp(last));
            EXPECT_LE(buffer->getNumberOfTuples(), CAPACITY);
            EXPECT_EQ(buffer->getChunkNumber(), INITIAL_CHUNK_NUMBER);
            EXPECT_TRUE(buffer->isLastChunk());
            emittedTuples += buffer->getNumberOfTuples();
            std::ranges::copy(keysIn(*buffer), std::back_inserter(keys));
            expectedFirst = last + 1;
        }
        EXPECT_EQ(expectedFirst, SequenceNumber::INITIAL + numberOfSequences);
        EXPECT_EQ(emittedTuples, totalTuples);
        std::ranges::sort(keys);
        EXPECT_EQ(std::ranges::adjacent_find(keys), keys.end()) << "a tuple was emitted twice";
    }

    /// Enough pooled buffers for the concurrent test to keep every output it emits.
    std::shared_ptr<BufferManager> bufferManager
        = BufferManager::create(64UL * 1024 * 1024, 0.5, BufferAlignment{64}, BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
    RecordingContext pec{bufferManager};

    /// Runs the timer of `coalescer` at `now` in place of the callbacks scheduled so far.
    void fireTimer(const Clock::time_point now)
    {
        pec.callbacks.clear();
        coalescer.onTimer(pec, now);
    }

    RangeCoalescer coalescer{keyLayout(), MAX_DELAY, MAX_HELD_RUNS};
};

TEST_F(RangeCoalescerTest, ConsecutiveOutputsMergeIntoOneRange)
{
    offer(1, 2);
    offer(2, 2);
    offer(3, 2);
    EXPECT_TRUE(pec.emitted.empty());

    coalescer.flushAll(pec);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 3, 6);
    auto keys = keysIn(pec.emitted[0]);
    std::ranges::sort(keys);
    EXPECT_EQ(keys, (std::vector{keyOf(1, 0), keyOf(1, 1), keyOf(2, 0), keyOf(2, 1), keyOf(3, 0), keyOf(3, 1)}));
}

TEST_F(RangeCoalescerTest, OutputBridgesTheGapBetweenTwoRuns)
{
    offer(1, 1);
    offer(3, 1);
    offer(2, 1);
    coalescer.flushAll(pec);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 3, 3);
}

TEST_F(RangeCoalescerTest, InputRangesMerge)
{
    coalescer.offer(output(1, 4, 2), pec, START);
    coalescer.offer(output(5, 9, 2), pec, START);
    coalescer.flushAll(pec);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 9, 4);
}

TEST_F(RangeCoalescerTest, FullerSideLeavesWhenAMergeDoesNotFit)
{
    offer(1, 4);
    offer(2, 5);
    EXPECT_TRUE(pec.emitted.empty());
    offer(3, 3);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 2, 9);

    coalescer.flushAll(pec);
    ASSERT_EQ(pec.emitted.size(), 2);
    expectRange(1, 3, 3, 3);
}

TEST_F(RangeCoalescerTest, EmptierNeighbourKeepsCollecting)
{
    offer(2, 3);
    offer(1, 4);
    offer(3, 5);
    ASSERT_EQ(pec.emitted.size(), 1);
    /// [1, 2] holds 7 tuples, so the 5 of range 3 do not fit, and they are the emptier side.
    expectRange(0, 1, 2, 7);
    offer(4, 5);
    ASSERT_EQ(pec.emitted.size(), 2);
    expectRange(1, 3, 4, 10);
}

TEST_F(RangeCoalescerTest, FullerOutputLeavesAndItsNeighbourKeepsCollecting)
{
    offer(1, 1);
    offer(2, 3);
    offer(3, 7);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 3, 3, 7);

    coalescer.flushAll(pec);
    ASSERT_EQ(pec.emitted.size(), 2);
    expectRange(1, 1, 2, 4);
}

TEST_F(RangeCoalescerTest, FullerRightNeighbourLeavesWhenAMergeDoesNotFit)
{
    offer(2, 3);
    offer(3, 4);
    offer(1, 4);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 2, 3, 7);

    coalescer.flushAll(pec);
    ASSERT_EQ(pec.emitted.size(), 2);
    expectRange(1, 1, 1, 4);
}

TEST_F(RangeCoalescerTest, RunsDoNotMergeBeyondTheWidthOfARangeOffset)
{
    constexpr SequenceNumber::Underlying widest = SequenceNumber::INITIAL + std::numeric_limits<uint32_t>::max();
    coalescer.offer(output(SequenceNumber::INITIAL, widest, 1), pec, START);
    offer(widest + 1, 1);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, SequenceNumber::INITIAL, widest, 1);

    coalescer.flushAll(pec);
    ASSERT_EQ(pec.emitted.size(), 2);
    expectRange(1, widest + 1, widest + 1, 1);
}

TEST_F(RangeCoalescerTest, FullRunLeavesAtOnce)
{
    offer(1, 5);
    offer(2, 5);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 2, CAPACITY);
}

TEST_F(RangeCoalescerTest, LoneOutputMoreThanHalfFullPassesThrough)
{
    offer(1, 6);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 1, 6);
    EXPECT_TRUE(pec.callbacks.empty());
}

TEST_F(RangeCoalescerTest, UnmergedOutputMoreThanHalfFullLeavesWithItsNeighbour)
{
    offer(1, 0);
    offer(2, 6);
    EXPECT_TRUE(pec.emitted.empty());
    offer(3, 6);
    ASSERT_EQ(pec.emitted.size(), 2);
    expectRange(0, 1, 2, 6);
    expectRange(1, 3, 3, 6);
    coalescer.flushAll(pec);
    EXPECT_EQ(pec.emitted.size(), 2);
}

TEST_F(RangeCoalescerTest, OutputMoreThanHalfFullMergesWithAHeldNeighbour)
{
    offer(1, 3);
    offer(2, 6);
    EXPECT_TRUE(pec.emitted.empty());
    coalescer.flushAll(pec);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 2, 9);
}

TEST_F(RangeCoalescerTest, EmptyOutputsOnlyExtendTheRange)
{
    offer(1, 0);
    offer(2, 3);
    offer(3, 0);
    coalescer.flushAll(pec);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 3, 3);
    auto keys = keysIn(pec.emitted[0]);
    std::ranges::sort(keys);
    EXPECT_EQ(keys, (std::vector{keyOf(2, 0), keyOf(2, 1), keyOf(2, 2)}));
}

TEST_F(RangeCoalescerTest, MergedRunCarriesTheMaximumWatermarkAndTheMinimumCreationTime)
{
    coalescer.offer(output(1, 1, 1, OriginId(1), Timestamp(5), Timestamp(7)), pec, START);
    coalescer.offer(output(2, 2, 1, OriginId(1), Timestamp(9), Timestamp(2)), pec, START);
    coalescer.offer(output(3, 3, 1, OriginId(1), Timestamp(3), Timestamp(4)), pec, START);
    coalescer.flushAll(pec);
    ASSERT_EQ(pec.emitted.size(), 1);
    EXPECT_EQ(pec.emitted[0].getWatermark(), Timestamp(9));
    EXPECT_EQ(pec.emitted[0].getCreationTimestampInMS(), Timestamp(2));
}

TEST_F(RangeCoalescerTest, RunsLeaveAtTheirDeadlineWithoutWaitingForAGap)
{
    offer(1, 1);
    offer(2, 1);
    offer(4, 1, START + (MAX_DELAY / 2));

    fireTimer(START + MAX_DELAY - 1us);
    EXPECT_TRUE(pec.emitted.empty());
    fireTimer(START + MAX_DELAY);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 2, 2);
    fireTimer(START + (MAX_DELAY / 2) + MAX_DELAY);
    ASSERT_EQ(pec.emitted.size(), 2);
    expectRange(1, 4, 4, 1);
}

TEST_F(RangeCoalescerTest, MergedRunKeepsTheDeadlineOfItsOldestMember)
{
    offer(1, 1);
    offer(2, 1, START + (MAX_DELAY / 2));
    fireTimer(START + MAX_DELAY);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 2, 2);
}

TEST_F(RangeCoalescerTest, OfferReleasesExpiredRunsOfItsOrigin)
{
    offer(1, 1);
    offer(5, 1, START + MAX_DELAY);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 1, 1);
}

TEST_F(RangeCoalescerTest, OriginsAreCoalescedIndependently)
{
    coalescer.offer(output(1, 1, 1, OriginId(1)), pec, START);
    coalescer.offer(output(1, 1, 1, OriginId(2)), pec, START);
    coalescer.offer(output(2, 2, 1, OriginId(2)), pec, START);
    coalescer.offer(output(2, 2, 1, OriginId(1)), pec, START);
    coalescer.flushAll(pec);
    ASSERT_EQ(pec.emitted.size(), 2);
    for (size_t i = 0; i < 2; ++i)
    {
        expectRange(i, 1, 2, 2);
    }
    EXPECT_NE(pec.emitted[0].getOriginId(), pec.emitted[1].getOriginId());
}

TEST_F(RangeCoalescerTest, TimerIsArmedOnceWhileRunsAreHeld)
{
    offer(1, 1);
    offer(2, 1);
    offer(5, 1);
    ASSERT_EQ(pec.callbacks.size(), 1);
    EXPECT_EQ(pec.callbacks[0].delay, MAX_DELAY);
}

TEST_F(RangeCoalescerTest, TimerFlushesExpiredRunsAndStopsOnceNothingIsHeld)
{
    offer(1, 1);
    ASSERT_EQ(pec.callbacks.size(), 1);
    EXPECT_EQ(pec.callbacks[0].delay, MAX_DELAY);
    fireTimer(START + MAX_DELAY);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 1, 1);
    EXPECT_TRUE(pec.callbacks.empty());
}

TEST_F(RangeCoalescerTest, TimerRearmsForTheEarliestRemainingDeadline)
{
    offer(1, 1);
    fireTimer(START + (MAX_DELAY / 4));
    EXPECT_TRUE(pec.emitted.empty());
    ASSERT_EQ(pec.callbacks.size(), 1);
    EXPECT_EQ(pec.callbacks[0].delay, MAX_DELAY - (MAX_DELAY / 4));
}

/// The oldest run leaves inside a merged output that filled up, so the timer must wait for the next run's deadline.
TEST_F(RangeCoalescerTest, FullMergeRefreshesTheEarliestDeadline)
{
    offer(1, 1, START - (MAX_DELAY / 2));
    offer(3, 1);
    offer(2, CAPACITY - 1);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 2, CAPACITY);
    fireTimer(START);
    ASSERT_EQ(pec.callbacks.size(), 1);
    EXPECT_EQ(pec.callbacks[0].delay, MAX_DELAY);
}

TEST_F(RangeCoalescerTest, HeldRunsBeyondTheCapLeaveLowestFirst)
{
    RangeCoalescer capped{keyLayout(), MAX_DELAY, 2};
    capped.offer(output(1, 1, 1), pec, START);
    capped.offer(output(3, 3, 1), pec, START);
    EXPECT_TRUE(pec.emitted.empty());
    capped.offer(output(5, 5, 1), pec, START);
    ASSERT_EQ(pec.emitted.size(), 1);
    expectRange(0, 1, 1, 1);
}

TEST_F(RangeCoalescerTest, FlushAllEmitsEverythingWithoutScheduling)
{
    offer(1, 1);
    offer(3, 1);
    coalescer.flushAll(pec);
    ASSERT_EQ(pec.emitted.size(), 2);
    EXPECT_EQ(pec.callbacks.size(), 1);
    coalescer.flushAll(pec);
    EXPECT_EQ(pec.emitted.size(), 2);
    EXPECT_EQ(pec.callbacks.size(), 1);
}

TEST_F(RangeCoalescerTest, MergeCopiesSparsePayloadsIntoOneChild)
{
    constexpr uint64_t referenceSize = sizeof(VariableSizedAccess);
    const BufferLayout layout{
        .segments = {{.offset = 0, .stride = referenceSize}},
        .references = {{.offset = 0, .stride = referenceSize}},
        .capacity = CAPACITY,
        .bufferSize = CAPACITY * referenceSize};
    RangeCoalescer varSized{layout, MAX_DELAY, MAX_HELD_RUNS};

    /// Each output references a one byte payload in a child of its own, at index 0 of its buffer.
    auto withChild = [&](const SequenceNumber::Underlying sequence)
    {
        auto buffer = bufferManager->getBufferBlocking();
        auto child = bufferManager->getBufferBlocking();
        *child.getAvailableMemoryArea<uint8_t>().data() = static_cast<uint8_t>('a' + sequence);
        child.setNumberOfTuples(1);
        const auto index = buffer.storeChildBuffer(child);
        const VariableSizedAccess slot{index, VariableSizedAccess::Size(1)};
        std::memcpy(buffer.getAvailableMemoryArea<uint8_t>().data(), &slot, sizeof(slot));
        buffer.setNumberOfTuples(1);
        buffer.setSequenceRange(SequenceNumber(sequence), 0);
        buffer.setOriginId(OriginId(1));
        return buffer;
    };
    varSized.offer(withChild(1), pec, START);
    varSized.offer(withChild(2), pec, START);
    varSized.flushAll(pec);

    ASSERT_EQ(pec.emitted.size(), 1);
    const auto& merged = pec.emitted[0];
    ASSERT_EQ(merged.getNumberOfTuples(), 2);
    ASSERT_EQ(merged.getNumberOfChildBuffers(), 1);
    std::vector<uint8_t> payloads;
    for (uint64_t i = 0; i < 2; ++i)
    {
        VariableSizedAccess slot;
        std::memcpy(&slot, merged.getAvailableMemoryArea<uint8_t>().data() + (i * referenceSize), sizeof(slot));
        EXPECT_EQ(slot.getIndex().getRawValue(), 0);
        payloads.push_back(merged.loadChildBuffer(slot.getIndex()).getAvailableMemoryArea<uint8_t>()[slot.getOffset().getRawOffset()]);
    }
    std::ranges::sort(payloads);
    EXPECT_EQ(payloads, (std::vector<uint8_t>{'b', 'c'}));
}

/// Shuffled outputs offered by one thread merge, and the emitted buffers cover every input range exactly once.
TEST_F(RangeCoalescerTest, ShuffledOutputsMergeAndCoverEverySequenceExactlyOnce)
{
    constexpr SequenceNumber::Underlying numberOfSequences = 2000;
    uint64_t totalTuples = 0;
    for (const auto& [sequence, tuples] : shuffledOutputs(numberOfSequences, totalTuples))
    {
        coalescer.offer(output(sequence, sequence, tuples, OriginId(1), Timestamp(sequence)), pec, START);
    }
    coalescer.flushAll(pec);
    expectExactCover(numberOfSequences, totalTuples);
    EXPECT_LT(pec.emitted.size(), numberOfSequences / 2) << "outputs should have merged";
}

/// Only due pipeline callbacks flush while offers race on arming the timer, so a lost wakeup leaves runs missing from the cover.
TEST_F(RangeCoalescerTest, ConcurrentOffersDrainThroughTheTimerAlone)
{
    constexpr SequenceNumber::Underlying numberOfSequences = 20000;
    RangeCoalescer concurrent{keyLayout(), 5ms, MAX_HELD_RUNS};
    uint64_t totalTuples = 0;
    const auto outputs = shuffledOutputs(numberOfSequences, totalTuples);

    offerConcurrently(concurrent, outputs, [&] { pec.runDueCallbacks(Clock::now()); });
    /// No flushAll: what is still held has to leave through the callbacks. The timeout only guards against callbacks that never end.
    const auto giveUp = Clock::now() + 10s;
    while (pec.runDueCallbacks(Clock::now()) > 0)
    {
        ASSERT_LT(Clock::now(), giveUp) << "the callbacks kept rescheduling";
        std::this_thread::yield();
    }
    expectExactCover(numberOfSequences, totalTuples);
}

}
