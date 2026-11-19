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

#include <InputCollector.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{
constexpr uint64_t CAPACITY = 10;
constexpr uint32_t BUFFER_SIZE = 256;
constexpr std::chrono::microseconds MAX_DELAY{1000};
constexpr auto START = InputCollector::Clock::time_point{} + std::chrono::hours(1);

struct RepeatRecordingContext final : PipelineExecutionContext
{
    void repeatTask(const TupleBuffer&, const std::chrono::microseconds delay) override { repeats.push_back(delay); }

    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override
    {
        INVARIANT(false, "This function should not be called");
        return false;
    }

    void scheduleCallback(std::chrono::microseconds, std::function<void(PipelineExecutionContext&)>) override
    {
        INVARIANT(false, "This function should not be called");
    }

    TupleBuffer allocateTupleBuffer() override
    {
        INVARIANT(false, "This function should not be called");
        std::unreachable();
    }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return INITIAL<WorkerThreadId>; }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return nullptr; }

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

    std::vector<std::chrono::microseconds> repeats;
};
}

class InputCollectorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("InputCollectorTest.log", LogLevel::LOG_DEBUG); }

    void TearDown() override
    {
        InputCollector::releaseUnitProxy();
        BaseUnitTest::TearDown();
    }

    /// A complete input of `origin` 1 covering `sequence` with `tuples` tuples.
    [[nodiscard]] TupleBuffer input(const SequenceNumber::Underlying sequence, const uint64_t tuples) const
    {
        auto buffer = bufferManager->getBufferBlocking();
        buffer.setNumberOfTuples(tuples);
        buffer.setSequenceRange(SequenceNumber(sequence), 0);
        buffer.setChunkNumber(INITIAL_CHUNK_NUMBER);
        buffer.setLastChunk(true);
        buffer.setOriginId(OriginId(1));
        return buffer;
    }

    void repeatIfArmed(const TupleBuffer& carrier) { InputCollector::repeatIfArmedProxy(&pec, &carrier); }

    std::shared_ptr<BufferManager> bufferManager
        = BufferManager::create(1024UL * 1024, 0.5, BufferAlignment{64}, BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
    RepeatRecordingContext pec;
    InputCollector collector{CAPACITY, MAX_DELAY};
};

/// An input more than half full passes through while nothing is held.
TEST_F(InputCollectorTest, HalfFullInputPassesThrough)
{
    const auto halfFull = input(1, (CAPACITY / 2) + 1);
    const auto* unit = collector.collect(halfFull);
    ASSERT_NE(unit, nullptr);
    EXPECT_EQ(unit->numberOfBuffers, 1U);
    EXPECT_EQ(unit->sequenceNumber, SequenceNumber::Underlying{1});
    repeatIfArmed(halfFull);
    EXPECT_TRUE(pec.repeats.empty());
}

/// A small input is held and arms the timer with itself as the carrier.
TEST_F(InputCollectorTest, SmallInputIsHeldAndArmsTheTimer)
{
    const auto carrier = input(1, 2);
    EXPECT_EQ(collector.collect(carrier), nullptr);
    repeatIfArmed(carrier);
    ASSERT_EQ(pec.repeats.size(), 1U);
    EXPECT_LE(pec.repeats.front(), MAX_DELAY);
}

/// Adjacent inputs that fill the capacity together form one unit covering both sequence numbers, with the later watermark and the
/// earlier creation time.
TEST_F(InputCollectorTest, AdjacentInputsFillingTheCapacityFormOneUnit)
{
    auto first = input(1, 4);
    first.setWatermark(Timestamp(20));
    first.setCreationTimestampInMS(Timestamp(5));
    auto second = input(2, CAPACITY - 4);
    second.setWatermark(Timestamp(10));
    second.setCreationTimestampInMS(Timestamp(7));
    EXPECT_EQ(collector.collect(first), nullptr);
    const auto* unit = collector.collect(second);
    ASSERT_NE(unit, nullptr);
    EXPECT_EQ(unit->numberOfBuffers, 2U);
    EXPECT_EQ(unit->origin, OriginId::Underlying{1});
    EXPECT_EQ(unit->sequenceNumber, SequenceNumber::Underlying{2});
    EXPECT_EQ(unit->rangeOffset, 1U);
    EXPECT_EQ(unit->watermark, Timestamp::Underlying{20});
    EXPECT_EQ(unit->creation, Timestamp::Underlying{5});
}

/// An input more than half full that merges with no held run is processed alone while another run stays held.
TEST_F(InputCollectorTest, UnmergedHalfFullInputLeavesWhileOthersAreHeld)
{
    EXPECT_EQ(collector.collect(input(1, 2)), nullptr);
    const auto* unit = collector.collect(input(3, (CAPACITY / 2) + 1));
    ASSERT_NE(unit, nullptr);
    EXPECT_EQ(unit->numberOfBuffers, 1U);
    EXPECT_EQ(unit->sequenceNumber, SequenceNumber::Underlying{3});
}

/// A repeat that a failed task requested but never issued does not leak into the next task on the thread.
TEST_F(InputCollectorTest, StaleRepeatIsDiscarded)
{
    EXPECT_EQ(collector.collect(input(1, 2)), nullptr);
    InputCollector other{CAPACITY, MAX_DELAY};
    const auto halfFull = input(1, (CAPACITY / 2) + 1);
    EXPECT_NE(other.collect(halfFull), nullptr);
    repeatIfArmed(halfFull);
    EXPECT_TRUE(pec.repeats.empty());
}

/// Each repeat of the carrier takes the oldest expired run and re-arms the timer until nothing is held.
TEST_F(InputCollectorTest, CarrierRepeatTakesExpiredRunsUntilNothingIsHeld)
{
    const auto carrier = input(1, 2);
    EXPECT_EQ(collector.collectAt(carrier, START), nullptr);
    repeatIfArmed(carrier);
    EXPECT_EQ(collector.collectAt(input(3, 2), START + std::chrono::microseconds(1)), nullptr);
    repeatIfArmed(carrier);
    ASSERT_EQ(pec.repeats.size(), 1U);

    const auto* unit = collector.collectAt(carrier, START + MAX_DELAY);
    ASSERT_NE(unit, nullptr);
    EXPECT_EQ(unit->sequenceNumber, SequenceNumber::Underlying{1});
    repeatIfArmed(carrier);
    ASSERT_EQ(pec.repeats.size(), 2U);
    EXPECT_EQ(pec.repeats.back(), std::chrono::microseconds(1));

    unit = collector.collectAt(carrier, START + MAX_DELAY + std::chrono::microseconds(1));
    ASSERT_NE(unit, nullptr);
    EXPECT_EQ(unit->sequenceNumber, SequenceNumber::Underlying{3});
    repeatIfArmed(carrier);
    EXPECT_EQ(pec.repeats.size(), 2U);
}

/// Exceeding the cap on held buffers takes the run that holds the most buffers.
TEST_F(InputCollectorTest, HeldBufferCapTakesTheLargestRun)
{
    EXPECT_EQ(collector.collectAt(input(1, 1), START), nullptr);
    EXPECT_EQ(collector.collectAt(input(2, 1), START), nullptr);
    /// Odd sequence numbers from 5 on adjoin no held run.
    SequenceNumber::Underlying sequence = 5;
    for (size_t held = 2; held < InputCollector::MAX_HELD_BUFFERS; ++held, sequence += 2)
    {
        ASSERT_EQ(collector.collectAt(input(sequence, 1), START), nullptr);
    }
    const auto* unit = collector.collectAt(input(sequence, 1), START);
    ASSERT_NE(unit, nullptr);
    EXPECT_EQ(unit->numberOfBuffers, 2U);
    EXPECT_EQ(unit->sequenceNumber, SequenceNumber::Underlying{2});
}

}
