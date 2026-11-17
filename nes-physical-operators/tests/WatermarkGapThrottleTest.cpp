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

#include <Watermark/WatermarkGapThrottle.hpp>

#include <cstdint>
#include <latch>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <gtest/gtest.h>
#include <BackpressureManager.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{
constexpr uint64_t MAX_WATERMARK_GAP = 10;
constexpr OriginId ORIGIN_1{1};
constexpr OriginId ORIGIN_2{2};
constexpr OriginId SOURCE_1{101};
constexpr OriginId SOURCE_2{102};

/// Two input origins, each driven by its own source.
class WatermarkGapThrottleTest : public ::testing::Test
{
protected:
    void SetUp() override { Logger::setupLogging("WatermarkGapThrottleTest.log", LogLevel::LOG_DEBUG); }

    /// Advances the watermark of `origin` with its next sequence number and notifies the throttle, as a build buffer would.
    void advance(const OriginId origin, const Timestamp::Underlying watermark)
    {
        auto& sequenceNumber = nextSequenceNumber.try_emplace(origin, SequenceNumber::INITIAL).first->second;
        std::ignore = watermarks.updateWatermark(
            Timestamp(watermark), SequenceData(SequenceNumber(sequenceNumber++), ChunkNumber(ChunkNumber::INITIAL), true), origin);
        throttle.onWatermarkMoved(watermarks);
    }

    BackpressureManager manager{{SOURCE_1, SOURCE_2}, {{ORIGIN_1, {SOURCE_1}}, {ORIGIN_2, {SOURCE_2}}}};
    BackpressureListener source1 = manager.getListener(SOURCE_1);
    BackpressureListener source2 = manager.getListener(SOURCE_2);
    WatermarkGapThrottle throttle{{ORIGIN_1, ORIGIN_2}, MAX_WATERMARK_GAP};
    MultiOriginWatermarkProcessor watermarks{{ORIGIN_1, ORIGIN_2}};
    std::unordered_map<OriginId, SequenceNumber::Underlying> nextSequenceNumber;
};
}

TEST_F(WatermarkGapThrottleTest, BlocksTheSourceOfAnOriginThatRunsAheadByMoreThanTheGap)
{
    throttle.start(manager.newVoter());
    advance(ORIGIN_1, MAX_WATERMARK_GAP);
    EXPECT_FALSE(source1.isBlocked());

    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(source1.isBlocked());
    EXPECT_FALSE(source2.isBlocked());
}

/// Another voter cannot block the source of an input that is ahead but within the gap, while the window is on the critical path.
TEST_F(WatermarkGapThrottleTest, WaitsOnEveryInputItDoesNotHold)
{
    constexpr OriginId outputOrigin{3};
    const BackpressureManager plan{
        {SOURCE_1, SOURCE_2},
        {{ORIGIN_1, {SOURCE_1}}, {ORIGIN_2, {SOURCE_2}}, {outputOrigin, {SOURCE_1, SOURCE_2}}},
        {{outputOrigin, {ORIGIN_1, ORIGIN_2}}}};
    const auto planSource1 = plan.getListener(SOURCE_1);
    auto other = plan.newVoter();
    other.applyBackpressureTo(ORIGIN_1);
    throttle.start(plan.newWindowVoter(outputOrigin));

    advance(ORIGIN_1, MAX_WATERMARK_GAP);
    EXPECT_FALSE(planSource1.isBlocked());

    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(planSource1.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, UnblocksOnceTheGapHasHalved)
{
    throttle.start(manager.newVoter());
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(source1.isBlocked());

    /// Origin 1 is at MAX_WATERMARK_GAP + 1, so the gap is one more than half of MAX_WATERMARK_GAP.
    advance(ORIGIN_2, MAX_WATERMARK_GAP / 2);
    EXPECT_TRUE(source1.isBlocked());

    /// The gap is exactly half of MAX_WATERMARK_GAP.
    advance(ORIGIN_2, (MAX_WATERMARK_GAP / 2) + 1);
    EXPECT_FALSE(source1.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, UnblocksAnOriginThatIsDone)
{
    throttle.start(manager.newVoter());
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(source1.isBlocked());

    advance(ORIGIN_1, Timestamp::INFINITE_VALUE);
    EXPECT_FALSE(source1.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, SlowestOriginEndingUnblocksTheLeadingSource)
{
    throttle.start(manager.newVoter());
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    ASSERT_TRUE(source1.isBlocked());

    advance(ORIGIN_2, Timestamp::INFINITE_VALUE);
    EXPECT_FALSE(source1.isBlocked());
    EXPECT_FALSE(source2.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, NotificationsBeforeStartHaveNoEffect)
{
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_FALSE(source1.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, StartingAgainKeepsTheVotes)
{
    throttle.start(manager.newVoter());
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(source1.isBlocked());

    throttle.start(manager.newVoter());
    EXPECT_TRUE(source1.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, StopWithdrawsAllVotes)
{
    throttle.start(manager.newVoter());
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(source1.isBlocked());

    throttle.stop();
    EXPECT_FALSE(source1.isBlocked());

    advance(ORIGIN_1, 2 * MAX_WATERMARK_GAP);
    EXPECT_FALSE(source1.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, SlowestOriginVetoesTheVoteOfAnotherWindow)
{
    throttle.start(manager.newVoter());
    auto otherWindow = manager.newVoter();
    otherWindow.applyBackpressureTo(ORIGIN_2);
    advance(ORIGIN_1, 1);
    EXPECT_FALSE(source2.isBlocked());

    /// Origin 2 stays the slowest while origin 1 runs ahead, so origin 2 stays released.
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(source1.isBlocked());
    EXPECT_FALSE(source2.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, HardVoteIsNotVetoed)
{
    throttle.start(manager.newVoter());
    auto sink = manager.newVoter();
    sink.applyBackpressureToAll();
    advance(ORIGIN_1, 1);
    EXPECT_TRUE(source2.isBlocked());
}

/// Origin 1 runs far ahead, so its source is blocked.
/// Then several threads move origin 2 to the same watermark concurrently, so their notifications overlap.
/// NOTE: This is a smoke test, as the window in which a lost update would leave the source blocked is too small to hit reliably.
TEST_F(WatermarkGapThrottleTest, ConcurrentNotificationsEndWithTheSourceUnblocked)
{
    constexpr uint64_t rounds = 20;
    constexpr uint64_t steps = 4096;
    constexpr uint64_t threads = 4;
    for (uint64_t round = 0; round < rounds; ++round)
    {
        const BackpressureManager roundManager({SOURCE_1, SOURCE_2}, {{ORIGIN_1, {SOURCE_1}}, {ORIGIN_2, {SOURCE_2}}});
        const auto roundSource1 = roundManager.getListener(SOURCE_1);
        WatermarkGapThrottle roundThrottle({ORIGIN_1, ORIGIN_2}, MAX_WATERMARK_GAP);
        MultiOriginWatermarkProcessor roundWatermarks({ORIGIN_1, ORIGIN_2});
        roundThrottle.start(roundManager.newVoter());

        std::ignore = roundWatermarks.updateWatermark(
            Timestamp(steps), SequenceData(SequenceNumber(SequenceNumber::INITIAL), ChunkNumber(ChunkNumber::INITIAL), true), ORIGIN_1);
        roundThrottle.onWatermarkMoved(roundWatermarks);
        ASSERT_TRUE(roundSource1.isBlocked());
        {
            std::latch startTogether(threads);
            std::vector<std::jthread> movers;
            movers.reserve(threads);
            for (uint64_t thread = 0; thread < threads; ++thread)
            {
                movers.emplace_back(
                    [&, thread]
                    {
                        startTogether.arrive_and_wait();
                        /// Thread i owns the sequence numbers i + 1, i + 1 + threads, and so on, and the watermark is the sequence number.
                        for (uint64_t step = thread + 1; step <= steps; step += threads)
                        {
                            std::ignore = roundWatermarks.updateWatermark(
                                Timestamp(step), SequenceData(SequenceNumber(step), ChunkNumber(ChunkNumber::INITIAL), true), ORIGIN_2);
                            roundThrottle.onWatermarkMoved(roundWatermarks);
                        }
                    });
            }
        }
        ASSERT_FALSE(roundSource1.isBlocked()) << "in round " << round;
    }
}

TEST(WatermarkGapThrottleConstructionTest, RequiresSeveralInputOrigins)
{
    EXPECT_DEATH_DEBUG([[maybe_unused]] const WatermarkGapThrottle throttle({ORIGIN_1}, MAX_WATERMARK_GAP), "");
}

TEST(WatermarkGapThrottleConstructionTest, RequiresAGap)
{
    const std::vector origins{ORIGIN_1, ORIGIN_2};
    EXPECT_DEATH_DEBUG([[maybe_unused]] const WatermarkGapThrottle throttle(origins, 0), "");
}

}
