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

#include <algorithm>
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
constexpr OriginId OUTPUT_ORIGIN{3};
constexpr OriginId SOURCE_1{101};
constexpr OriginId SOURCE_2{102};

/// Passes the current watermarks of `watermarks` to `throttle`, as the window handler does.
void notify(WatermarkGapThrottle& throttle, const MultiOriginWatermarkProcessor& watermarks)
{
    throttle.onWatermarksMoved(watermarks);
}

/// A window over two input origins, each driven by its own source.
/// The throttle votes with an internal voter, as in a running query, so its releases are waits on the critical path.
BackpressureManager createWindowPlan()
{
    return BackpressureManager{
        {SOURCE_1, SOURCE_2},
        {.sourcesOfOrigins = {{ORIGIN_1, {SOURCE_1}}, {ORIGIN_2, {SOURCE_2}}, {OUTPUT_ORIGIN, {SOURCE_1, SOURCE_2}}},
         .inputsOfOrigins = {{OUTPUT_ORIGIN, {ORIGIN_1, ORIGIN_2}}}}};
}

class WatermarkGapThrottleTest : public Testing::BaseUnitTest
{
protected:
    static void SetUpTestSuite() { Logger::setupLogging("WatermarkGapThrottleTest.log", LogLevel::LOG_DEBUG); }

    /// Advances the watermark of `origin` with its next sequence number and notifies the throttle, as a build buffer would.
    /// Returns whether the update notified the throttle.
    bool advance(const OriginId origin, const Timestamp::Underlying watermark)
    {
        auto& sequenceNumber = nextSequenceNumber.try_emplace(origin, SequenceNumber::INITIAL).first->second;
        const auto update = watermarks.updateWatermark(
            Timestamp(watermark), SequenceData(SequenceNumber(sequenceNumber++), ChunkNumber(ChunkNumber::INITIAL), true), origin);
        const bool slowestAdvanced = update.global > highestGlobal;
        highestGlobal = std::max(highestGlobal, update.global);
        const bool notified = throttle.mayChangeVotes(origin, update.origin, update.global, update.originAdvanced, slowestAdvanced);
        if (notified)
        {
            notify(throttle, watermarks);
        }
        return notified;
    }

    BackpressureManager manager = createWindowPlan();
    BackpressureListener source1 = manager.getListener(SOURCE_1);
    BackpressureListener source2 = manager.getListener(SOURCE_2);
    WatermarkGapThrottle throttle{{ORIGIN_1, ORIGIN_2}, MAX_WATERMARK_GAP};
    MultiOriginWatermarkProcessor watermarks{{ORIGIN_1, ORIGIN_2}};
    std::unordered_map<OriginId, SequenceNumber::Underlying> nextSequenceNumber;
    Timestamp highestGlobal{Timestamp::INITIAL_VALUE};
};
}

TEST_F(WatermarkGapThrottleTest, BlocksTheSourceOfAnOriginThatRunsAheadByMoreThanTheGap)
{
    throttle.start(manager.newInternalVoter(OUTPUT_ORIGIN));
    advance(ORIGIN_1, MAX_WATERMARK_GAP);
    EXPECT_FALSE(source1.isBlocked());

    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(source1.isBlocked());
    EXPECT_FALSE(source2.isBlocked());
}

/// Another voter cannot block the source of an input that is ahead but within the gap, while the window is on the critical path.
TEST_F(WatermarkGapThrottleTest, WaitsOnEveryInputItDoesNotHold)
{
    auto other = manager.newInternalVoter(OUTPUT_ORIGIN);
    other.applyBackpressureTo(ORIGIN_1);
    throttle.start(manager.newInternalVoter(OUTPUT_ORIGIN));

    /// The slowest origin advances, so the throttle casts its first votes: it holds no origin and waits on both.
    advance(ORIGIN_2, 1);
    EXPECT_FALSE(source1.isBlocked());

    /// Origin 1 is ahead by exactly the gap. The throttle still waits on it, so the apply of the other voter has no effect.
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_FALSE(source1.isBlocked());

    advance(ORIGIN_1, MAX_WATERMARK_GAP + 2);
    EXPECT_TRUE(source1.isBlocked());
    EXPECT_FALSE(source2.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, UnblocksOnceTheGapHasHalved)
{
    throttle.start(manager.newInternalVoter(OUTPUT_ORIGIN));
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(source1.isBlocked());

    /// Origin 1 is at MAX_WATERMARK_GAP + 1, so the gap is one more than half of MAX_WATERMARK_GAP.
    advance(ORIGIN_2, MAX_WATERMARK_GAP / 2);
    EXPECT_TRUE(source1.isBlocked());

    /// The gap is exactly half of MAX_WATERMARK_GAP.
    advance(ORIGIN_2, (MAX_WATERMARK_GAP / 2) + 1);
    EXPECT_FALSE(source1.isBlocked());
}

/// A held origin cannot be held again, so its buffers that are still in flight do not notify the throttle.
TEST_F(WatermarkGapThrottleTest, AHeldOriginNotifiesOnlyWhenItIsDone)
{
    throttle.start(manager.newInternalVoter(OUTPUT_ORIGIN));
    EXPECT_TRUE(advance(ORIGIN_1, MAX_WATERMARK_GAP + 1));
    ASSERT_TRUE(source1.isBlocked());

    EXPECT_FALSE(advance(ORIGIN_1, MAX_WATERMARK_GAP + 2));
    EXPECT_TRUE(source1.isBlocked());

    /// The slowest origin still notifies, and the release makes the next update of origin 1 notify again.
    EXPECT_TRUE(advance(ORIGIN_2, MAX_WATERMARK_GAP));
    ASSERT_FALSE(source1.isBlocked());
    EXPECT_TRUE(advance(ORIGIN_1, (2 * MAX_WATERMARK_GAP) + 1));
    ASSERT_TRUE(source1.isBlocked());

    EXPECT_TRUE(advance(ORIGIN_1, Timestamp::INFINITE_VALUE));
    EXPECT_FALSE(source1.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, UnblocksAnOriginThatIsDone)
{
    throttle.start(manager.newInternalVoter(OUTPUT_ORIGIN));
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(source1.isBlocked());

    advance(ORIGIN_1, Timestamp::INFINITE_VALUE);
    EXPECT_FALSE(source1.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, SlowestOriginEndingUnblocksTheLeadingSource)
{
    throttle.start(manager.newInternalVoter(OUTPUT_ORIGIN));
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    ASSERT_TRUE(source1.isBlocked());

    advance(ORIGIN_2, Timestamp::INFINITE_VALUE);
    EXPECT_FALSE(source1.isBlocked());
    EXPECT_FALSE(source2.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, StopWithdrawsAllVotes)
{
    throttle.start(manager.newInternalVoter(OUTPUT_ORIGIN));
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(source1.isBlocked());

    throttle.stop();
    EXPECT_FALSE(source1.isBlocked());
}

/// Threads read their snapshots before they take the lock of the throttle, so snapshots can arrive out of order.
TEST_F(WatermarkGapThrottleTest, AnOlderSnapshotDoesNotReleaseAHeldOrigin)
{
    throttle.start(manager.newInternalVoter(OUTPUT_ORIGIN));
    const std::vector newer{Timestamp(MAX_WATERMARK_GAP + 1), Timestamp(Timestamp::INITIAL_VALUE)};
    const std::vector older{Timestamp(Timestamp::INITIAL_VALUE), Timestamp(Timestamp::INITIAL_VALUE)};
    throttle.onWatermarksMoved(newer);
    EXPECT_TRUE(source1.isBlocked());

    throttle.onWatermarksMoved(older);
    EXPECT_TRUE(source1.isBlocked());
}

/// The window waits on its slowest origin, which puts that origin on the critical path.
TEST_F(WatermarkGapThrottleTest, AnotherVoterCannotBlockTheSlowestOrigin)
{
    throttle.start(manager.newInternalVoter(OUTPUT_ORIGIN));
    auto other = manager.newInternalVoter(OUTPUT_ORIGIN);
    other.applyBackpressureTo(ORIGIN_2);
    advance(ORIGIN_1, 1);
    EXPECT_FALSE(source2.isBlocked());

    /// Origin 2 stays the slowest while origin 1 runs ahead, so origin 2 stays released.
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(source1.isBlocked());
    EXPECT_FALSE(source2.isBlocked());
}

TEST_F(WatermarkGapThrottleTest, HardVoteBlocksEvenTheSlowestOrigin)
{
    throttle.start(manager.newInternalVoter(OUTPUT_ORIGIN));
    auto sink = manager.newExternalVoter();
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
        const auto roundManager = createWindowPlan();
        const auto roundSource1 = roundManager.getListener(SOURCE_1);
        WatermarkGapThrottle roundThrottle({ORIGIN_1, ORIGIN_2}, MAX_WATERMARK_GAP);
        MultiOriginWatermarkProcessor roundWatermarks({ORIGIN_1, ORIGIN_2});
        roundThrottle.start(roundManager.newInternalVoter(OUTPUT_ORIGIN));

        std::ignore = roundWatermarks.updateWatermark(
            Timestamp(steps), SequenceData(SequenceNumber(SequenceNumber::INITIAL), ChunkNumber(ChunkNumber::INITIAL), true), ORIGIN_1);
        notify(roundThrottle, roundWatermarks);
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
                            notify(roundThrottle, roundWatermarks);
                        }
                    });
            }
        }
        ASSERT_FALSE(roundSource1.isBlocked()) << "in round " << round;
    }
}

TEST(WatermarkGapThrottleConstructionTest, RequiresSeveralInputOrigins)
{
    SKIP_IF_TSAN();
    /// The tests before this one start the logger thread. A death test that forks could then wait on a thread that the child does not have.
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH_DEBUG([[maybe_unused]] const WatermarkGapThrottle throttle({ORIGIN_1}, MAX_WATERMARK_GAP), "");
}

TEST(WatermarkGapThrottleConstructionTest, RequiresAGap)
{
    SKIP_IF_TSAN();
    /// The tests before this one start the logger thread. A death test that forks could then wait on a thread that the child does not have.
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const std::vector origins{ORIGIN_1, ORIGIN_2};
    EXPECT_DEATH_DEBUG([[maybe_unused]] const WatermarkGapThrottle throttle(origins, 0), "");
}

}
