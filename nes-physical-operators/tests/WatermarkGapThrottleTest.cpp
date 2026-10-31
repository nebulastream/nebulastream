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

#include <chrono>
#include <cstdint>
#include <future>
#include <latch>
#include <optional>
#include <ranges>
#include <stop_token>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
namespace
{
constexpr uint64_t MAX_WATERMARK_GAP = 10;
constexpr OriginId ORIGIN_1{1};
constexpr OriginId ORIGIN_2{2};
constexpr OriginId SOURCE_1{101};
constexpr OriginId SOURCE_2{102};

/// Returns whether `wait` returns within `timeout`. A still blocked waiting thread is unblocked via its stop token.
bool passesWithin(const BackpressureListener& listener, const std::chrono::milliseconds timeout)
{
    std::promise<void> passed;
    auto future = passed.get_future();
    const std::jthread waiter(
        [&](const std::stop_token& stopToken)
        {
            listener.wait(stopToken);
            if (!stopToken.stop_requested())
            {
                passed.set_value();
            }
        });
    return future.wait_for(timeout) == std::future_status::ready;
}

/// A short grace period suffices to detect a blocked listener, as a slow waiting thread only makes it look more blocked.
bool isBlocked(const BackpressureListener& listener)
{
    return !passesWithin(listener, std::chrono::milliseconds(50));
}

/// An unblocked listener gets a generous timeout, as the waiting thread may be slow to start on a loaded machine.
bool isUnblocked(const BackpressureListener& listener)
{
    return passesWithin(listener, std::chrono::seconds(10));
}
}

/// Two input origins, each driven by its own source, unless a test passes other sources.
class WatermarkGapThrottleTest : public ::testing::Test
{
protected:
    void SetUp() override { Logger::setupLogging("WatermarkGapThrottleTest.log", LogLevel::LOG_DEBUG); }

    explicit WatermarkGapThrottleTest(SourcesOfInputOrigins sourcesOfInputOrigins = {{ORIGIN_1, {SOURCE_1}}, {ORIGIN_2, {SOURCE_2}}})
        : channels(createBackpressureChannels({SOURCE_1, SOURCE_2}))
        , throttle(sourcesOfInputOrigins)
        , watermarks(sourcesOfInputOrigins | std::views::keys | std::ranges::to<std::vector>())
    {
    }

    [[nodiscard]] std::optional<WatermarkBackpressure> newBackpressure() const
    {
        return WatermarkBackpressure{.controller = channels.first.newHolder(), .maxWatermarkGap = MAX_WATERMARK_GAP};
    }

    /// Advances the watermark of `origin` with its next sequence number and notifies the throttle, as a build buffer would.
    void advance(const OriginId origin, const Timestamp::Underlying watermark)
    {
        auto& sequenceNumber = nextSequenceNumber.try_emplace(origin, SequenceNumber::INITIAL).first->second;
        std::ignore = watermarks.updateWatermark(
            Timestamp(watermark), SequenceData(SequenceNumber(sequenceNumber++), ChunkNumber(ChunkNumber::INITIAL), true), origin);
        throttle.onWatermarkMoved(watermarks);
    }

    [[nodiscard]] const BackpressureListener& listener(const OriginId source) const { return channels.second.at(source); }

    std::pair<BackpressureController, std::unordered_map<OriginId, BackpressureListener>> channels;
    WatermarkGapThrottle throttle;
    MultiOriginWatermarkProcessor watermarks;
    std::unordered_map<OriginId, SequenceNumber::Underlying> nextSequenceNumber;
};

TEST_F(WatermarkGapThrottleTest, ThrottlesTheSourceOfAnOriginThatRunsAheadByMoreThanTheGap)
{
    throttle.start(newBackpressure());
    advance(ORIGIN_1, MAX_WATERMARK_GAP);
    EXPECT_TRUE(isUnblocked(listener(SOURCE_1)));

    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(isBlocked(listener(SOURCE_1)));
    EXPECT_TRUE(isUnblocked(listener(SOURCE_2)));
}

TEST_F(WatermarkGapThrottleTest, ReleasesOnceTheGapHasHalved)
{
    throttle.start(newBackpressure());
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(isBlocked(listener(SOURCE_1)));

    advance(ORIGIN_2, 5); /// gap 6
    EXPECT_TRUE(isBlocked(listener(SOURCE_1)));

    advance(ORIGIN_2, 6); /// gap 5
    EXPECT_TRUE(isUnblocked(listener(SOURCE_1)));
}

TEST_F(WatermarkGapThrottleTest, ReleasesAnOriginThatIsDone)
{
    throttle.start(newBackpressure());
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(isBlocked(listener(SOURCE_1)));

    advance(ORIGIN_1, Timestamp::INFINITE_VALUE);
    EXPECT_TRUE(isUnblocked(listener(SOURCE_1)));
}

TEST_F(WatermarkGapThrottleTest, StaysDisabledWithoutBackpressure)
{
    throttle.start(std::nullopt);
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(isUnblocked(listener(SOURCE_1)));
}

TEST_F(WatermarkGapThrottleTest, StartingAgainKeepsTheThrottle)
{
    throttle.start(newBackpressure());
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(isBlocked(listener(SOURCE_1)));

    throttle.start(newBackpressure());
    EXPECT_TRUE(isBlocked(listener(SOURCE_1)));
}

TEST_F(WatermarkGapThrottleTest, StopReleasesThrottledSourcesAndDisablesTheThrottle)
{
    throttle.start(newBackpressure());
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(isBlocked(listener(SOURCE_1)));

    throttle.stop();
    EXPECT_TRUE(isUnblocked(listener(SOURCE_1)));

    advance(ORIGIN_1, 2 * MAX_WATERMARK_GAP);
    EXPECT_TRUE(isUnblocked(listener(SOURCE_1)));
}

/// Origin 1 runs far ahead, so its source is throttled. Then several threads move origin 2 to the same watermark concurrently, so
/// most notifications find another thread updating, and the source must end up released.
/// This is a smoke test: the window in which a lost update would leave the source throttled is too small to hit reliably from here.
TEST_F(WatermarkGapThrottleTest, ConcurrentNotificationsEndWithTheSourceReleased)
{
    constexpr uint64_t rounds = 20;
    constexpr uint64_t steps = 4096;
    constexpr uint64_t threads = 4;
    for (uint64_t round = 0; round < rounds; ++round)
    {
        const SourcesOfInputOrigins sources{{ORIGIN_1, {SOURCE_1}}, {ORIGIN_2, {SOURCE_2}}};
        auto [controller, listeners] = createBackpressureChannels({SOURCE_1, SOURCE_2});
        WatermarkGapThrottle roundThrottle(sources);
        MultiOriginWatermarkProcessor roundWatermarks({ORIGIN_1, ORIGIN_2});
        roundThrottle.start(WatermarkBackpressure{.controller = controller.newHolder(), .maxWatermarkGap = MAX_WATERMARK_GAP});

        std::ignore = roundWatermarks.updateWatermark(
            Timestamp(steps), SequenceData(SequenceNumber(SequenceNumber::INITIAL), ChunkNumber(ChunkNumber::INITIAL), true), ORIGIN_1);
        roundThrottle.onWatermarkMoved(roundWatermarks);
        ASSERT_TRUE(isBlocked(listeners.at(SOURCE_1)));
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
                        /// Every thread owns every threads-th sequence number; the watermark is the sequence number.
                        for (uint64_t step = thread + 1; step <= steps; step += threads)
                        {
                            std::ignore = roundWatermarks.updateWatermark(
                                Timestamp(step), SequenceData(SequenceNumber(step), ChunkNumber(ChunkNumber::INITIAL), true), ORIGIN_2);
                            roundThrottle.onWatermarkMoved(roundWatermarks);
                        }
                    });
            }
        }
        ASSERT_TRUE(isUnblocked(listeners.at(SOURCE_1))) << "in round " << round;
    }
}

class WatermarkGapThrottleWithSharedSourceTest : public WatermarkGapThrottleTest
{
protected:
    WatermarkGapThrottleWithSharedSourceTest() : WatermarkGapThrottleTest({{ORIGIN_1, {SOURCE_1}}, {ORIGIN_2, {SOURCE_1, SOURCE_2}}}) { }
};

TEST_F(WatermarkGapThrottleWithSharedSourceTest, NeverThrottlesASourceOfTheSlowestOrigin)
{
    throttle.start(newBackpressure());
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(isUnblocked(listener(SOURCE_1)));
}

class WatermarkGapThrottleWithSingleOriginTest : public WatermarkGapThrottleTest
{
protected:
    WatermarkGapThrottleWithSingleOriginTest() : WatermarkGapThrottleTest({{ORIGIN_1, {SOURCE_1}}}) { }
};

TEST_F(WatermarkGapThrottleWithSingleOriginTest, StaysDisabled)
{
    throttle.start(newBackpressure());
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(isUnblocked(listener(SOURCE_1)));
}

class WatermarkGapThrottleWithUncontrolledSourceTest : public WatermarkGapThrottleTest
{
protected:
    WatermarkGapThrottleWithUncontrolledSourceTest() : WatermarkGapThrottleTest({{ORIGIN_1, {SOURCE_1}}, {ORIGIN_2, {OriginId(103)}}}) { }
};

TEST_F(WatermarkGapThrottleWithUncontrolledSourceTest, StaysDisabled)
{
    throttle.start(newBackpressure());
    advance(ORIGIN_1, MAX_WATERMARK_GAP + 1);
    EXPECT_TRUE(isUnblocked(listener(SOURCE_1)));
}

}
