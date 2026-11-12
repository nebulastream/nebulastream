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
#include <limits>
#include <Util/CheckedDeadline.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;
}

TEST(CheckedDeadlineTest, ZeroAndOrdinaryDurationsPreserveTheStartTime)
{
    const auto now = Clock::time_point{10s};
    EXPECT_EQ(checkedSteadyDeadline(0, "test", now), now);
    EXPECT_EQ(checkedSteadyDeadline(42, "test", now), now + 42ms);
}

TEST(CheckedDeadlineTest, RejectsUnsignedNarrowingAndClockTickOverflow)
{
    const auto now = Clock::time_point{};
    EXPECT_THROW(static_cast<void>(checkedSteadyDeadline(std::numeric_limits<uint64_t>::max(), "test", now)), Exception);
    EXPECT_THROW(
        static_cast<void>(checkedSteadyDeadline(static_cast<uint64_t>(std::numeric_limits<int64_t>::max()), "test", now)), Exception);
}

TEST(CheckedDeadlineTest, ChecksDeadlineAdditionAsWellAsDurationConversion)
{
    const auto now = Clock::time_point::max() - 10ms;
    EXPECT_LE(checkedSteadyDeadline(10, "test", now), Clock::time_point::max());
    EXPECT_THROW(static_cast<void>(checkedSteadyDeadline(11, "test", now)), Exception);
    EXPECT_EQ(checkedSteadyDeadline(0, "test", Clock::time_point::max()), Clock::time_point::max());
    EXPECT_THROW(static_cast<void>(checkedSteadyDeadline(1, "test", Clock::time_point::max())), Exception);
}

TEST(CheckedDeadlineTest, LargestWholeMillisecondDeadlineIsAccepted)
{
    const auto now = Clock::time_point{10s};
    const auto maximum
        = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::time_point::max() - now).count());
    EXPECT_LE(checkedSteadyDeadline(maximum, "test", now), Clock::time_point::max());
    EXPECT_THROW(static_cast<void>(checkedSteadyDeadline(maximum + 1, "test", now)), Exception);
}

TEST(CheckedDeadlineTest, NegativeEpochDoesNotOverflowTheAvailabilityCalculation)
{
    const auto now = Clock::time_point{-10s};
    EXPECT_EQ(checkedSteadyDeadline(42, "test", now), now + 42ms);
}

}
