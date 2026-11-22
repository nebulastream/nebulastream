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
#include <bits/chrono.h>
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
    const auto unsignedOverflow = checkedSteadyDeadline(std::numeric_limits<uint64_t>::max(), "test", now);
    ASSERT_FALSE(unsignedOverflow);
    EXPECT_EQ(unsignedOverflow.error().code(), ErrorCode::ArithmeticalError);
    const auto tickOverflow = checkedSteadyDeadline(static_cast<uint64_t>(std::numeric_limits<int64_t>::max()), "test", now);
    ASSERT_FALSE(tickOverflow);
    EXPECT_EQ(tickOverflow.error().code(), ErrorCode::ArithmeticalError);
}

TEST(CheckedDeadlineTest, ChecksDeadlineAdditionAsWellAsDurationConversion)
{
    const auto now = Clock::time_point::max() - 10ms;
    const auto lastDeadline = checkedSteadyDeadline(10, "test", now);
    ASSERT_TRUE(lastDeadline);
    EXPECT_LE(*lastDeadline, Clock::time_point::max());
    const auto additionOverflow = checkedSteadyDeadline(11, "test", now);
    ASSERT_FALSE(additionOverflow);
    EXPECT_EQ(additionOverflow.error().code(), ErrorCode::ArithmeticalError);
    EXPECT_EQ(checkedSteadyDeadline(0, "test", Clock::time_point::max()), Clock::time_point::max());
    EXPECT_FALSE(checkedSteadyDeadline(1, "test", Clock::time_point::max()));
}

TEST(CheckedDeadlineTest, LargestWholeMillisecondDeadlineIsAccepted)
{
    const auto now = Clock::time_point{10s};
    const auto maximum
        = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::time_point::max() - now).count());
    const auto deadline = checkedSteadyDeadline(maximum, "test", now);
    ASSERT_TRUE(deadline);
    EXPECT_LE(*deadline, Clock::time_point::max());
    EXPECT_FALSE(checkedSteadyDeadline(maximum + 1, "test", now));
}

TEST(CheckedDeadlineTest, NegativeEpochDoesNotOverflowTheAvailabilityCalculation)
{
    const auto now = Clock::time_point{-10s};
    EXPECT_EQ(checkedSteadyDeadline(42, "test", now), now + 42ms);
}

}
