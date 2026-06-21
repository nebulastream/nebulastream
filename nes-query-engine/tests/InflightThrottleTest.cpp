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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <stop_token>
#include <thread>
#include <gtest/gtest.h>
#include <InflightThrottle.hpp>

using namespace NES;
using namespace std::chrono_literals;

class InflightThrottleTest : public ::testing::Test
{
};

/// AIMD policy is a pure value object -- additive-increase on stall, clamped to max.
TEST_F(InflightThrottleTest, PolicyAdditiveIncreaseClampedToMax)
{
    InflightPolicy policy{.min = 2, .max = 16, .current = 4, .additiveStep = 4};
    EXPECT_EQ(policy.onStall(), 8U);
    EXPECT_EQ(policy.onStall(), 12U);
    EXPECT_EQ(policy.onStall(), 16U);
    EXPECT_EQ(policy.onStall(), 16U); /// clamp at max
}

/// Multiplicative-decrease on idle, clamped to min.
TEST_F(InflightThrottleTest, PolicyMultiplicativeDecreaseClampedToMin)
{
    InflightPolicy policy{.min = 2, .max = 16, .current = 16};
    EXPECT_EQ(policy.onIdleDecay(), 8U);
    EXPECT_EQ(policy.onIdleDecay(), 4U);
    EXPECT_EQ(policy.onIdleDecay(), 2U);
    EXPECT_EQ(policy.onIdleDecay(), 2U); /// clamp at min
}

/// Acquire reports whether it had to wait (the saturation signal), and release frees a slot.
TEST_F(InflightThrottleTest, AcquireReleaseAndWaitSignal)
{
    InflightThrottle throttle(2);
    std::stop_source stop;
    const auto first = throttle.acquire(stop.get_token());
    EXPECT_TRUE(first.acquired);
    EXPECT_FALSE(first.waited);
    const auto second = throttle.acquire(stop.get_token());
    EXPECT_TRUE(second.acquired);
    EXPECT_FALSE(second.waited);
    EXPECT_EQ(throttle.currentInUse(), 2U);

    throttle.release();
    EXPECT_EQ(throttle.currentInUse(), 1U);
    const auto third = throttle.acquire(stop.get_token());
    EXPECT_TRUE(third.acquired);
    EXPECT_EQ(throttle.currentInUse(), 2U);
}

/// A blocked acquire returns (without taking a slot) when the source is asked to stop.
TEST_F(InflightThrottleTest, StopUnblocksBlockedAcquire)
{
    InflightThrottle throttle(1);
    std::stop_source stop;
    ASSERT_TRUE(throttle.acquire(stop.get_token()).acquired); /// take the only slot

    std::atomic<bool> finished{false};
    InflightThrottle::AcquireResult result{};
    std::thread worker(
        [&]
        {
            result = throttle.acquire(stop.get_token());
            finished = true;
        });

    std::this_thread::sleep_for(50ms);
    EXPECT_FALSE(finished.load()); /// blocked: no slot available

    stop.request_stop();
    worker.join();
    EXPECT_FALSE(result.acquired); /// returned due to stop, no slot taken
    EXPECT_TRUE(result.waited);
    EXPECT_EQ(throttle.currentInUse(), 1U); /// still only the first slot
}

/// Growing the limit wakes a waiter (the adaptive-increase path).
TEST_F(InflightThrottleTest, SetLimitGrowWakesWaiter)
{
    InflightThrottle throttle(1);
    std::stop_source stop;
    ASSERT_TRUE(throttle.acquire(stop.get_token()).acquired); /// take the only slot

    std::atomic<bool> acquired{false};
    std::thread worker(
        [&]
        {
            if (throttle.acquire(stop.get_token()).acquired)
            {
                acquired = true;
            }
        });

    std::this_thread::sleep_for(50ms);
    EXPECT_FALSE(acquired.load()); /// blocked at limit 1

    throttle.setLimit(2); /// grow -> waiter wakes and acquires
    worker.join();
    EXPECT_TRUE(acquired.load());
    EXPECT_EQ(throttle.currentInUse(), 2U);
}
