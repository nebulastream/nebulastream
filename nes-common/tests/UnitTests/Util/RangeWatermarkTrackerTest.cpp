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
#include <thread>
#include <vector>
#include <Sequencing/FracturedNumber.hpp>
#include <Sequencing/RangeWatermarkTracker.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <Time/Timestamp.hpp>
#include <gtest/gtest.h>

using namespace NES;

TEST(RangeWatermarkTrackerTest, SingleRange)
{
    RangeWatermarkTracker tracker;
    tracker.insert(SequenceRange(FracturedNumber(1), FracturedNumber(2)), Timestamp(100));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 100);
}

TEST(RangeWatermarkTrackerTest, SequentialRanges)
{
    RangeWatermarkTracker tracker;
    tracker.insert(SequenceRange(FracturedNumber(1), FracturedNumber(2)), Timestamp(100));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 100);

    tracker.insert(SequenceRange(FracturedNumber(2), FracturedNumber(3)), Timestamp(200));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 200);

    tracker.insert(SequenceRange(FracturedNumber(3), FracturedNumber(4)), Timestamp(300));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 300);
}

TEST(RangeWatermarkTrackerTest, OutOfOrder)
{
    RangeWatermarkTracker tracker;
    tracker.insert(SequenceRange(FracturedNumber(2), FracturedNumber(3)), Timestamp(200));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 0);

    tracker.insert(SequenceRange(FracturedNumber(1), FracturedNumber(2)), Timestamp(100));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 200);
}

TEST(RangeWatermarkTrackerTest, FusedRange)
{
    RangeWatermarkTracker tracker;
    tracker.insert(SequenceRange(FracturedNumber(1), FracturedNumber(5)), Timestamp(200));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 200);
}

TEST(RangeWatermarkTrackerTest, FracturedRangesDepth2)
{
    RangeWatermarkTracker tracker;
    tracker.insert(
        SequenceRange(FracturedNumber(std::vector<uint64_t>{1, 0}), FracturedNumber(std::vector<uint64_t>{1, 1})), Timestamp(50));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 50);

    tracker.insert(
        SequenceRange(FracturedNumber(std::vector<uint64_t>{1, 1}), FracturedNumber(std::vector<uint64_t>{2, 0})), Timestamp(100));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 100);
}

TEST(RangeWatermarkTrackerTest, GapPreventsAdvance)
{
    RangeWatermarkTracker tracker;
    tracker.insert(SequenceRange(FracturedNumber(1), FracturedNumber(2)), Timestamp(100));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 100);

    /// Insert range 3-4, skipping 2-3 — watermark should not advance past 100
    tracker.insert(SequenceRange(FracturedNumber(3), FracturedNumber(4)), Timestamp(300));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 100);

    /// Fill the gap — watermark should jump to 300
    tracker.insert(SequenceRange(FracturedNumber(2), FracturedNumber(3)), Timestamp(200));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 300);
}

TEST(RangeWatermarkTrackerTest, ConcurrentInserts)
{
    RangeWatermarkTracker tracker;
    constexpr int numThreads = 4;
    constexpr int rangesPerThread = 250;
    constexpr int totalRanges = numThreads * rangesPerThread;

    std::vector<std::thread> threads;
    for (int t = 0; t < numThreads; ++t)
    {
        threads.emplace_back(
            [&tracker, t]
            {
                for (int i = 0; i < rangesPerThread; ++i)
                {
                    uint64_t seq = static_cast<uint64_t>(t * rangesPerThread + i + 1);
                    tracker.insert(SequenceRange(FracturedNumber(seq), FracturedNumber(seq + 1)), Timestamp(seq * 10));
                }
            });
    }
    for (auto& thread : threads)
    {
        thread.join();
    }

    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), totalRanges * 10);
}

TEST(RangeWatermarkTrackerTest, WatermarkMonotonicity)
{
    RangeWatermarkTracker tracker;
    /// Insert ranges with decreasing timestamps — watermark should never decrease
    tracker.insert(SequenceRange(FracturedNumber(1), FracturedNumber(2)), Timestamp(300));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 300);

    tracker.insert(SequenceRange(FracturedNumber(2), FracturedNumber(3)), Timestamp(100));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 300);

    tracker.insert(SequenceRange(FracturedNumber(3), FracturedNumber(4)), Timestamp(200));
    EXPECT_EQ(tracker.getCurrentWatermark().getRawValue(), 300);
}

TEST(RangeWatermarkTrackerTest, DepthEnforcementDeathTest)
{
    RangeWatermarkTracker tracker;
    tracker.insert(SequenceRange(FracturedNumber(1), FracturedNumber(2)), Timestamp(100));

    /// Inserting a depth-2 range into a depth-1 tracker should fail
    EXPECT_DEATH(
        tracker.insert(
            SequenceRange(FracturedNumber(std::vector<uint64_t>{2, 0}), FracturedNumber(std::vector<uint64_t>{2, 1})),
            Timestamp(200)),
        "");
}
