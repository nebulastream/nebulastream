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

#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>

#include <ranges>
#include <Sequencing/FracturedNumber.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <gtest/gtest.h>
#include "Sequencing/RangeWatermarkTracker.hpp"

#include "BaseUnitTest.hpp"

using namespace NES;

TEST(SequenceRangeTest, EmptyDefaultConstruction)
{
    SequenceRangeTracker tracker(1);
    EXPECT_EQ(tracker.completedUpTo(), FracturedNumber(1));
    EXPECT_EQ(tracker.watermark(), FracturedNumber(1));
}

TEST(SequenceRangeTest, SimpleCompletionOfAdjacentRanges)
{
    SequenceRangeTracker tracker(1);
    tracker.completeRange(SequenceRange(FracturedNumber(1), FracturedNumber(2)));
    EXPECT_EQ(tracker.completedUpTo(), FracturedNumber(2));
    EXPECT_EQ(tracker.watermark(), FracturedNumber(2));
}

TEST(SequenceRangeTest, SimpleCompletionOfMultipleAdjacentRangesOutOfOrder)
{
    SequenceRangeTracker tracker(1);
    tracker.completeRange(SequenceRange(FracturedNumber(3), FracturedNumber(4)));
    tracker.completeRange(SequenceRange(FracturedNumber(1), FracturedNumber(2)));
    tracker.completeRange(SequenceRange(FracturedNumber(2), FracturedNumber(3)));
    EXPECT_EQ(tracker.completedUpTo(), FracturedNumber(4));
    EXPECT_EQ(tracker.watermark(), FracturedNumber(4));
}

TEST(SequenceRangeTest, RejectDifferentDeps)
{
    SequenceRangeTracker tracker(2);
    tracker.completeRange(SequenceRange(FracturedNumber({3, 1}), FracturedNumber({4, 0})));
    ASSERT_DEATH_DEBUG({ tracker.completeRange(SequenceRange(FracturedNumber(1), FracturedNumber(2))); }, "");
}

TEST(SequenceRangeTest, StressTest)
{
    std::vector ranges{
        SequenceRange(FracturedNumber(1), FracturedNumber(2)),
        SequenceRange(FracturedNumber(2), FracturedNumber(3)),
        SequenceRange(FracturedNumber(3), FracturedNumber(4)),
        SequenceRange(FracturedNumber(4), FracturedNumber(5)),
        SequenceRange(FracturedNumber(5), FracturedNumber(6)),
        SequenceRange(FracturedNumber(6), FracturedNumber(7)),
        SequenceRange(FracturedNumber(7), FracturedNumber(8)),
        SequenceRange(FracturedNumber(8), FracturedNumber(9)),
        SequenceRange(FracturedNumber(9), FracturedNumber(10))};

    auto fractures = ranges | std::views::transform([](SequenceRange r) { return r.fracture(5); }) | std::views::join
        | std::views::transform([](SequenceRange r) { return r.fracture(10); }) | std::views::join | std::ranges::to<std::vector>();
    fractures.emplace_back(SequenceRange(FracturedNumber({10, 0, 1}), FracturedNumber({10, 1, 0})));

    // ASSERT_EQ(fractures.size(), 451);

    std::random_device rd;
    std::ranges::shuffle(fractures, rd);

    SequenceRangeTracker tracker(3);
    for (auto fracture : fractures)
    {
        tracker.completeRange(fracture);
    }

    ASSERT_EQ(tracker.completedUpTo(), FracturedNumber({10, 0, 0}));
    ASSERT_EQ(tracker.watermark(), FracturedNumber({10, 1, 0}));
}
