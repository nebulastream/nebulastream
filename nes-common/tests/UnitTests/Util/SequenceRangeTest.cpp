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
#include <vector>
#include <Sequencing/FracturedNumber.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <gtest/gtest.h>

using namespace NES;

TEST(SequenceRangeTest, DefaultConstructorIsInvalid)
{
    auto range = SequenceRange::invalid();
    EXPECT_FALSE(range.isValid());
}

TEST(SequenceRangeTest, BasicConstruction)
{
    SequenceRange range(FracturedNumber(1), FracturedNumber(2));
    EXPECT_TRUE(range.isValid());
    EXPECT_EQ(range.start, FracturedNumber(1));
    EXPECT_EQ(range.end, FracturedNumber(2));
    EXPECT_EQ(range.toString(), "[1, 2)");
}

TEST(SequenceRangeTest, ConstructionFailsForInvalidRange)
{
    EXPECT_ANY_THROW(SequenceRange(FracturedNumber(2), FracturedNumber(1)));
    EXPECT_ANY_THROW(SequenceRange(FracturedNumber(1), FracturedNumber(1)));
}

TEST(SequenceRangeTest, IsAdjacent)
{
    SequenceRange r1(FracturedNumber(1), FracturedNumber(2));
    SequenceRange r2(FracturedNumber(2), FracturedNumber(3));
    SequenceRange r3(FracturedNumber(3), FracturedNumber(4));

    EXPECT_TRUE(r1.isAdjacentTo(r2));
    EXPECT_TRUE(r2.isAdjacentTo(r3));
    EXPECT_FALSE(r1.isAdjacentTo(r3));
}

TEST(SequenceRangeTest, Merge)
{
    SequenceRange r1(FracturedNumber(1), FracturedNumber(2));
    SequenceRange r2(FracturedNumber(2), FracturedNumber(3));

    auto merged = r1.merge(r2);
    EXPECT_EQ(merged.start, FracturedNumber(1));
    EXPECT_EQ(merged.end, FracturedNumber(3));
    EXPECT_EQ(merged.toString(), "[1, 3)");
}

TEST(SequenceRangeTest, MergeNonAdjacentThrows)
{
    SequenceRange r1(FracturedNumber(1), FracturedNumber(2));
    SequenceRange r3(FracturedNumber(3), FracturedNumber(4));
    EXPECT_ANY_THROW(static_cast<void>(r1.merge(r3)));
}

TEST(SequenceRangeTest, FractureIntoTwo)
{
    SequenceRange range(FracturedNumber(1), FracturedNumber(2));
    auto parts = range.fracture(2);

    ASSERT_EQ(parts.size(), 2);
    /// [1, 2) -> [1.0, 1.1) + [1.1, 2.0)
    EXPECT_EQ(parts[0].start, FracturedNumber(std::vector<uint64_t>{1, 0}));
    EXPECT_EQ(parts[0].end, FracturedNumber(std::vector<uint64_t>{1, 1}));
    EXPECT_EQ(parts[1].start, FracturedNumber(std::vector<uint64_t>{1, 1}));
    EXPECT_EQ(parts[1].end, FracturedNumber(std::vector<uint64_t>{2, 0}));

    /// Verify sub-ranges are adjacent
    EXPECT_TRUE(parts[0].isAdjacentTo(parts[1]));
}

TEST(SequenceRangeTest, FractureIntoThree)
{
    SequenceRange range(FracturedNumber(1), FracturedNumber(2));
    auto parts = range.fracture(3);

    ASSERT_EQ(parts.size(), 3);
    /// [1, 2) -> [1.0, 1.1) + [1.1, 1.2) + [1.2, 2.0)
    EXPECT_EQ(parts[0].start, FracturedNumber(std::vector<uint64_t>{1, 0}));
    EXPECT_EQ(parts[0].end, FracturedNumber(std::vector<uint64_t>{1, 1}));
    EXPECT_EQ(parts[1].start, FracturedNumber(std::vector<uint64_t>{1, 1}));
    EXPECT_EQ(parts[1].end, FracturedNumber(std::vector<uint64_t>{1, 2}));
    EXPECT_EQ(parts[2].start, FracturedNumber(std::vector<uint64_t>{1, 2}));
    EXPECT_EQ(parts[2].end, FracturedNumber(std::vector<uint64_t>{2, 0}));

    /// All sub-ranges are adjacent
    EXPECT_TRUE(parts[0].isAdjacentTo(parts[1]));
    EXPECT_TRUE(parts[1].isAdjacentTo(parts[2]));
}

TEST(SequenceRangeTest, RecursiveFracture)
{
    /// [1, 2) -> fracture(2) -> [1.0, 1.1) + [1.1, 2.0)
    /// [1.0, 1.1) -> fracture(2) -> [1.0.0, 1.0.1) + [1.0.1, 1.1.0)
    SequenceRange range(FracturedNumber(1), FracturedNumber(2));
    auto level1 = range.fracture(2);
    auto level2 = level1[0].fracture(2);

    ASSERT_EQ(level2.size(), 2);
    EXPECT_EQ(level2[0].start, FracturedNumber(std::vector<uint64_t>{1, 0, 0}));
    EXPECT_EQ(level2[0].end, FracturedNumber(std::vector<uint64_t>{1, 0, 1}));
    EXPECT_EQ(level2[1].start, FracturedNumber(std::vector<uint64_t>{1, 0, 1}));
    EXPECT_EQ(level2[1].end, FracturedNumber(std::vector<uint64_t>{1, 1, 0}));

    EXPECT_TRUE(level2[0].isAdjacentTo(level2[1]));
}

TEST(SequenceRangeTest, FractureMergeRoundTrip)
{
    SequenceRange range(FracturedNumber(5), FracturedNumber(6));
    auto parts = range.fracture(3);
    auto merged = parts[0].merge(parts[1]).merge(parts[2]);

    EXPECT_EQ(merged.start, FracturedNumber(std::vector<uint64_t>{5, 0}));
    EXPECT_EQ(merged.end, FracturedNumber(std::vector<uint64_t>{6, 0}));
}

TEST(SequenceRangeTest, ContainsPoint)
{
    SequenceRange range(FracturedNumber(std::vector<uint64_t>{1, 0}), FracturedNumber(std::vector<uint64_t>{2, 0}));
    EXPECT_TRUE(range.contains(FracturedNumber(std::vector<uint64_t>{1, 0})));
    EXPECT_TRUE(range.contains(FracturedNumber(std::vector<uint64_t>{1, 5})));
    EXPECT_FALSE(range.contains(FracturedNumber(std::vector<uint64_t>{2, 0})));
    EXPECT_FALSE(range.contains(FracturedNumber(std::vector<uint64_t>{0, 5})));
}

TEST(SequenceRangeTest, Covers)
{
    SequenceRange outer(FracturedNumber(1), FracturedNumber(5));
    SequenceRange inner(FracturedNumber(2), FracturedNumber(4));
    SequenceRange overlapping(FracturedNumber(3), FracturedNumber(6));

    EXPECT_TRUE(outer.covers(inner));
    EXPECT_FALSE(outer.covers(overlapping));
    EXPECT_FALSE(inner.covers(outer));
}

TEST(SequenceRangeTest, Overlaps)
{
    auto frac = [](auto... numbers) { return FracturedNumber(std::vector<uint64_t>{static_cast<uint64_t>(numbers)...}); };
    auto overlaps = [](auto seq1Start, auto seq1End, auto seq2start, auto seq2end)
    { return SequenceRange(seq1Start, seq1End).overlap(SequenceRange(seq2start, seq2end)); };

    EXPECT_TRUE(overlaps(frac(1, 0), frac(2, 0), frac(1, 0), frac(2, 0)));
    EXPECT_TRUE(overlaps(frac(1, 1), frac(2, 0), frac(1, 0), frac(1, 2)));
    EXPECT_FALSE(overlaps(frac(1, 0), frac(2, 0), frac(2, 0), frac(2, 1)));
}

TEST(SequenceRangeTest, Comparison)
{
    SequenceRange r1(FracturedNumber(1), FracturedNumber(2));
    SequenceRange r2(FracturedNumber(2), FracturedNumber(3));
    SequenceRange r3(FracturedNumber(1), FracturedNumber(3));

    EXPECT_LT(r1, r2);
    EXPECT_EQ(r1, r3); /// Same start means equal for comparison
}

TEST(SequenceRangeTest, ToString)
{
    SequenceRange range(FracturedNumber(std::vector<uint64_t>{1, 0}), FracturedNumber(std::vector<uint64_t>{2, 0}));
    EXPECT_EQ(range.toString(), "[1.0, 2.0)");
}
