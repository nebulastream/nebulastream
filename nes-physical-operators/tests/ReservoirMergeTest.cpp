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

#include <algorithm>
#include <array>
#include <cstdint>
#include <unordered_set>
#include <vector>

#include <Statistics/ReservoirMerge.hpp>
#include <gtest/gtest.h>

namespace NES
{
namespace
{

std::vector<uint64_t> runSelection(
    const uint64_t seenLeft, const uint64_t seenRight, const uint64_t capacity, const uint64_t seed, ReservoirMergeSelection& selection)
{
    const uint64_t storedLeft = std::min(seenLeft, capacity);
    const uint64_t storedRight = std::min(seenRight, capacity);
    std::vector<uint64_t> positions(std::min(capacity, storedLeft + storedRight));
    selection = computeReservoirMergeSelection(seenLeft, storedLeft, seenRight, storedRight, capacity, seed, positions.data());
    return positions;
}

}

TEST(ReservoirMergeTest, selectionRespectsBoundsAndDistinctness)
{
    for (const auto [seenLeft, seenRight, capacity] :
         {std::array<uint64_t, 3>{100, 50, 10},
          std::array<uint64_t, 3>{5, 100, 10},
          std::array<uint64_t, 3>{100, 5, 10},
          std::array<uint64_t, 3>{7, 8, 10},
          std::array<uint64_t, 3>{1000, 1000, 1}})
    {
        ReservoirMergeSelection selection{};
        const auto positions = runSelection(seenLeft, seenRight, capacity, 42, selection);
        const uint64_t storedLeft = std::min(seenLeft, capacity);
        const uint64_t storedRight = std::min(seenRight, capacity);

        ASSERT_EQ(selection.total, std::min(capacity, storedLeft + storedRight));
        ASSERT_LE(selection.fromFirst, storedLeft);
        ASSERT_LE(selection.total - selection.fromFirst, storedRight);

        /// Positions must be in range and distinct within each side
        std::unordered_set<uint64_t> leftPositions;
        std::unordered_set<uint64_t> rightPositions;
        for (uint64_t i = 0; i < selection.total; ++i)
        {
            if (i < selection.fromFirst)
            {
                ASSERT_LT(positions[i], storedLeft);
                ASSERT_TRUE(leftPositions.insert(positions[i]).second) << "duplicate left position";
            }
            else
            {
                ASSERT_LT(positions[i], storedRight);
                ASSERT_TRUE(rightPositions.insert(positions[i]).second) << "duplicate right position";
            }
        }
    }
}

TEST(ReservoirMergeTest, deterministicForFixedInputsAndSeed)
{
    ReservoirMergeSelection selectionA{};
    ReservoirMergeSelection selectionB{};
    const auto positionsA = runSelection(1000, 500, 32, 7, selectionA);
    const auto positionsB = runSelection(1000, 500, 32, 7, selectionB);
    EXPECT_EQ(selectionA.fromFirst, selectionB.fromFirst);
    EXPECT_EQ(positionsA, positionsB);

    ReservoirMergeSelection selectionC{};
    const auto positionsC = runSelection(1000, 500, 32, 8, selectionC);
    EXPECT_TRUE(selectionA.fromFirst != selectionC.fromFirst || positionsA != positionsC) << "different seeds should differ";
}

TEST(ReservoirMergeTest, smallUnionKeepsEverything)
{
    /// If both reservoirs together fit the capacity, every stored tuple must survive
    ReservoirMergeSelection selection{};
    const auto positions = runSelection(4, 6, 10, 42, selection);
    EXPECT_EQ(selection.total, 10);
    EXPECT_EQ(selection.fromFirst, 4);
    (void)positions;
}

TEST(ReservoirMergeTest, hypergeometricMeanMatchesExpectation)
{
    /// E[fromFirst] = capacity * seenLeft / (seenLeft + seenRight)
    constexpr uint64_t seenLeft = 3000;
    constexpr uint64_t seenRight = 1000;
    constexpr uint64_t capacity = 100;
    constexpr int trials = 2000;
    double sum = 0;
    for (int trial = 0; trial < trials; ++trial)
    {
        ReservoirMergeSelection selection{};
        runSelection(seenLeft, seenRight, capacity, trial, selection);
        sum += static_cast<double>(selection.fromFirst);
    }
    const double mean = sum / trials;
    constexpr double expected = capacity * static_cast<double>(seenLeft) / (seenLeft + seenRight);
    /// Std dev of the hypergeometric here is ~4.2, so the mean over 2000 trials has a std error of ~0.1
    EXPECT_NEAR(mean, expected, 0.5);
}

}
