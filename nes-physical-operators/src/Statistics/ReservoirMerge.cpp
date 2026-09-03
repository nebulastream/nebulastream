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

#include <Statistics/ReservoirMerge.hpp>

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <random>
#include <vector>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// Draws `count` distinct positions from [0, populationSize) via a partial Fisher-Yates shuffle and appends them
/// to positionsOut.
void drawDistinctPositions(const uint64_t populationSize, const uint64_t count, std::mt19937_64& gen, uint64_t*& positionsOut)
{
    std::vector<uint64_t> positions(populationSize);
    std::iota(positions.begin(), positions.end(), 0);
    for (uint64_t i = 0; i < count; ++i)
    {
        std::uniform_int_distribution<uint64_t> dis(i, populationSize - 1);
        std::swap(positions[i], positions[dis(gen)]);
        *positionsOut = positions[i];
        ++positionsOut; /// NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    }
}

}

ReservoirMergeSelection computeReservoirMergeSelection(
    const uint64_t seenLeft,
    const uint64_t storedLeft,
    const uint64_t seenRight,
    const uint64_t storedRight,
    const uint64_t capacity,
    const uint64_t seed,
    uint64_t* positionsOut)
{
    PRECONDITION(positionsOut != nullptr, "positionsOut must not be null");
    PRECONDITION(storedLeft == std::min(seenLeft, capacity), "left reservoir must hold min(seen, capacity) tuples");
    PRECONDITION(storedRight == std::min(seenRight, capacity), "right reservoir must hold min(seen, capacity) tuples");

    const uint64_t total = std::min(capacity, storedLeft + storedRight);

    /// Deterministic for fixed inputs; mixing in the seen counts decorrelates concurrent merges with the same seed.
    std::mt19937_64 gen(folly::hash::hash_combine(seed, seenLeft, seenRight, capacity));

    /// Sequential urn draws: each of the `total` survivor slots picks a tuple uniformly from the remaining
    /// combined population, giving fromFirst ~ Hypergeometric(seenLeft + seenRight, total, seenLeft).
    uint64_t fromFirst = 0;
    uint64_t remainingLeft = seenLeft;
    uint64_t remaining = seenLeft + seenRight;
    for (uint64_t i = 0; i < total; ++i)
    {
        std::uniform_int_distribution<uint64_t> dis(0, remaining - 1);
        if (dis(gen) < remainingLeft)
        {
            ++fromFirst;
            --remainingLeft;
        }
        --remaining;
    }

    /// fromFirst <= storedLeft holds: fromFirst <= min(total, seenLeft) <= min(capacity, seenLeft) = storedLeft.
    drawDistinctPositions(storedLeft, fromFirst, gen, positionsOut);
    drawDistinctPositions(storedRight, total - fromFirst, gen, positionsOut);

    return {.fromFirst = fromFirst, .total = total};
}

}
