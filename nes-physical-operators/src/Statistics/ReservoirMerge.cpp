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
#include <random>
#include <ranges>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

void drawDistinctPositions(const uint64_t populationSize, const uint64_t count, std::mt19937_64& gen, uint64_t*& positionsOut)
{
    const auto population = std::views::iota(uint64_t{0}, populationSize);
    positionsOut = std::ranges::sample(population, positionsOut, count, gen);
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
    std::uniform_int_distribution<uint64_t> dis;
    for (uint64_t i = 0; i < total; ++i)
    {
        if (dis(gen, std::uniform_int_distribution<uint64_t>::param_type{0, remaining - 1}) < remainingLeft)
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
