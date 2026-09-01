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

#pragma once

#include <cstdint>

namespace NES
{

/// Result of computeReservoirMergeSelection: the first `fromFirst` entries of positionsOut index into the
/// left reservoir, the remaining `total - fromFirst` entries index into the right reservoir.
struct ReservoirMergeSelection
{
    uint64_t fromFirst;
    uint64_t total;
};

/// Selects which tuples survive when merging two reservoir samples into one of at most `capacity` tuples.
///
/// Both inputs are uniform samples of their populations (seenLeft and seenRight tuples). A statistically exact
/// merge makes every tuple of the combined population equally likely to be in the result: the number of survivors
/// taken from the left reservoir follows Hypergeometric(seenLeft + seenRight, capacity, seenLeft), and the chosen
/// count of positions is then drawn uniformly without replacement from each reservoir.
/// The draw is deterministic for fixed inputs and seed.
///
/// positionsOut must have room for min(capacity, storedLeft + storedRight) entries.
/// Preconditions: storedLeft == min(seenLeft, capacity) and storedRight == min(seenRight, capacity), which
/// guarantees the hypergeometric split is always satisfiable.
ReservoirMergeSelection computeReservoirMergeSelection(
    uint64_t seenLeft,
    uint64_t storedLeft,
    uint64_t seenRight,
    uint64_t storedRight,
    uint64_t capacity,
    uint64_t seed,
    uint64_t* positionsOut);

}
