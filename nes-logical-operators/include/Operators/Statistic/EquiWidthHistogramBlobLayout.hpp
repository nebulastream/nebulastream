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

/// Layout of the serialized equi-width histogram blob:
/// | numberOfBins: uint64 @ 0 | minValue: uint64 @ 8 | maxValue: uint64 @ 16 | counters: uint64 * numberOfBins @ 24 |
///
/// The bin bounds are not stored. They follow from the three header numbers in O(1), so a blob of n bins costs
/// 24 + 8n bytes instead of carrying two bounds per bin, and the aggregation state is the counters alone.
///
/// There is no version, layout or counter-size field. These blobs live in the statistic store of one running system
/// and are never read by another build of the code, and the store already tags every blob with the type name of the
/// aggregation that wrote it. A different payload is a different type name, not a different version of this one.
///
/// The histogram is defined over unsigned integers only, so minValue and maxValue are plain uint64 and the bounds a
/// probe reports are UINT64.
///
/// Only the numbers live here, without the accessors that read and write them: the logical function needs them to
/// turn the memory budget from the SQL call into a bin count, and nes-logical-operators cannot depend on
/// nes-statistics, where Statistics/EquiWidthHistogramBlob.hpp adds the Nautilus accessors on top of this.
struct EquiWidthHistogramBlob
{
    static constexpr uint64_t NUMBER_OF_BINS_OFFSET = 0;
    static constexpr uint64_t MIN_VALUE_OFFSET = 8;
    static constexpr uint64_t MAX_VALUE_OFFSET = 16;
    static constexpr uint64_t HEADER_SIZE = 24;
    static constexpr uint64_t COUNTER_SIZE = sizeof(uint64_t);
};

/// The number of bins a memory budget of budgetBytes pays for, or 0 when it does not pay for a single bin. The
/// comparison comes before the subtraction so that a budget below the header cannot wrap.
constexpr uint64_t equiWidthHistogramBinsForBudget(const uint64_t budgetBytes)
{
    return budgetBytes < EquiWidthHistogramBlob::HEADER_SIZE + EquiWidthHistogramBlob::COUNTER_SIZE
        ? 0
        : (budgetBytes - EquiWidthHistogramBlob::HEADER_SIZE) / EquiWidthHistogramBlob::COUNTER_SIZE;
}

/// The smallest budget that pays for numberOfBins bins, i.e. the inverse of equiWidthHistogramBinsForBudget. Used to
/// tell a caller whose budget bought more bins than its value range can fill which budget it should have asked for.
constexpr uint64_t equiWidthHistogramBudgetForBins(const uint64_t numberOfBins)
{
    return EquiWidthHistogramBlob::HEADER_SIZE + (EquiWidthHistogramBlob::COUNTER_SIZE * numberOfBins);
}

/// The width of a regular bin: integer division, so the last bin absorbs the remainder. The caller guarantees
/// numberOfBins > 0 and maxValue - minValue >= numberOfBins, which makes this at least 1.
constexpr uint64_t equiWidthHistogramBinWidth(const uint64_t minValue, const uint64_t maxValue, const uint64_t numberOfBins)
{
    return (maxValue - minValue) / numberOfBins;
}

}
