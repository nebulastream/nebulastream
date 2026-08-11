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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <vector>

#include <UncompiledFieldIndexFunction.hpp>

namespace NES
{

/// Uncompiled (plain-C++) analog of the compiled Hl7FieldIndex: the SAME flattened structural-event band
/// (SimdHl7::flattenHl7SimdInto) and the SAME fixed-stride addressing -- a complete message of F leaves
/// contributes exactly F band events, so with I0 = the band index of the pair OPENING the first complete
/// message, record r's field i spans
///   [ band[I0 + r*F + i] + (i == 0 ? messageDelimiterSize : 1),  band[I0 + r*F + i + 1] )
/// -- read back with an interpreted per-field loop instead of the compiled strided read. This makes the
/// uncompiled rung differ from the compiled SequentialHL7 ONLY in interpretation vs compilation, not in
/// indexing algorithm (the UncompiledFieldBand vs FieldBand split for CSV).
///
/// Ownership: the FIF is constructed fresh per raw buffer (stack instance in the task), so it OWNS its band
/// storage -- no thread_local slot pool as in the compiled plugin (the pool exists there because three FIF
/// roles live in one static thread_local IndexPhaseResult; here each instance is private).
class UncompiledHl7FIF final : public UncompiledFieldIndexFunction<UncompiledHl7FIF>
{
    friend UncompiledFieldIndexFunction<UncompiledHl7FIF>;

    /// --- UncompiledFieldIndexFunction (CRTP) read interface ---

    [[nodiscard]] UncompiledFieldIndex applyGetByteOffsetOfFirstTuple() const { return this->offsetOfFirstTuple; }

    [[nodiscard]] UncompiledFieldIndex applyGetByteOffsetOfLastTuple() const { return this->offsetOfLastTuple; }

    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }

    [[nodiscard]] bool applyHasNext(const size_t tupleIdx) const { return tupleIdx < this->totalNumberOfTuples; }

    [[nodiscard]] std::string_view applyReadFieldAt(const std::string_view bufferView, const size_t tupleIdx, const size_t fieldIdx) const
    {
        const size_t recordBase = firstPairBandIdx + (tupleIdx * numFields);
        /// The first leaf starts after the 1- or 2-byte message delimiter; every later leaf after its 1-byte
        /// structural delimiter. The leaf ends AT the next event position.
        const UncompiledFieldIndex leafStartAdjustment = (fieldIdx == 0) ? static_cast<UncompiledFieldIndex>(delimiterSize) : 1U;
        const UncompiledFieldIndex fieldStart = band[recordBase + fieldIdx] + leafStartAdjustment;
        const UncompiledFieldIndex fieldEnd = band[recordBase + fieldIdx + 1];
        return bufferView.substr(fieldStart, fieldEnd - fieldStart);
    }

public:
    UncompiledHl7FIF() = default;
    ~UncompiledHl7FIF() = default;

    /// --- write-side contract (called by SequentialUncompiledHL7InputFormatIndexer) ---

    void startSetup(const size_t numberOfFieldsInSchema, const size_t messageDelimiterSize)
    {
        this->numFields = numberOfFieldsInSchema;
        this->delimiterSize = messageDelimiterSize;
        this->totalNumberOfTuples = 0;
        this->firstPairBandIdx = 0;
    }

    struct BandPointers
    {
        uint32_t* band;
        uint32_t* pairBandIdx;
    };

    /// Ensures capacity for the flatten's caller-provided-capacity contract (band >= viewSize + 16 for the
    /// unconditional 8-slot batches, pairBandIdx >= viewSize/2 + 2) and returns the writable bases.
    BandPointers prepareBands(const size_t viewSize)
    {
        if (band.size() < viewSize + 16)
        {
            band.resize(viewSize + 16);
        }
        if (pairBandIdx.size() < (viewSize / 2) + 2)
        {
            pairBandIdx.resize((viewSize / 2) + 2);
        }
        return {.band = band.data(), .pairBandIdx = pairBandIdx.data()};
    }

    void markNoTupleDelimiters()
    {
        this->offsetOfFirstTuple = std::numeric_limits<UncompiledFieldIndex>::max();
        this->offsetOfLastTuple = std::numeric_limits<UncompiledFieldIndex>::max();
        this->totalNumberOfTuples = 0;
    }

    void markWithTupleDelimiters(const UncompiledFieldIndex offsetToFirstTuple, const UncompiledFieldIndex offsetToLastTuple)
    {
        this->offsetOfFirstTuple = offsetToFirstTuple;
        this->offsetOfLastTuple = offsetToLastTuple;
    }

    /// Called after the flatten: records the band index of the first complete message's opening pair and the
    /// complete-record count (numPairs - 1).
    void setFlattenResult(const size_t firstPairIndex, const size_t numberOfRecords)
    {
        this->firstPairBandIdx = firstPairIndex;
        this->totalNumberOfTuples = numberOfRecords;
    }

private:
    std::vector<uint32_t> band; ///< growable storage; the reader bounds to totalNumberOfTuples
    std::vector<uint32_t> pairBandIdx;
    size_t firstPairBandIdx{}; ///< I0: band index of the pair opening the first complete message
    size_t numFields{};
    size_t delimiterSize{};
    size_t totalNumberOfTuples{};
    UncompiledFieldIndex offsetOfFirstTuple{};
    UncompiledFieldIndex offsetOfLastTuple{};
};

}
