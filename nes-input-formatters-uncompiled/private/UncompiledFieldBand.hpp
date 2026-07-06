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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <vector>

#include <UncompiledFieldIndexFunction.hpp>

namespace NES
{

/// Uncompiled (plain-C++) analog of the compiled `FieldBand` (nes-input-formatters). It is the field
/// index function that lets the UNCOMPILED CSV indexers reuse the fast branchless SIMD "flat band"
/// (`SimdCsv::indexBandInto`) instead of a per-field scalar `find` scan.
///
/// Where `UncompiledFieldOffsets<ONE>` stores, per tuple, `N+1` grouped, +1-adjusted field starts,
/// UncompiledFieldBand stores the raw structural positions the branchless walk emits: every unquoted
/// field delimiter and every tuple delimiter, in document order. Complete tuples live between
/// consecutive tuple delimiters; each spans exactly `N` band entries (`N-1` field delims + 1 tuple
/// delim). For tuple `tupleIdx` the reads are uniform (stride `N`, offset by the anchor index `b0`):
///   rawStart = band[b0 + tupleIdx*N + i]      (delimiter BEFORE field i; a tuple delim for i==0)
///   rawEnd   = band[b0 + tupleIdx*N + i + 1]  (delimiter AFTER  field i; a tuple delim for i==N-1)
///   fieldStart = rawStart + 1                 fieldSize = rawEnd - rawStart - 1
/// Both the parallel and the sequential CSV indexer use `indexBandInto` (b0 at the first newline); the
/// leading row [0 -> firstNewline] is completed outside the band (SequenceShredder in parallel mode, a
/// single-threaded carry accumulator in sequential mode -- the same split as the compiled formatters).
///
/// CAVEAT (mirrors the compiled FieldBand): positional fixed-stride N assumes exactly N structural
/// positions per tuple. A ragged row silently misaligns the whole band -- there is no per-tuple
/// field-count validation here (unlike the scalar grouping it replaces). Fine for well-formed data;
/// production needs a verification + scalar fallback.
class UncompiledFieldBand final : public UncompiledFieldIndexFunction<UncompiledFieldBand>
{
    friend UncompiledFieldIndexFunction<UncompiledFieldBand>;

    /// --- UncompiledFieldIndexFunction (CRTP) read interface ---

    [[nodiscard]] UncompiledFieldIndex applyGetByteOffsetOfFirstTuple() const { return this->offsetOfFirstTuple; }

    [[nodiscard]] UncompiledFieldIndex applyGetByteOffsetOfLastTuple() const { return this->offsetOfLastTuple; }

    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }

    [[nodiscard]] bool applyHasNext(size_t tupleIdx) const { return tupleIdx < this->totalNumberOfTuples; }

    [[nodiscard]] std::string_view applyReadFieldAt(const std::string_view bufferView, const size_t tupleIdx, const size_t fieldIdx) const
    {
        /// stride is N (raw positions), offset by b0 (the band index of the anchor tuple delimiter)
        const size_t tupleBase = (tupleIdx * numberOfFieldsInSchema) + firstTupleBandIndex;
        const UncompiledFieldIndex rawStart = band[tupleBase + fieldIdx]; ///< delimiter before field
        const UncompiledFieldIndex rawEnd = band[tupleBase + fieldIdx + 1]; ///< delimiter after field
        const UncompiledFieldIndex fieldStart = rawStart + 1U; ///< uint32 wraparound for the synthetic anchor
        const UncompiledFieldIndex fieldSize = rawEnd - rawStart - 1U;
        return bufferView.substr(fieldStart, fieldSize);
    }

public:
    UncompiledFieldBand() = default;
    ~UncompiledFieldBand() = default;

    /// --- write-side contract (called by the CSV indexers via SimdCsv::indexBand[Sequential]Into) ---

    void startSetup(size_t numberOfFieldsInSchema, size_t /*sizeOfFieldDelimiter*/)
    {
        this->numberOfFieldsInSchema = numberOfFieldsInSchema;
        this->totalNumberOfTuples = 0;
        this->firstTupleBandIndex = 0;
    }

    /// Ensures capacity for `maxPositions` structural offsets plus the flattenBits 16-slot branchless
    /// slack, and returns the writable band base. The true count is recorded later by finalize*().
    UncompiledFieldIndex* prepareBand(size_t maxPositions)
    {
        constexpr size_t flattenSlack = 16;
        if (band.size() < maxPositions + flattenSlack)
        {
            band.resize(maxPositions + flattenSlack);
        }
        return band.data();
    }

    void markNoTupleDelimiters()
    {
        this->offsetOfFirstTuple = std::numeric_limits<UncompiledFieldIndex>::max();
        this->offsetOfLastTuple = std::numeric_limits<UncompiledFieldIndex>::max();
        this->totalNumberOfTuples = 0;
    }

    /// PARALLEL finalize (used by both the parallel UncompiledCSV and the sequential SequentialUncompiledCSV
    /// -- the latter completes the leading row [0 -> firstDelim] separately from a carry accumulator, exactly
    /// like the compiled SequentialCSV; also used for the delimiter-wrapped spanning tuple `\n<record>\n`).
    /// `count` raw positions were written; firstDelim/lastDelim are the byte offsets of the first and last
    /// tuple delimiters. Derive the anchor index b0 and the complete-tuple count.
    void finalize(size_t count, UncompiledFieldIndex firstDelim, UncompiledFieldIndex lastDelim)
    {
        const auto* begin = band.data();
        const auto* end = band.data() + count;
        const auto b0 = static_cast<size_t>(std::lower_bound(begin, end, firstDelim) - begin);
        const auto bLast = static_cast<size_t>(std::lower_bound(begin, end, lastDelim) - begin);
        this->firstTupleBandIndex = b0;
        this->offsetOfFirstTuple = firstDelim;
        this->offsetOfLastTuple = lastDelim;
        this->totalNumberOfTuples = (numberOfFieldsInSchema == 0) ? 0 : ((bLast - b0) / numberOfFieldsInSchema);
    }

private:
    size_t numberOfFieldsInSchema{};
    size_t totalNumberOfTuples{};
    size_t firstTupleBandIndex{}; ///< b0: band index of the first tuple delimiter (anchor of tuple 0)
    UncompiledFieldIndex offsetOfFirstTuple{};
    UncompiledFieldIndex offsetOfLastTuple{};
    std::vector<UncompiledFieldIndex> band; ///< growable storage; the reader bounds to totalNumberOfTuples
};

}
