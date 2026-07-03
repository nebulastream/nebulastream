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
#include <memory>
#include <unordered_map>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/inline.hpp> /// TMP DIAGNOSTIC: NAUTILUS_INLINE marker for getBandDataProxy
#include <ErrorHandling.hpp>
#include <FieldIndexFunction.hpp>
#include <FieldOffsets.hpp> /// for the free `includesField` helper (shared with FieldOffsets)
#include <RawTupleBuffer.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// EXPERIMENT (REVERT/productionize before merge): a FieldIndexFunction over the simdcsv "flat band".
///
/// Where FieldOffsets<ONE> stores, per tuple, N+1 *grouped, +1-adjusted field starts*
/// `[field0Start, .., field(N-1)Start, newlinePos]` (produced by the per-field scalar grouping that makes
/// the structured indexer ~= the scalar one), FieldBand stores the *raw structural positions* the branchless
/// `flattenBits` walk emits: every unquoted comma and every newline, in document order. Same `uint32` band,
/// different contents — so the speed-up lives entirely on the write side (no per-field grouping / pending).
///
/// Band layout for one raw buffer: `[ leadFragmentCommas..., nl_0, c,c,..,c(N-1), nl_1, c,.., nl_2, .. , trailFragmentCommas ]`.
/// `nl_0` is the first newline (it terminates the leading spanning fragment and is the *anchor* of the first
/// complete tuple). Complete tuples live between consecutive newlines; each spans exactly N band entries
/// (N-1 commas + 1 newline) for well-formed fixed-arity CSV, so newline k sits at `b0 + k*N` where `b0` is
/// the band index of `nl_0`. For tuple `recordIndex` (0-based) the field reads are uniform:
///   start = band[b0 + recordIndex*N + i]      (the delimiter *before* field i; a newline for i==0)
///   end   = band[b0 + recordIndex*N + i + 1]  (the delimiter *after*  field i; a newline for i==N-1)
///   fieldAddress = recordBufferPtr + start + 1     fieldSize = end - start - 1   (one delimiter byte)
/// Spanning tuples are indexed on a buffer the shredder wraps as `\n<record>\n`, so b0==0, T==1 — same path.
///
/// CAVEAT (experiment-only): positional fixed-stride N assumes exactly N structural positions per tuple. A
/// ragged row silently misaligns the whole band (no per-tuple field-count validation, unlike indexInto).
/// Fine for well-formed benchmark data; production needs a verification + scalar fallback.
class FieldBand final : public FieldIndexFunction<FieldBand>
{
    friend FieldIndexFunction<FieldBand>;

    [[nodiscard]] FieldIndex applyGetByteOffsetOfFirstTuple() const { return this->offsetOfFirstTuple; }

    [[nodiscard]] FieldIndex applyGetByteOffsetOfLastTuple() const { return this->offsetOfLastTuple; }

    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }

    /// TMP DIAGNOSTIC (benchmark experiment): mark for nautilus inlining (perf showed the proxy call at
    /// ~3.4% self-time). nes-input-formatters is already pass-applied, so the marker alone suffices here.
    NAUTILUS_INLINE static const FieldIndex* getBandDataProxy(const FieldBand* const fieldBand) { return fieldBand->band; }

    [[nodiscard]] nautilus::val<bool> applyHasNext(const nautilus::val<uint64_t>& recordIdx, nautilus::val<FieldBand*> fieldBandPtr) const
    {
        nautilus::val<uint64_t> total = *getMemberWithOffset<size_t>(fieldBandPtr, offsetof(FieldBand, totalNumberOfTuples));
        return recordIdx < total;
    }

    template <typename IndexerMetaData>
    [[nodiscard]] Record applyReadSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& recordBufferPtr,
        const nautilus::val<uint64_t>& recordIndex,
        const IndexerMetaData& metaData,
        nautilus::val<FieldBand*> fieldBandPtr,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const std::unordered_map<DataType::Type, bool>& lazyOverloads) const
    {
        Record record;
        const auto bandPtr = invoke(getBandDataProxy, fieldBandPtr);
        const nautilus::val<uint64_t> base0 = *getMemberWithOffset<size_t>(fieldBandPtr, offsetof(FieldBand, firstTupleBandIndex));
        /// stride is N (raw positions), not N+1, and offset by b0 (the band index of the anchor newline)
        const auto tupleBase = (recordIndex * nautilus::static_val(metaData.getNumberOfFields())) + base0;
        for (nautilus::static_val<uint64_t> i = 0; i < metaData.getNumberOfFields(); ++i)
        {
            const auto& fieldName = metaData.getFieldNameAt(i);
            const auto& fieldDataType = metaData.getFieldDataTypeAt(i);
            if (not includesField(projections, fieldName))
            {
                continue;
            }
            const auto startAddress = bandPtr + (tupleBase + i);
            const auto endAddress = bandPtr + (tupleBase + i + nautilus::static_val<uint64_t>(1));
            const auto rawStart = readValueFromMemRef<FieldIndex>(startAddress); /// delimiter before field i
            const auto rawEnd = readValueFromMemRef<FieldIndex>(endAddress); /// delimiter after field i
            const auto fieldAddress = recordBufferPtr + (rawStart + nautilus::val<FieldIndex>(1));
            const auto fieldSize = rawEnd - rawStart - nautilus::val<FieldIndex>(1);
            parseRawValueIntoRecord(
                fieldDataType,
                record,
                fieldAddress,
                fieldSize,
                fieldName,
                metaData.getNullValues(),
                metaData.getQuotationType(),
                parserTypes.at(fieldDataType.type),
                lazyOverloads.at(fieldDataType.type));
        }
        return record;
    }

public:
    FieldBand() = default;

    ~FieldBand() { delete[] band; }

    /// Move-only and owns a raw band buffer. A raw owning pointer (rather than std::unique_ptr) keeps
    /// FieldBand standard-layout, which IndexPhaseResult requires for its offsetof use -- std::unique_ptr
    /// is not standard-layout under libstdc++. IndexPhaseResult is reset via move-assignment.
    FieldBand(FieldBand&& other) noexcept { *this = std::move(other); }

    FieldBand& operator=(FieldBand&& other) noexcept
    {
        if (this != &other)
        {
            delete[] band;
            band = other.band;
            other.band = nullptr;
            bandCapacity = other.bandCapacity;
            other.bandCapacity = 0;
            numberOfFieldsInSchema = other.numberOfFieldsInSchema;
            totalNumberOfTuples = other.totalNumberOfTuples;
            firstTupleBandIndex = other.firstTupleBandIndex;
            offsetOfFirstTuple = other.offsetOfFirstTuple;
            offsetOfLastTuple = other.offsetOfLastTuple;
        }
        return *this;
    }

    FieldBand(const FieldBand&) = delete;
    FieldBand& operator=(const FieldBand&) = delete;

    /// --- write-side contract (called by SIMDCSVBand indexer via SimdCsv::indexBandInto) ---

    void startSetup(size_t numberOfFieldsInSchema, size_t /*sizeOfFieldDelimiter*/)
    {
        this->numberOfFieldsInSchema = numberOfFieldsInSchema;
        this->totalNumberOfTuples = 0;
        this->firstTupleBandIndex = 0;
    }

    /// Ensures capacity for `maxPositions` structural offsets plus flattenBits' 16-slot branchless slack,
    /// and returns the writable band base. The true count is recorded later by finalize()/markNoTupleDelimiters().
    /// TMP DIAGNOSTIC (benchmark experiment): `make_unique_for_overwrite` does NOT zero-initialize, unlike the
    /// former `std::vector::resize`. The band is sized to a worst-case `numBytes` (every byte a delimiter) but
    /// well-formed CSV uses ~1/8 of it; `flattenBits` overwrites exactly the entries the reader bounds to
    /// `count`, so the unused tail never needs to exist. Zeroing it faulted+memset the whole worst-case span
    /// (perf showed ~9.7% self-time in memset, mostly page-faults); not zeroing faults only the written pages.
    FieldIndex* prepareBand(size_t maxPositions)
    {
        constexpr size_t flattenSlack = 16;
        if (bandCapacity < maxPositions + flattenSlack)
        {
            delete[] band;
            band = new FieldIndex[maxPositions + flattenSlack];
            bandCapacity = maxPositions + flattenSlack;
        }
        return band;
    }

    void markNoTupleDelimiters()
    {
        this->offsetOfFirstTuple = std::numeric_limits<FieldIndex>::max();
        this->offsetOfLastTuple = std::numeric_limits<FieldIndex>::max();
        this->totalNumberOfTuples = 0;
    }

    /// `count` raw positions were written into prepareBand()'s buffer; firstNewline/lastNewline are the byte
    /// offsets of the first and last tuple delimiters. Derive the anchor index b0 and the complete-tuple count.
    void finalize(size_t count, FieldIndex firstNewline, FieldIndex lastNewline)
    {
        const auto begin = band;
        const auto end = band + static_cast<std::ptrdiff_t>(count);
        const auto b0 = static_cast<size_t>(std::lower_bound(begin, end, firstNewline) - begin);
        const auto bLast = static_cast<size_t>(std::lower_bound(begin, end, lastNewline) - begin);
        this->firstTupleBandIndex = b0;
        this->offsetOfFirstTuple = firstNewline;
        this->offsetOfLastTuple = lastNewline;
        this->totalNumberOfTuples = (numberOfFieldsInSchema == 0) ? 0 : ((bLast - b0) / numberOfFieldsInSchema);
    }

    /// SEQUENTIAL (synthetic-anchor) finalize. Unlike the parallel finalize, the aligned leading tuple
    /// [0 -> firstNewline] IS a complete tuple (tuple 0), because the blocking source runner prepends the
    /// prior buffer's truncated bytes so byte 0 always starts a tuple. `indexBandSequentialInto` writes a
    /// synthetic anchor band[0] = FieldIndex max (-1); the real structural positions live at band[1 ..
    /// 1+count] in document order. So b0 is fixed at 0 and tuple 0 field 0 reads byte 0 via uint32
    /// wraparound (start+1 -> 0). The last complete tuple ends at lastNewline, which sits at band index
    /// T*N, so T = bandIndexOf(lastNewline) / N. offsetOfFirstTuple stays the first newline byte offset:
    /// the sequential glue uses it to split leading spanning bytes / detect hasTupleDelimiter.
    void finalizeSequential(size_t count, FieldIndex firstNewline, FieldIndex lastNewline)
    {
        this->firstTupleBandIndex = 0;
        this->offsetOfFirstTuple = firstNewline;
        this->offsetOfLastTuple = lastNewline;
        const auto begin = band + 1; /// skip the synthetic anchor at band[0]
        const auto end = band + 1 + static_cast<std::ptrdiff_t>(count);
        const auto bLast = static_cast<size_t>(std::lower_bound(begin, end, lastNewline) - band);
        this->totalNumberOfTuples = (numberOfFieldsInSchema == 0) ? 0 : (bLast / numberOfFieldsInSchema);
    }

    /// Sequential spanning: when the leading bytes [0 -> firstNewline] are the tail of a spanning tuple
    /// (parsed separately as the leading record via parseLeadingRecord), drop band tuple 0 from the
    /// raw-buffer read so it is not parsed twice. Advancing the anchor by N lands b0 on the first real
    /// newline (parallel semantics for the remaining complete tuples).
    void skipFirstTuple()
    {
        if (totalNumberOfTuples > 0)
        {
            firstTupleBandIndex += numberOfFieldsInSchema;
            totalNumberOfTuples -= 1;
        }
    }

private:
    size_t numberOfFieldsInSchema{};
    size_t totalNumberOfTuples{};
    size_t firstTupleBandIndex{}; /// b0: band index of the first newline (anchor of the first complete tuple)
    FieldIndex offsetOfFirstTuple{};
    FieldIndex offsetOfLastTuple{};
    FieldIndex* band{nullptr}; /// non-zeroing growable owning storage (see prepareBand); reader bounds to `count`
    size_t bandCapacity{0};
};

}
