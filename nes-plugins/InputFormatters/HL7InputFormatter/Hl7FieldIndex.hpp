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
#include <type_traits>
#include <unordered_map>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/inline.hpp>
#include <ErrorHandling.hpp>
#include <FieldIndexFunction.hpp>
#include <FieldOffsets.hpp> /// for the free `includesField` helper (shared with FieldOffsets)
#include <RawTupleBuffer.hpp>
#include <RawValueParser.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// A FieldIndexFunction for SIMDHL7 that reads the flattened structural-event band ON THE FLY at read
/// time (the SchemaJSONFieldIndex pattern) -- eliminating the per-message group framing that the
/// FieldOffsets<ONE>-based indexer materialized (a `pending` vector of leaf starts, re-emitted per
/// message through emplaceFieldOffset: every offset written twice through branchy vector appends).
///
/// Under arity validation every complete message of F leaves contributes exactly F band events (F-1
/// structural delimiters + the terminating message-delimiter pair), so field addressing is a direct
/// fixed-stride index into the band produced by SimdHl7::flattenHl7SimdInto: with I0 = the band index
/// of the pair OPENING the first complete message, record r's field i spans
///   [ band[I0 + r*F + i] + (i == 0 ? messageDelimiterSize : 1),  band[I0 + r*F + i + 1] )
/// -- the first leaf starts after the two message-delimiter bytes, every later leaf after its 1-byte
/// structural delimiter, and each leaf ends AT the next event position (record r's last-leaf end is
/// the pair that opens record r+1, which exists for every COMPLETE message). Like FieldOffsets, only
/// PROJECTED fields are touched.
///
/// Ownership: the band lives in a thread_local slot pool (SIMDHL7InputFormatIndexer.cpp), keyed by
/// this FIF's (stable) address -- the three per-thread FIF roles (rawBufferFIF / leading / trailing
/// spanning) each get their OWN persistent buffer, because all three bands are live during a single
/// readBuffer pass. This FIF caches only a NON-owning pointer into that slot, which keeps it
/// standard-layout (IndexPhaseResult requires standard layout for offsetof).
class Hl7FieldIndex final : public FieldIndexFunction<Hl7FieldIndex>
{
    friend FieldIndexFunction<Hl7FieldIndex>;

    /// FieldIndexFunction (CRTP) interface functions
    [[nodiscard]] FieldIndex applyGetByteOffsetOfFirstTuple() const { return this->offsetOfFirstTuple; }

    [[nodiscard]] FieldIndex applyGetByteOffsetOfLastTuple() const { return this->offsetOfLastTuple; }

    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }

    /// Returns the (non-owning) base of the flattened event band for this FIF's current buffer.
    /// NAUTILUS_INLINE: nes-input-formatters is pass-applied, so the marker folds the proxy call into the read.
    NAUTILUS_INLINE static const FieldIndex* getBandProxy(const Hl7FieldIndex* const fieldIndex) { return fieldIndex->band; }

    [[nodiscard]] nautilus::val<bool>
    applyHasNext(const nautilus::val<uint64_t>& recordIdx, nautilus::val<Hl7FieldIndex*> fieldIndexPtr) const
    {
        nautilus::val<uint64_t> total = *getMemberWithOffset<size_t>(fieldIndexPtr, offsetof(Hl7FieldIndex, totalNumberOfTuples));
        return recordIdx < total;
    }

    template <typename IndexerMetaData>
    [[nodiscard]] Record applyReadSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& recordBufferPtr,
        const nautilus::val<uint64_t>& recordIndex,
        const IndexerMetaData& metaData,
        nautilus::val<Hl7FieldIndex*> fieldIndexPtr,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const std::unordered_map<DataType::Type, bool>& lazyOverloads) const
    {
        /// static loop over the (fixed) schema fields; skips non-projected fields and only traces the invoke
        /// functions for the fields we actually need (projection push-down, like FieldOffsets).
        Record record;
        const auto bandPtr = invoke(getBandProxy, fieldIndexPtr);
        const nautilus::val<uint64_t> firstPairBandIdx
            = *getMemberWithOffset<size_t>(fieldIndexPtr, offsetof(Hl7FieldIndex, firstPairBandIdx));

        /// F is a host constant (folds), so recordBase folds to a mul by constant.
        const auto eventsPerRecord = nautilus::static_val<uint64_t>(metaData.getNumberOfFields());
        const auto recordBase = firstPairBandIdx + (recordIndex * eventsPerRecord);

        for (nautilus::static_val<uint64_t> i = 0; i < metaData.getNumberOfFields(); ++i)
        {
            const auto& fieldName = metaData.getFieldNameAt(i);
            const auto& fieldDataType = metaData.getFieldDataTypeAt(i);
            if (not includesField(projections, fieldName))
            {
                continue;
            }
            const auto eventAddress = bandPtr + (recordBase + i);
            const auto nextEventAddress = bandPtr + (recordBase + i + nautilus::static_val<uint64_t>(1));
            /// The first leaf starts after the message delimiter (SIMDHL7 enforces 2 bytes); every
            /// later leaf after its 1-byte structural delimiter. Host constant per field slot -> folds.
            const auto leafStartAdjustment
                = nautilus::val<FieldIndex>(static_cast<FieldIndex>(i == uint64_t{0} ? metaData.getTupleDelimitingBytes().size() : 1));
            const auto fieldOffsetStart = readValueFromMemRef<FieldIndex>(eventAddress) + leafStartAdjustment;
            const auto fieldOffsetEnd = readValueFromMemRef<FieldIndex>(nextEventAddress);

            const auto fieldSize = fieldOffsetEnd - fieldOffsetStart;
            const auto fieldAddress = recordBufferPtr + fieldOffsetStart;
            parseRawValueIntoRecord(
                fieldDataType,
                record,
                fieldAddress,
                fieldSize,
                fieldName,
                metaData.getNullValues(),
                metaData.getQuotationType(),
                parserTypes.at(fieldDataType.type),
                lazyOverloads.at(fieldDataType.type),
                varSizedCharacteristicsOf(metaData));
        }
        return record;
    }

public:
    Hl7FieldIndex() = default;
    ~Hl7FieldIndex() = default;

    /// --- write-side contract (called by SIMDHL7InputFormatIndexer::indexRawBuffer) ---

    void startSetup(const uint32_t numberOfFieldsInSchema)
    {
        this->numFields = numberOfFieldsInSchema;
        this->totalNumberOfTuples = 0;
        this->band = nullptr;
        this->firstPairBandIdx = 0;
    }

    void markNoTupleDelimiters()
    {
        this->offsetOfFirstTuple = std::numeric_limits<FieldIndex>::max();
        this->offsetOfLastTuple = std::numeric_limits<FieldIndex>::max();
        this->totalNumberOfTuples = 0;
    }

    void markWithTupleDelimiters(const FieldIndex offsetToFirstTuple, const FieldIndex offsetToLastTuple)
    {
        this->offsetOfFirstTuple = offsetToFirstTuple;
        this->offsetOfLastTuple = offsetToLastTuple;
    }

    /// Called after the flatten: caches the NON-owning band base (into the thread_local slot pool), the
    /// band index of the first complete message's opening pair, and the complete-record count.
    void setFlattenResult(const FieldIndex* const bandPtr, const size_t firstPairIndex, const size_t numberOfRecords)
    {
        this->band = bandPtr;
        this->firstPairBandIdx = firstPairIndex;
        this->totalNumberOfTuples = numberOfRecords;
    }

private:
    const FieldIndex* band{nullptr}; /// non-owning; into the thread_local slot pool (valid for this buffer's read phase)
    size_t firstPairBandIdx{}; /// I0: band index of the pair opening the first complete message
    uint32_t numFields{};
    size_t totalNumberOfTuples{};
    FieldIndex offsetOfFirstTuple{};
    FieldIndex offsetOfLastTuple{};
};

static_assert(std::is_standard_layout_v<Hl7FieldIndex>, "Hl7FieldIndex must be standard-layout for IndexPhaseResult's offsetof use");

}
