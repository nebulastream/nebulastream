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
#include <string>
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

/// PROTOTYPE (option 2): a FieldIndexFunction for SchemaJSON that reads simdjson's stage-1 structural
/// indexes ON THE FLY at read time -- eliminating the second "gather" pass that the FieldOffsets-based
/// SchemaJSON did (which copied every value's (start,end) from simdjson's structural_indexes band into a
/// separate FieldOffsets band). Under the fixed flat schema, a record of F fields emits S = 4F+1 structural
/// indexes; value f lives at local slot 3 + 4f and its byte range ends at the next structural entry (the ','
/// or '}'). So field addressing is a direct fixed-stride index into simdjson's own structural_indexes array
/// -- no band #2, no gather loop, and (like FieldOffsets) only PROJECTED fields are touched.
///
/// Ownership: the simdjson dom_parser_implementation that holds the structural_indexes lives in a
/// thread_local pool (SchemaJSONStage1.cpp), keyed by this FIF's (stable) address. That buffer therefore
/// persists + grows across buffers (steady state = zero allocation, mirroring the baseline's persistent
/// tlStage1), while each of the three per-thread FIF roles (rawBufferFIF / leading / trailing spanning) gets
/// its OWN buffer -- they must not clobber, because all three structural-index sets are live during a single
/// readBuffer pass. This FIF caches only a NON-owning pointer into that buffer (like FieldBand::band), which
/// keeps it standard-layout -- IndexPhaseResult requires standard layout for offsetof, and std::unique_ptr is
/// NOT standard-layout under this stdlib (see FieldBand).
class SchemaJSONFieldIndex final : public FieldIndexFunction<SchemaJSONFieldIndex>
{
    friend FieldIndexFunction<SchemaJSONFieldIndex>;

    /// FieldIndexFunction (CRTP) interface functions
    [[nodiscard]] FieldIndex applyGetByteOffsetOfFirstTuple() const { return this->offsetOfFirstTuple; }

    [[nodiscard]] FieldIndex applyGetByteOffsetOfLastTuple() const { return this->offsetOfLastTuple; }

    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }

    /// Returns the (non-owning) base of simdjson's structural_indexes array for this FIF's current buffer.
    /// NAUTILUS_INLINE: nes-input-formatters is pass-applied, so the marker folds the proxy call into the read.
    NAUTILUS_INLINE static const FieldIndex* getStructuralIndexesProxy(const SchemaJSONFieldIndex* const fieldIndex)
    {
        return fieldIndex->structuralIndexes;
    }

    [[nodiscard]] nautilus::val<bool>
    applyHasNext(const nautilus::val<uint64_t>& recordIdx, nautilus::val<SchemaJSONFieldIndex*> fieldIndexPtr) const
    {
        nautilus::val<uint64_t> total = *getMemberWithOffset<size_t>(fieldIndexPtr, offsetof(SchemaJSONFieldIndex, totalNumberOfTuples));
        return recordIdx < total;
    }

    template <typename IndexerMetaData>
    [[nodiscard]] Record applyReadSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& recordBufferPtr,
        const nautilus::val<uint64_t>& recordIndex,
        const IndexerMetaData& metaData,
        nautilus::val<SchemaJSONFieldIndex*> fieldIndexPtr,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const std::unordered_map<DataType::Type, bool>& lazyOverloads) const
    {
        /// static loop over the (fixed) schema fields; skips non-projected fields and only traces the invoke
        /// functions for the fields we actually need (projection push-down, like FieldOffsets).
        Record record;
        const auto structuralIndexesPtr = invoke(getStructuralIndexesProxy, fieldIndexPtr);
        const nautilus::val<FieldIndex> base = *getMemberWithOffset<FieldIndex>(fieldIndexPtr, offsetof(SchemaJSONFieldIndex, base));

        /// S = 4F+1 is a host constant (folds), so recordBase folds to a shift/mul by constant.
        const auto structuralIndexesPerRecord = nautilus::static_val<uint64_t>((4 * metaData.getNumberOfFields()) + 1);
        const auto recordBase = recordIndex * structuralIndexesPerRecord;

        for (nautilus::static_val<uint64_t> i = 0; i < metaData.getNumberOfFields(); ++i)
        {
            const auto& fieldName = metaData.getFieldNameAt(i);
            const auto& fieldDataType = metaData.getFieldDataTypeAt(i);
            if (not includesField(projections, fieldName))
            {
                continue;
            }
            /// local layout within a record's S-index block: value of field f is at slot 3 + 4f, and the next
            /// structural entry (slot 4 + 4f, a ',' or the closing '}') terminates the value.
            nautilus::static_val<uint64_t> fieldSlotBase = i * 4;
            nautilus::static_val<uint64_t> valueSlot = fieldSlotBase + 3;
            const auto valueStartAddress = structuralIndexesPtr + (recordBase + valueSlot);
            const auto valueEndAddress = structuralIndexesPtr + (recordBase + valueSlot + nautilus::static_val<uint64_t>(1));
            const auto valueStart = readValueFromMemRef<FieldIndex>(valueStartAddress);
            const auto valueEnd = readValueFromMemRef<FieldIndex>(valueEndAddress);

            /// The structural offsets are RELATIVE to the indexed region (which starts at `base` within the raw
            /// buffer); add `base` to reach the absolute byte. Right-trim JSON whitespace that sits between the
            /// value and its terminator (pretty-printed JSON, e.g. `-5 ,` or `"a,b:c" }`); packed NDJSON does 0
            /// decrements. This mirrors the original gather's trim, now performed at READ time on the record
            /// bytes -- it is REQUIRED for correctness (bool literals and quoted strings would otherwise carry a
            /// trailing space past the closing quote; see SchemaJSONValues.test). Cheap: the last value byte is
            /// read once and the decrement loop is entered only if it is actually whitespace. The read is always
            /// in bounds -- the body runs only while valueEndTrimmed > valueStart, so base + valueEndTrimmed - 1
            /// is inside the value.
            nautilus::val<FieldIndex> valueEndTrimmed = valueEnd;
            nautilus::val<bool> keepTrimming = valueEndTrimmed > valueStart;
            while (keepTrimming)
            {
                const nautilus::val<int8_t> prevByte = *(recordBufferPtr + (base + valueEndTrimmed - nautilus::val<FieldIndex>(1)));
                const nautilus::val<bool> isWhitespace = (prevByte == nautilus::val<int8_t>(' '))
                    || (prevByte == nautilus::val<int8_t>('\t')) || (prevByte == nautilus::val<int8_t>('\n'))
                    || (prevByte == nautilus::val<int8_t>('\r'));
                if (isWhitespace)
                {
                    valueEndTrimmed = valueEndTrimmed - nautilus::val<FieldIndex>(1);
                    keepTrimming = valueEndTrimmed > valueStart;
                }
                else
                {
                    keepTrimming = nautilus::val<bool>(false);
                }
            }
            const auto fieldAddress = recordBufferPtr + (base + valueStart);
            const auto fieldSize = valueEndTrimmed - valueStart;
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
    SchemaJSONFieldIndex() = default;
    ~SchemaJSONFieldIndex() = default;

    /// --- write-side contract (called by SchemaJSONInputFormatIndexer::indexRawBuffer + schemaJsonIndexIntoFieldIndex) ---

    void startSetup(const uint32_t numberOfFieldsInSchema)
    {
        this->numFields = numberOfFieldsInSchema;
        this->structuralIndexesPerRecord = (4 * numberOfFieldsInSchema) + 1;
        this->totalNumberOfTuples = 0;
        this->structuralIndexes = nullptr;
        this->base = 0;
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

    /// Called after stage-1: caches the NON-owning structural-index base (into the thread_local pool), the
    /// region base offset, and the record count for this buffer.
    void setStage1Result(const FieldIndex* const structuralIndexesPtr, const FieldIndex regionBase, const size_t numberOfRecords)
    {
        this->structuralIndexes = structuralIndexesPtr;
        this->base = regionBase;
        this->totalNumberOfTuples = numberOfRecords;
    }

private:
    const FieldIndex* structuralIndexes{nullptr}; /// non-owning; into the thread_local pool (valid for this buffer's read phase)
    FieldIndex base{}; /// absolute offset of the indexed region within recordBufferPtr
    uint32_t numFields{};
    uint32_t structuralIndexesPerRecord{}; /// 4F+1
    size_t totalNumberOfTuples{};
    FieldIndex offsetOfFirstTuple{};
    FieldIndex offsetOfLastTuple{};
};

static_assert(
    std::is_standard_layout_v<SchemaJSONFieldIndex>, "SchemaJSONFieldIndex must be standard-layout for IndexPhaseResult's offsetof use");

}
