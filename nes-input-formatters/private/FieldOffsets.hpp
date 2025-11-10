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
#include <cstdlib>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <Concepts.hpp>
#include <ErrorHandling.hpp>
#include <FieldIndexFunction.hpp>
#include <InputFormatter.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

inline bool
includesField(const std::vector<NES::Record::RecordFieldIdentifier>& projections, const NES::Record::RecordFieldIdentifier& fieldIndex)
{
    return std::ranges::find(projections, fieldIndex) != projections.end();
}

namespace NES
{

enum class NumRequiredOffsetsPerField : uint8_t
{
    ONE,
    TWO,
};

template <NumRequiredOffsetsPerField NumOffsetsPerField>
class FieldOffsets final : public FieldIndexFunction<FieldOffsets<NumOffsetsPerField>>
{
    friend FieldIndexFunction<FieldOffsets>;

    /// FieldIndexFunction (CRTP) interface functions
    [[nodiscard]] FieldIndex applyGetByteOffsetOfFirstTuple() const { return this->offsetOfFirstTuple; }

    [[nodiscard]] FieldIndex applyGetByteOffsetOfLastTuple() const { return this->offsetOfLastTuple; }

    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }

    static const FieldIndex* getTupleBufferForEntryProxy(const FieldOffsets* const fieldOffsets)
    {
        /// At this point, the index values are fixed, thus the vector cannot grow anymore and accessing the pointer is safe
        return fieldOffsets->indexValues.data();
    }

    [[nodiscard]] nautilus::val<bool>
    applyHasNext(const nautilus::val<uint64_t>& recordIdx, nautilus::val<FieldOffsets*> fieldOffsetsPtr) const
    {
        nautilus::val<uint64_t> totalNumberOfTuples
            = *getMemberWithOffset<size_t>(fieldOffsetsPtr, offsetof(FieldOffsets, totalNumberOfTuples));
        return recordIdx < totalNumberOfTuples;
    }

    template <typename IndexerMetaData>
    [[nodiscard]] Record applyReadSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& recordBufferPtr,
        const nautilus::val<uint64_t>& recordIndex,
        const IndexerMetaData& metaData,
        nautilus::val<FieldOffsets*> fieldOffsetsPtr,
        ArenaRef& arenaRef) const
    requires(NumOffsetsPerField == NumRequiredOffsetsPerField::ONE)
    {
        /// static loop over number of fields (which don't change)
        /// skips fields that are not part of projection and only traces invoke functions for fields that we need
        Record record;
        const auto indexBufferPtr = invoke(getTupleBufferForEntryProxy, fieldOffsetsPtr);
        for (nautilus::static_val<uint64_t> i = 0; i < metaData.getNumberOfFields(); ++i)
        {
            const auto& fieldName = metaData.getFieldNameAt(i);
            const auto& fieldDataType = metaData.getFieldDataTypeAt(i);
            if (not includesField(projections, fieldName))
            {
                continue;
            }

            const auto numPriorFields = recordIndex * nautilus::static_val(metaData.getNumberOfFields() + 1);
            const auto recordOffsetAddress = indexBufferPtr + (numPriorFields + i);
            const auto recordOffsetEndAddress = indexBufferPtr + (numPriorFields + i + nautilus::static_val<uint64_t>(1));
            const auto fieldOffsetStart = readValueFromMemRef<FieldIndex>(recordOffsetAddress);
            const auto fieldOffsetEnd = readValueFromMemRef<FieldIndex>(recordOffsetEndAddress);

            const auto sizeOfDelimiter = (i + 1 == metaData.getNumberOfFields()) ? 0 : metaData.getFieldDelimitingBytes().size();
            const auto fieldSize = fieldOffsetEnd - fieldOffsetStart - sizeOfDelimiter;
            const auto fieldAddress = recordBufferPtr + fieldOffsetStart;
            parseRawValueIntoRecord(fieldDataType.type, record, fieldAddress, fieldSize, fieldName, metaData.getQuotationType(), arenaRef);
        }
        return record;
    }

    template <typename IndexerMetaData>
    [[nodiscard]] Record applyReadSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier> projections,
        const nautilus::val<int8_t*>& recordBufferPtr,
        const nautilus::val<uint64_t>& recordIndex,
        const IndexerMetaData& metaData,
        const nautilus::val<FieldOffsets*> fieldOffsetsPtr,
        ArenaRef& arenaRef) const
    requires(NumOffsetsPerField == NumRequiredOffsetsPerField::TWO)
    {
        /// static loop over number of fields (which don't change)
        /// skips fields that are not part of projection and only traces invoke functions for fields that we need
        Record record;
        const auto indexBufferPtr = invoke(getTupleBufferForEntryProxy, fieldOffsetsPtr);
        for (nautilus::static_val<uint64_t> i = 0; i < metaData.getNumberOfFields(); ++i)
        {
            const auto& fieldName = metaData.getFieldNameAt(i);
            const auto& fieldDataType = metaData.getFieldDataTypeAt(i);
            if (not includesField(projections, fieldName))
            {
                continue;
            }
            const auto numPriorFields = recordIndex * nautilus::static_val(metaData.getNumberOfFields());
            nautilus::static_val<uint64_t> offsetPairStart = i * 2;
            nautilus::static_val<uint64_t> offsetPairEnd = offsetPairStart + 1;
            const auto recordOffsetAddress = indexBufferPtr + ((numPriorFields * 2) + offsetPairStart);
            const auto recordOffsetEndAddress = indexBufferPtr + ((numPriorFields * 2) + offsetPairEnd);
            const auto fieldOffsetStart = readValueFromMemRef<FieldIndex>(recordOffsetAddress);
            const auto fieldOffsetEnd = readValueFromMemRef<FieldIndex>(recordOffsetEndAddress);

            auto fieldSize = fieldOffsetEnd - fieldOffsetStart;
            const auto fieldAddress = recordBufferPtr + fieldOffsetStart;
            parseRawValueIntoRecord(fieldDataType.type, record, fieldAddress, fieldSize, fieldName, metaData.getQuotationType(), arenaRef);
        }
        return record;
    }

public:
    FieldOffsets() = default;
    ~FieldOffsets() = default;

    /// InputFormatter interface functions:
    void startSetup(size_t numberOfFieldsInSchema, size_t sizeOfFieldDelimiter)
    {
        PRECONDITION(
            sizeOfFieldDelimiter <= std::numeric_limits<FieldIndex>::max(),
            "Size of tuple delimiter must be smaller than: {}",
            std::numeric_limits<FieldIndex>::max());
        this->sizeOfFieldDelimiter = static_cast<FieldIndex>(sizeOfFieldDelimiter);
        this->numberOfFieldsInSchema = numberOfFieldsInSchema;
        this->numberOfOffsetsPerTuple
            = this->numberOfFieldsInSchema + static_cast<size_t>(NumOffsetsPerField == NumRequiredOffsetsPerField::ONE);
        this->totalNumberOfTuples = 0;
    }

    /// Assures that there is space to write one more tuple and returns a pointer to write the field offsets (of one tuple) to.
    /// @Note expects that users of function write 'number of fields in schema + 1' offsets to pointer, manually incrementing the pointer by one for each offset.
    void emplaceFieldOffset(FieldIndex offset)
    requires(NumOffsetsPerField == NumRequiredOffsetsPerField::ONE)
    {
        this->indexValues.emplace_back(offset);
    }

    /// Ensures (vs std::pair) that values lie consecutively in memory
    struct __attribute__((packed)) IndexPairs
    {
        FieldIndex fieldValueStart;
        FieldIndex fieldValueEnd;
    };

    std::span<IndexPairs> emplaceTupleOffsets(const size_t numberOfFieldsInSchema)
    requires(NumOffsetsPerField == NumRequiredOffsetsPerField::TWO)
    {
        const size_t numberOfRequiredOffsets = numberOfFieldsInSchema * 2;
        const size_t startIdx = indexValues.size();
        indexValues.resize(startIdx + numberOfRequiredOffsets);
        auto fieldIndexPairSpan = std::span(indexValues).subspan(startIdx, numberOfRequiredOffsets);
        return std::span(std::bit_cast<IndexPairs*>(fieldIndexPairSpan.data()), fieldIndexPairSpan.size());
    }

    /// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
    void markNoTupleDelimiters()
    {
        this->offsetOfFirstTuple = std::numeric_limits<FieldIndex>::max();
        this->offsetOfLastTuple = std::numeric_limits<FieldIndex>::max();
    }

    [[nodiscard]] size_t getNumberOfTuples() const
    requires(NumOffsetsPerField == NumRequiredOffsetsPerField::ONE)
    {
        return indexValues.size();
    }

    [[nodiscard]] size_t getNumberOfTuples() const
    requires(NumOffsetsPerField == NumRequiredOffsetsPerField::TWO)
    {
        return indexValues.size() / 2;
    }

    void markWithTupleDelimiters(FieldIndex offsetToFirstTuple, FieldIndex offsetToLastTuple)
    {
        /// Make sure that the number of read fields matches the expected value.
        const auto [totalNumberOfTuples, remainder] = std::lldiv(getNumberOfTuples(), numberOfOffsetsPerTuple);
        INVARIANT(
            remainder == 0, "Number of indexes {} must be a multiple of number of fields in tuple {}", remainder, numberOfOffsetsPerTuple);
        /// Finalize the state of the FieldOffsets object
        this->totalNumberOfTuples = totalNumberOfTuples;
        this->offsetOfFirstTuple = offsetToFirstTuple;
        this->offsetOfLastTuple = offsetToLastTuple;
    }

private:
    FieldIndex sizeOfFieldDelimiter{};
    size_t numberOfFieldsInSchema{};
    size_t numberOfOffsetsPerTuple{};
    size_t totalNumberOfTuples{};
    FieldIndex offsetOfFirstTuple{};
    FieldIndex offsetOfLastTuple{};
    std::vector<FieldIndex> indexValues;
};

}
