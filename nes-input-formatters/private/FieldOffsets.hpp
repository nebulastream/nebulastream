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
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Concepts.hpp>
#include <FieldIndexFunction.hpp>
#include <InputFormatterTask.hpp>

#include "Nautilus/Interface/Record.hpp"

inline bool includesField(
    const std::vector<NES::Nautilus::Record::RecordFieldIdentifier>& projections,
    const NES::Nautilus::Record::RecordFieldIdentifier& fieldIndex)
{
    return std::ranges::find(projections, fieldIndex) != projections.end();
}

namespace NES::InputFormatters
{

class FieldOffsets final : public FieldIndexFunction<FieldOffsets>
{
    friend FieldIndexFunction;
    static constexpr size_t NUMBER_OF_RESERVED_FIELDS = 1; /// layout: [numberOfTuples | fieldOffsets ...]
    static constexpr size_t NUMBER_OF_RESERVED_BYTES = NUMBER_OF_RESERVED_FIELDS * sizeof(FieldIndex);
    static constexpr size_t OFFSET_OF_TOTAL_NUMBER_OF_TUPLES = 0;

    /// FieldIndexFunction (CRTP) interface functions
    [[nodiscard]] FieldIndex applyGetOffsetOfFirstTupleDelimiter() const;
    [[nodiscard]] FieldIndex applyGetOffsetOfLastTupleDelimiter() const;
    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const;
    [[nodiscard]] std::string_view applyReadFieldAt(std::string_view bufferView, size_t tupleIdx, size_t fieldIdx) const;

    const FieldIndex* getBufferByWithRecordIndex() const { return indexValues.data(); }

    static const FieldIndex* getTupleBufferForEntryProxy(const FieldOffsets* const fieldOffsets)
    {
        // Todo: determine which buffer to access first
        return fieldOffsets->getBufferByWithRecordIndex();
    }

    template <typename IndexerMetaData>
    [[nodiscard]] Record applyReadSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& recordBufferPtr,
        const nautilus::val<uint64_t>& recordIndex,
        const IndexerMetaData& metaData,
        nautilus::val<FieldOffsets*> fieldOffsetsPtr,
        const std::vector<Record::RecordFieldIdentifier>& requiredFields) const
    {
        /// static loop over number of fields (which don't change)
        /// skips fields that are not part of projection and only traces invoke functions for fields that we need
        Nautilus::Record record;
        const auto indexBufferPtr = nautilus::invoke(getTupleBufferForEntryProxy, fieldOffsetsPtr);
        for (nautilus::static_val<uint64_t> i = 0; i < metaData.getSchema().getNumberOfFields(); ++i)
        {
            const auto& fieldName = metaData.getSchema().getFieldAt(i).name;
            if (not includesField(projections, fieldName))
            {
                continue;
            }

            // std::cout << "field: " << fieldName << std::endl;
            // for (auto &field : requiredFields)
            // {
            //     std::cout << "required field: " << field << std::endl;
            // }
            if (not includesField(requiredFields, fieldName))
            {
                VarVal const stub = VarVal(nautilus::val<int>(42));
                record.write(fieldName, stub);
                continue;
            }

            // Todo: could access member: 'numberOfOffsetsPerTuple'
            const auto numPriorFields = recordIndex * nautilus::static_val(metaData.getSchema().getNumberOfFields() + 1);
            // const auto byteOffsetStart = numPriorFields * nautilus::static_val(sizeof(FieldOffsetsType));
            const auto recordOffsetAddress = indexBufferPtr + (numPriorFields + i);
            const auto recordOffsetEndAddress = indexBufferPtr + (numPriorFields + i + nautilus::static_val<uint64_t>(1));
            const auto fieldOffsetStart = Nautilus::Util::readValueFromMemRef<FieldIndex>(recordOffsetAddress);
            const auto fieldOffsetEnd = Nautilus::Util::readValueFromMemRef<FieldIndex>(recordOffsetEndAddress);

            auto fieldSize = fieldOffsetEnd - fieldOffsetStart;
            // Todo: find better way to only deduct size of field delimiter if field is last field
            if (i < metaData.getSchema().getNumberOfFields() - 1)
            {
                fieldSize -= nautilus::static_val<uint64_t>(
                    metaData.getTupleDelimitingBytes().size()); //Todo: should be 'getFieldDelimitingBytes'
            }
            const auto fieldAddress = recordBufferPtr + fieldOffsetStart;
            const auto& currentField = metaData.getSchema().getFieldAt(i);
            RawValueParser::parseLazyValueIntoRecord(currentField.dataType.type, record, fieldAddress, fieldSize, currentField.name);
        }
        return record;
    }

public:
    FieldOffsets() = default;
    ~FieldOffsets() = default;

    /// InputFormatter interface functions:
    void startSetup(size_t numberOfFieldsInSchema, size_t sizeOfFieldDelimiter);
    /// Assures that there is space to write one more tuple and returns a pointer to write the field offsets (of one tuple) to.
    /// @Note expects that users of function write 'number of fields in schema + 1' offsets to pointer, manually incrementing the pointer by one for each offset.
    void writeOffsetsOfNextTuple();
    void writeOffsetAt(FieldIndex offset, FieldIndex idx);

    /// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
    void markNoTupleDelimiters();
    void markWithTupleDelimiters(FieldIndex offsetToFirstTuple, FieldIndex offsetToLastTuple);

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
