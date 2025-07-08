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
#include <functional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <FieldIndexFunction.hpp>
#include <InputFormatterTask.hpp>
#include "Nautilus/Interface/Record.hpp"


namespace NES::InputFormatters
{

enum class NumRequiredOffsetsPerField : uint8_t
{
    ONE,
    TWO,
};

struct FieldOffsetTypePair
{
    FieldOffsetsType startOfField{};
    FieldOffsetsType endOfField{};
};

/// Determines the in-tuple-buffer representation of the offsets for type-safety reasons
template <NumRequiredOffsetsPerField N>
struct FieldOffsetTypeSelector;
template <>
struct FieldOffsetTypeSelector<NumRequiredOffsetsPerField::ONE>
{
    using type = FieldOffsetsType;
};
template <>
struct FieldOffsetTypeSelector<NumRequiredOffsetsPerField::TWO>
{
    using type = FieldOffsetTypePair;
};

template <NumRequiredOffsetsPerField NumOffsetsPerField>
class FieldOffsets final : public FieldIndexFunction<FieldOffsets<NumOffsetsPerField>>
{
    friend FieldIndexFunction<FieldOffsets>;

    /// FieldIndexFunction (CRTP) interface functions
    [[nodiscard]] FieldOffsetsType applyGetOffsetOfFirstTupleDelimiter() const { return this->offsetOfFirstTuple; }
    [[nodiscard]] FieldOffsetsType applyGetOffsetOfLastTupleDelimiter() const { return this->offsetOfLastTuple; }
    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }

    const typename FieldOffsetTypeSelector<NumOffsetsPerField>::type* getBufferByWithRecordIndex() const { return indexValues.data(); }
    static const typename FieldOffsetTypeSelector<NumOffsetsPerField>::type*
    getTupleBufferForEntryProxy(const FieldOffsets* const fieldOffsets)
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
        const RawValueParser::QuotationType quotationType,
        nautilus::val<FieldOffsets*> fieldOffsetsPtr) const
    {
        /// static loop over number of fields (which don't change)
        /// skips fields that are not part of projection and only traces invoke functions for fields that we need
        Nautilus::Record record;
        const auto indexBufferPtr = nautilus::invoke(getTupleBufferForEntryProxy, fieldOffsetsPtr);
        for (nautilus::static_val<uint64_t> i = 0; i < metaData.getSchema().getNumberOfFields(); ++i)
        {
            if (const auto& fieldName = metaData.getSchema().getFieldAt(i).name; not includesField(projections, fieldName))
            {
                continue;
            }

            switch (NumOffsetsPerField)
            {
                case NumRequiredOffsetsPerField::ONE: {
                    // Todo: could access member: 'numberOfOffsetsPerTuple'
                    const auto numPriorFields = recordIndex * nautilus::static_val(metaData.getSchema().getNumberOfFields() + 1);
                    // const auto byteOffsetStart = numPriorFields * nautilus::static_val(sizeof(FieldOffsetsType));
                    const auto recordOffsetAddress = indexBufferPtr + (numPriorFields + i);
                    const auto recordOffsetEndAddress = indexBufferPtr + (numPriorFields + i + nautilus::static_val<uint64_t>(1));
                    const auto fieldOffsetStart = Nautilus::Util::readValueFromMemRef<FieldOffsetsType>(recordOffsetAddress);
                    const auto fieldOffsetEnd = Nautilus::Util::readValueFromMemRef<FieldOffsetsType>(recordOffsetEndAddress);

                    auto fieldSize = fieldOffsetEnd - fieldOffsetStart;
                    // Todo: find better way to only deduct size of field delimiter if field is last field
                    if (i < metaData.getSchema().getNumberOfFields() - 1)
                    {
                        fieldSize -= nautilus::static_val<uint64_t>(
                            metaData.getTupleDelimitingBytes().size()); //Todo: should be 'getFieldDelimitingBytes'
                    }
                    const auto fieldAddress = recordBufferPtr + fieldOffsetStart;
                    const auto& currentField = metaData.getSchema().getFieldAt(i);
                    RawValueParser::parseRawValueIntoRecord(
                        currentField.dataType.type, record, fieldAddress, fieldSize, currentField.name, quotationType);
                    break;
                }
                case NumRequiredOffsetsPerField::TWO: {
                    const auto numPriorFields = recordIndex * nautilus::static_val(metaData.getSchema().getNumberOfFields());
                    INVARIANT(
                        sizeof(typename FieldOffsetTypeSelector<NumOffsetsPerField>::type) == (2 * (sizeof(FieldOffsetsType))),
                        "Violated memory layout assumption");
                    const auto recordOffsetAddress = indexBufferPtr + (numPriorFields + i);
                    const auto recordOffsetEndAddress = indexBufferPtr + (numPriorFields + i + nautilus::static_val<uint64_t>(1));
                    const auto fieldOffsetStart = Nautilus::Util::readValueFromMemRef<FieldOffsetsType>(recordOffsetAddress);
                    const auto fieldOffsetEnd = Nautilus::Util::readValueFromMemRef<FieldOffsetsType>(recordOffsetEndAddress);

                    auto fieldSize = fieldOffsetEnd - fieldOffsetStart;
                    // Todo: find better way to only deduct size of field delimiter if field is last field
                    if (i < metaData.getSchema().getNumberOfFields() - 1)
                    {
                        fieldSize -= nautilus::static_val<uint64_t>(
                            metaData.getTupleDelimitingBytes().size()); //Todo: should be 'getFieldDelimitingBytes'
                    }
                    const auto fieldAddress = recordBufferPtr + fieldOffsetStart;
                    const auto& currentField = metaData.getSchema().getFieldAt(i);
                    RawValueParser::parseRawValueIntoRecord(
                        currentField.dataType.type, record, fieldAddress, fieldSize, currentField.name, quotationType);
                    break;
                }
            }
        }
        return record;
    }

public:
    FieldOffsets() = default;
    ~FieldOffsets() = default;

    /// InputFormatter interface functions:
    void startSetup(const size_t numberOfFieldsInSchema, const size_t sizeOfFieldDelimiter)
    {
        PRECONDITION(
            sizeOfFieldDelimiter <= std::numeric_limits<FieldOffsetsType>::max(),
            "Size of tuple delimiter must be smaller than: {}",
            std::numeric_limits<FieldOffsetsType>::max());
        this->sizeOfFieldDelimiter = static_cast<FieldOffsetsType>(sizeOfFieldDelimiter);
        this->numberOfFieldsInSchema = numberOfFieldsInSchema;
        if constexpr (NumOffsetsPerField == NumRequiredOffsetsPerField::ONE)
        {
            this->numberOfOffsetsPerTuple = this->numberOfFieldsInSchema + 1;
        }
        else
        {
            this->numberOfOffsetsPerTuple = this->numberOfFieldsInSchema;
        }
        this->totalNumberOfTuples = 0;
    }

    void writeOffsetAt(const FieldOffsetsType offset, const FieldOffsetsType)
    requires(NumOffsetsPerField == NumRequiredOffsetsPerField::ONE)
    {
        // Todo: by hardcoding emplace_back, we loose the ability to place an offset at a specific index
        // .. for out of order JSON keys, it would be nice to know that at least space for one specific tuple is available
        // -> could reintroduce the 'writeNextTuple'
        this->indexValues.emplace_back(offset);
    }
    void writeOffsetAt(const FieldOffsetTypePair& offset, const FieldOffsetsType)
    requires(NumOffsetsPerField == NumRequiredOffsetsPerField::TWO)
    {
        // Todo: read above
        this->indexValues.emplace_back(offset);
    }

    /// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
    void markNoTupleDelimiters()
    {
        this->offsetOfFirstTuple = std::numeric_limits<FieldOffsetsType>::max();
        this->offsetOfLastTuple = std::numeric_limits<FieldOffsetsType>::max();
    }

    /// We need to specify tuple offsets, to resolve spanning tuples (stitching together the raw bytes of the spanning tuples)
    /// We need to specify the offsets manually, because the field indexes may not correspond to the tuple offsets, e.g., in a JSON tuple,
    /// the index of the first field, most likely is not the start of the tuple.
    void markWithTupleDelimiters(const FieldOffsetsType offsetToFirstTuple, const FieldOffsetsType offsetToLastTuple)
    {
        /// Make sure that the number of read fields matches the expected value.
        const auto finalIndex = indexValues.size();
        const auto [totalNumberOfTuples, remainder] = std::lldiv(finalIndex, numberOfOffsetsPerTuple);
        if (remainder != 0)
        {
            throw FormattingError(
                "Number of indexes {} must be a multiple of number of fields in tuple {}", finalIndex, (numberOfOffsetsPerTuple));
        }
        /// Finalize the state of the FieldOffsets object
        this->totalNumberOfTuples = totalNumberOfTuples;
        this->offsetOfFirstTuple = offsetToFirstTuple;
        this->offsetOfLastTuple = offsetToLastTuple;
    }

private:
    FieldOffsetsType sizeOfFieldDelimiter{};
    size_t numberOfFieldsInSchema{};
    size_t numberOfOffsetsPerTuple{};
    size_t totalNumberOfTuples{};
    FieldOffsetsType offsetOfFirstTuple{};
    FieldOffsetsType offsetOfLastTuple{};
    std::vector<typename FieldOffsetTypeSelector<NumOffsetsPerField>::type> indexValues;
};
}
