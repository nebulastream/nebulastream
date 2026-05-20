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

#include <FieldOffsetRawBufferIndex.hpp>

#include <cstdint>
#include <static.hpp>

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatter.hpp>
#include <InputParserUtil.hpp>
#include <RawBufferIndex.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>

namespace NES
{
FieldOffsetRawBufferIndex::FieldOffsetRawBufferIndex()
{
    INVARIANT(
        static_cast<void*>(this) == static_cast<void*>(static_cast<RawBufferIndex*>(this)),
        "RawBufferIndex base subobject must lay out at offset 0 in FieldOffsetRawBufferIndex");
}

[[nodiscard]] nautilus::val<bool>
FieldOffsetRawBufferIndex::hasNext(const nautilus::val<uint64_t>& tupleIdx, const nautilus::val<RawBufferIndex*>& rawBufferIndex) const
{
    nautilus::val<uint64_t> numTuples
        = readValueFromMemRef<size_t>(getMemberRef(rawBufferIndex, &FieldOffsetRawBufferIndex::totalNumberOfTuples));
    return tupleIdx < numTuples;
}

const FieldIndex* FieldOffsetRawBufferIndex::getIndexValuesProxy(const RawBufferIndex* rawBufferIndex)
{
    /// At this point, the index values are fixed, thus the vector cannot grow anymore and accessing the pointer is safe
    const auto* const fieldOffsets = dynamic_cast<const FieldOffsetRawBufferIndex* const>(rawBufferIndex);
    return fieldOffsets->indexValues.data();
}

Record FieldOffsetRawBufferIndex::readSpanningRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const nautilus::val<int8_t*>& recordBufferPtr,
    const nautilus::val<uint64_t>& recordIndex,
    const InputFormatIndexer& indexer,
    nautilus::val<RawBufferIndex*> rawBufferIndex,
    const TupleBufferRef& bufferRef,
    ArenaRef&) const
{
    Record record;
    const auto indexBufferPtr = nautilus::invoke(getIndexValuesProxy, rawBufferIndex);
    const auto numberOfFields = nautilus::static_val{bufferRef.getAllDataTypes().size()};
    for (nautilus::static_val<uint64_t> i = 0; i < numberOfFields; ++i)
    {
        const auto fieldName = bufferRef.getAllFieldNames().at(i);
        const auto fieldDataType = bufferRef.getAllDataTypes().at(i);
        if (not includesField(projections, fieldName))
        {
            continue;
        }

        const auto numPriorFields = recordIndex * nautilus::static_val(numberOfFields + 1);
        const auto fieldOffsetAddress = indexBufferPtr + (numPriorFields + i);
        const auto fieldOffsetEndAddress = indexBufferPtr + (numPriorFields + i + nautilus::static_val<uint64_t>(1));
        const auto fieldOffsetStart = readValueFromMemRef<FieldIndex>(fieldOffsetAddress);
        const auto fieldOffsetEnd = readValueFromMemRef<FieldIndex>(fieldOffsetEndAddress);

        const auto sizeOfDelimiter = (i + 1 == numberOfFields) ? 0 : indexer.getFieldDelimitingBytes().size();
        const auto fieldSize = fieldOffsetEnd - fieldOffsetStart - sizeOfDelimiter;
        const auto fieldAddress = recordBufferPtr + fieldOffsetStart;
        parseRawValueIntoRecord(
            fieldDataType, indexer.getParserType(fieldDataType.type), record, fieldAddress, fieldSize, fieldName, indexer.getNullValues());
    }
    return record;
}

void FieldOffsetRawBufferIndex::startSetup(const size_t numberOfFieldsInSchema, const size_t sizeOfFieldDelimiter)
{
    PRECONDITION(
        sizeOfFieldDelimiter <= std::numeric_limits<FieldIndex>::max(),
        "Size of field delimiter must be smaller than: {}",
        std::numeric_limits<FieldIndex>::max());
    this->sizeOfFieldDelimiter = static_cast<FieldIndex>(sizeOfFieldDelimiter);
    this->numberOfFieldsInSchema = numberOfFieldsInSchema;
    this->numberOfOffsetsPerTuple = this->numberOfFieldsInSchema + 1;
    this->totalNumberOfTuples = 0;
}

void FieldOffsetRawBufferIndex::markNoTupleDelimiters()
{
    this->offsetOfFirstTuple = std::numeric_limits<FieldIndex>::max();
    this->offsetOfLastTuple = std::numeric_limits<FieldIndex>::max();
}

void FieldOffsetRawBufferIndex::markWithTupleDelimiters(const FieldIndex offsetToFirstTuple, const FieldIndex offsetToLastTuple)
{
    /// Make sure that the number of read fields matches the expected value.
    const auto numberOfTuples = indexValues.size() / numberOfOffsetsPerTuple;
    const auto remainder = indexValues.size() % numberOfOffsetsPerTuple;
    INVARIANT(
        remainder == 0, "Number of indexes {} must be a multiple of number of fields in tuple {}", remainder, numberOfOffsetsPerTuple);
    /// Finalize the state of the FieldOffsetRawBufferIndex object
    this->totalNumberOfTuples = numberOfTuples;
    this->offsetOfFirstTuple = offsetToFirstTuple;
    this->offsetOfLastTuple = offsetToLastTuple;
}
}
