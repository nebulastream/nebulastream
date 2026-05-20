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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>
#include <function.hpp>
#include <static.hpp>

#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatter.hpp>
#include <InputParserUtil.hpp>
#include <RawBufferIndex.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

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
    PRECONDITION(
        dynamic_cast<const FieldOffsetRawBufferIndex*>(rawBufferIndex) != nullptr, "rawBufferIndex must be a FieldOffsetRawBufferIndex");
    /// At this point, the index values are fixed, thus the vector cannot grow anymore and accessing the pointer is safe
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): type verified by PRECONDITION above.
    const auto* const fieldOffsets = static_cast<const FieldOffsetRawBufferIndex*>(rawBufferIndex);
    return fieldOffsets->indexValues.data();
}

Record FieldOffsetRawBufferIndex::readSpanningRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const nautilus::val<int8_t*>& recordBufferPtr,
    const nautilus::val<uint64_t>& recordIndex,
    const InputFormatIndexer& indexer,
    nautilus::val<RawBufferIndex*> rawBufferIndex,
    const TupleBufferRef& bufferRef) const
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

        /// Retrieve input parser for the field and parse the value.
        /// These are the temporary defaults for our CSV format. Later, these arguments will be set by the user in the source definition.
        const InputParserConfig parserConfig{.nullable = fieldDataType.nullable, .quotedText = false, .hasTrailingSpace = false};
        const std::unique_ptr<InputParser> parser = provideInputParser(indexer.getParserType(fieldDataType.type), parserConfig);
        const VarVal parsedVal = parser->parseToVarVal(fieldAddress, fieldSize, indexer.getNullValues());
        record.write(fieldName, parsedVal);
    }
    return record;
}

void FieldOffsetRawBufferIndex::startSetup(const size_t numberOfFieldsInSchema)
{
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
        remainder == 0,
        "Number of indexes {} must be a multiple of number of fields in tuple {} (remainder: {})",
        indexValues.size(),
        numberOfOffsetsPerTuple,
        remainder);
    /// Finalize the state of the FieldOffsetRawBufferIndex object
    this->totalNumberOfTuples = numberOfTuples;
    this->offsetOfFirstTuple = offsetToFirstTuple;
    this->offsetOfLastTuple = offsetToLastTuple;
}
}
