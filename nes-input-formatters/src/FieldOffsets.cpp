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

#include <FieldOffsets.hpp>

#include <cstddef>
#include <limits>
#include <string_view>
#include <utility>

#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterTask.hpp>

namespace NES::InputFormatters
{

void FieldOffsets::startSetup(const size_t numberOfFieldsInSchema, const size_t sizeOfFieldDelimiter)
{
    PRECONDITION(
        sizeOfFieldDelimiter <= std::numeric_limits<FieldOffsetsType>::max(),
        "Size of tuple delimiter must be smaller than: {}",
        std::numeric_limits<FieldOffsetsType>::max());
    this->sizeOfFieldDelimiter = static_cast<FieldOffsetsType>(sizeOfFieldDelimiter);
    this->numberOfFieldsInSchema = numberOfFieldsInSchema;
    this->numberOfIndexesPerTuple = numberOfFieldsInSchema + 1;
    this->totalNumberOfTuples = 0;
}

FieldOffsetsType FieldOffsets::applyGetOffsetOfFirstTupleDelimiter() const
{
    return this->offsetOfFirstTuple;
}

FieldOffsetsType FieldOffsets::applyGetOffsetOfLastTupleDelimiter() const
{
    return this->offsetOfLastTuple;
}

size_t FieldOffsets::applyGetTotalNumberOfTuples() const
{
    return this->totalNumberOfTuples;
}

std::string_view FieldOffsets::applyReadFieldAt(const std::string_view bufferView, const size_t tupleIdx, const size_t fieldIdx) const
{
    /// If this function becomes bottleneck, we could template on whether we support tuple-based indexing or not
    /// Without tuple-based indexing, we can significantly simplify the offset calculation
    PRECONDITION(tupleIdx < this->totalNumberOfTuples, "TupleIdx {} is out of range [0-{}].", tupleIdx, this->totalNumberOfTuples);
    PRECONDITION(fieldIdx < this->numberOfFieldsInSchema, "FieldIdx {} is out of range [0-{}].", fieldIdx, numberOfFieldsInSchema);

    const auto tupleOffset = tupleIdx * this->numberOfIndexesPerTuple;
    const auto fieldStartIdx = this->indexValues[tupleOffset + fieldIdx];
    const auto fieldEndIdx = this->indexValues[tupleOffset + fieldIdx + 1];
    const auto fieldEndIdxAdjusted = fieldEndIdx - (static_cast<FieldOffsetsType>(fieldIdx + 1 < this->numberOfFieldsInSchema) * this->sizeOfFieldDelimiter);
    const auto fieldLength = fieldEndIdxAdjusted - fieldStartIdx;

    return bufferView.substr(fieldStartIdx, fieldLength);
}

void FieldOffsets::writeOffsetAt(const FieldOffsetsType offset)
{
    this->indexValues.emplace_back(offset);
}

/// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
void FieldOffsets::markNoTupleDelimiters()
{
    this->offsetOfFirstTuple = std::numeric_limits<FieldOffsetsType>::max();
    this->offsetOfLastTuple = std::numeric_limits<FieldOffsetsType>::max();
}

/// We need to specify tuple offsets, to resolve spanning tuples (stitching together the raw bytes of the spanning tuples)
/// We need to specify the offsets manually, because the field indexes may not correspond to the tuple offsets, e.g., in a JSON tuple,
/// the index of the first field, most likely is not the start of the tuple.
void FieldOffsets::markWithTupleDelimiters(const FieldOffsetsType offsetToFirstTuple, const FieldOffsetsType offsetToLastTuple)
{
    /// Make sure that the number of read fields matches the expected value.
    const auto finalIndex = indexValues.size();
    const auto [totalNumberOfTuples, remainder] = std::lldiv(finalIndex, numberOfIndexesPerTuple);
    if (remainder != 0)
    {
        throw FormattingError(
            "Number of indexes {} must be a multiple of number of fields in tuple {}", finalIndex, numberOfIndexesPerTuple);
    }
    /// Finalize the state of the FieldOffsets object
    this->totalNumberOfTuples = totalNumberOfTuples;
    this->offsetOfFirstTuple = offsetToFirstTuple;
    this->offsetOfLastTuple = offsetToLastTuple;
}


}
