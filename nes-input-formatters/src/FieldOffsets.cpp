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

#include <cstddef>
#include <limits>
#include <string_view>
#include <utility>

#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatterTask.hpp>

namespace NES::InputFormatters
{

void FieldOffsets::startSetup(const size_t numberOfFieldsInSchema, const size_t sizeOfFieldDelimiter)
{
    PRECONDITION(
        sizeOfFieldDelimiter <= std::numeric_limits<FieldOffsetsType>::max(),
        "Size of tuple delimiter must be smaller than: {}",
        std::numeric_limits<FieldOffsetsType>::max());
    this->currentIndex = NUMBER_OF_RESERVED_FIELDS;
    this->sizeOfFieldDelimiter = static_cast<FieldOffsetsType>(sizeOfFieldDelimiter);
    this->numberOfFieldsInSchema = numberOfFieldsInSchema;
    this->maxNumberOfTuplesInFormattedBuffer
        = (this->bufferProvider.getBufferSize() - NUMBER_OF_RESERVED_BYTES) / ((numberOfFieldsInSchema + 1) * sizeof(FieldOffsetsType));
    PRECONDITION(
        this->maxNumberOfTuplesInFormattedBuffer != 0,
        "The buffer is of size {}, which is not large enough to represent a single tuple.",
        this->bufferProvider.getBufferSize());
    this->maxIndex = NUMBER_OF_RESERVED_FIELDS + ((numberOfFieldsInSchema + 1) * maxNumberOfTuplesInFormattedBuffer);
    this->totalNumberOfTuples = 0;
    this->offsetBuffers.emplace_back(this->bufferProvider.getBufferBlocking());
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

    /// Calculate buffer from which to read field
    const auto tupleIdxForDiv = (tupleIdx == 0) ? 0 : tupleIdx;
    const auto bufferNumber = tupleIdxForDiv / maxNumberOfTuplesInFormattedBuffer;
    PRECONDITION(bufferNumber < this->offsetBuffers.size(), "Buffer {} is out of range [0-{}].", bufferNumber, this->offsetBuffers.size());
    const auto targetBuffer = this->offsetBuffers[bufferNumber];

    const auto numTuplesInPriorBuffers = bufferNumber * maxNumberOfTuplesInFormattedBuffer;
    const auto tuplesAlreadyInCurrentBuffer = tupleIdx - numTuplesInPriorBuffers;
    const auto fieldOffset = (tuplesAlreadyInCurrentBuffer * (numberOfFieldsInSchema + 1)) + fieldIdx;

    const auto startOfField = targetBuffer[NUMBER_OF_RESERVED_FIELDS + fieldOffset];
    /// There are 'numberOfFieldsInSchema + 1' offsets for each tuple, so it is safe to use 'fieldIdx + 1'
    const auto endOfField = targetBuffer[NUMBER_OF_RESERVED_FIELDS + fieldOffset + 1];
    /// The last field does not end in a tuple delimiter, so we can't deduct
    const auto sizeOfField
        = (fieldIdx != numberOfFieldsInSchema - 1) ? endOfField - startOfField - this->sizeOfFieldDelimiter : endOfField - startOfField;
    return bufferView.substr(startOfField, sizeOfField);
}

void FieldOffsets::writeOffsetsOfNextTuple()
{
    currentIndex += numberOfFieldsInSchema + 1;
    if (currentIndex >= maxIndex)
    {
        allocateNewChildBuffer();
        currentIndex = NUMBER_OF_RESERVED_FIELDS;
    }
}

void FieldOffsets::writeOffsetAt(const FieldOffsetsType offset, const FieldOffsetsType idx)
{
    this->offsetBuffers.back()[currentIndex + idx] = offset;
}

template <bool ContainsOffsets>
void FieldOffsets::finishSetup(const FieldOffsetsType offsetToFirstTuple, const FieldOffsetsType offsetToLastTuple)
{
    this->offsetOfFirstTuple = offsetToFirstTuple;
    this->offsetOfLastTuple = offsetToLastTuple;

    if constexpr (ContainsOffsets)
    {
        /// Make sure that the number of read fields matches the expected value.
        if ((currentIndex - NUMBER_OF_RESERVED_FIELDS) % (numberOfFieldsInSchema + 1) != 0)
        {
            throw FormattingError(
                "Number of indexes {} must be a multiple of number of fields in tuple {}",
                currentIndex - NUMBER_OF_RESERVED_FIELDS,
                (numberOfFieldsInSchema + 1));
        }

        /// First, set the number of tuples for the current field offsets buffer
        const auto numberOfTuplesRepresentedInCurrentBuffer = (currentIndex - NUMBER_OF_RESERVED_FIELDS) / (numberOfFieldsInSchema + 1);
        totalNumberOfTuples += numberOfTuplesRepresentedInCurrentBuffer;
        setNumberOfRawTuples(numberOfTuplesRepresentedInCurrentBuffer);

        this->currentIndex = NUMBER_OF_RESERVED_FIELDS;
        /// Set the first buffer as the current buffer. Adjusts the number of tuples of the first buffer. Determines if there is a child buffer.
        --this->offsetBuffers.front()[OFFSET_OF_TOTAL_NUMBER_OF_TUPLES];
    }
}

template void FieldOffsets::finishSetup<false>(FieldOffsetsType offsetToFirstTuple, FieldOffsetsType offsetToLastTuple);
template void FieldOffsets::finishSetup<true>(FieldOffsetsType offsetToFirstTuple, FieldOffsetsType offsetToLastTuple);


void FieldOffsets::allocateNewChildBuffer()
{
    INVARIANT(
        (currentIndex - NUMBER_OF_RESERVED_FIELDS) % (numberOfFieldsInSchema + 1) == 0,
        "Number of indexes {} must be a multiple of number of fields in tuple {}",
        currentIndex - NUMBER_OF_RESERVED_FIELDS,
        (numberOfFieldsInSchema + 1));

    setNumberOfRawTuples(maxNumberOfTuplesInFormattedBuffer);
    totalNumberOfTuples += maxNumberOfTuplesInFormattedBuffer;
    this->offsetBuffers.emplace_back(bufferProvider.getBufferBlocking());
    this->currentIndex = NUMBER_OF_RESERVED_FIELDS;
}

void FieldOffsets::setNumberOfRawTuples(const FieldOffsetsType numberOfTuples)
{
    this->offsetBuffers.back()[OFFSET_OF_TOTAL_NUMBER_OF_TUPLES] = numberOfTuples + 1;
}


}
