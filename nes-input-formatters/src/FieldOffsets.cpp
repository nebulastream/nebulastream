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
#include <cstdint>
#include <memory>
#include <utility>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>

namespace
{
/// We always add 1 to the number of tuples, because we represent an overfull buffer as the max number of tuples + 1.
/// When iterating over the index buffers, we always deduct 1 from the number of tuples, yielding the correct number of tuples in both cases
/// (overfull, not overfull)
void setNumberOfRawTuples(
    const NES::InputFormatters::FieldOffsetsType numberOfTuples,
    NES::InputFormatters::FieldOffsetsType* bufferPtr,
    const NES::InputFormatters::FieldOffsetsType totalNumberOfTuplesOffset)
{
    bufferPtr[totalNumberOfTuplesOffset] = numberOfTuples + 1;
}
}

namespace NES::InputFormatters
{

FieldOffsets::FieldOffsets(
    const size_t numberOfFieldsInBuffer,
    const size_t sizeOfFormattedBufferInBytes,
    Memory::AbstractBufferProvider* fieldOffsetBufferProvider)
    : currentIndex(NUMBER_OF_RESERVED_FIELDS)
    , numberOfFieldsInTuple(numberOfFieldsInBuffer)
    , maxNumberOfTuplesInFormattedBuffer(
          (sizeOfFormattedBufferInBytes - NUMBER_OF_RESERVED_BYTES) / ((numberOfFieldsInTuple + 1) * sizeof(FieldOffsetsType)))
    , maxIndex(NUMBER_OF_RESERVED_FIELDS + ((numberOfFieldsInTuple + 1) * maxNumberOfTuplesInFormattedBuffer))
    , totalNumberOfTuples(0)
    , rootFieldOffsetBuffer(fieldOffsetBufferProvider->getBufferBlocking())
    , fieldOffsetBufferProvider(fieldOffsetBufferProvider)
    , currentFieldOffsetBuffer(this->rootFieldOffsetBuffer)
{
}

FieldOffsetsType* FieldOffsets::writeNextTuple()
{
    return processTuple<true>();
}

FieldOffsetsType* FieldOffsets::readNextTuple()
{
    return processTuple<false>();
}

void FieldOffsets::allocateNewChildBuffer()
{
    auto newBuffer = fieldOffsetBufferProvider->getBufferBlocking();
    const auto indexOfNewBuffer = currentFieldOffsetBuffer.storeChildBuffer(newBuffer);
    INVARIANT(
        (currentIndex - NUMBER_OF_RESERVED_FIELDS) % (numberOfFieldsInTuple + 1) == 0,
        "Number of indexes {} must be a multiple of number of fields in tuple {}",
        currentIndex - NUMBER_OF_RESERVED_FIELDS,
        (numberOfFieldsInTuple + 1));

    setNumberOfRawTuples(
        maxNumberOfTuplesInFormattedBuffer, this->currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>(), OFFSET_OF_TOTAL_NUMBER_OF_TUPLES);
    totalNumberOfTuples += maxNumberOfTuplesInFormattedBuffer;
    this->currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>()[OFFSET_OF_INDEX_TO_CHILD_BUFFER] = indexOfNewBuffer;
    this->currentFieldOffsetBuffer = currentFieldOffsetBuffer.loadChildBuffer(indexOfNewBuffer);
    this->currentIndex = NUMBER_OF_RESERVED_FIELDS;
}

size_t FieldOffsets::finishWrite()
{
    /// Make sure that the number of read fields matches the expected value.
    if ((currentIndex - NUMBER_OF_RESERVED_FIELDS) % (numberOfFieldsInTuple + 1) != 0)
    {
        throw FormattingError(
            "Number of indexes {} must be a multiple of number of fields in tuple {}",
            currentIndex - NUMBER_OF_RESERVED_FIELDS,
            (numberOfFieldsInTuple + 1));
    }

    /// First, set the number of tuples for the current field offsets buffer
    const auto numberOfTuplesRepresentedInCurrentBuffer = (currentIndex - NUMBER_OF_RESERVED_FIELDS) / (numberOfFieldsInTuple + 1);
    totalNumberOfTuples += numberOfTuplesRepresentedInCurrentBuffer;
    setNumberOfRawTuples(
        numberOfTuplesRepresentedInCurrentBuffer,
        this->currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>(),
        OFFSET_OF_TOTAL_NUMBER_OF_TUPLES);

    this->currentIndex = NUMBER_OF_RESERVED_FIELDS;
    /// Set the first buffer as the current buffer. Adjusts the number of tuples of the first buffer. Determines if there is a child buffer.
    this->currentFieldOffsetBuffer = rootFieldOffsetBuffer;
    --this->currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>()[OFFSET_OF_TOTAL_NUMBER_OF_TUPLES];
    return totalNumberOfTuples;
}

};
