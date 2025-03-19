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
#include <InputFormatters/AsyncInputFormatterTask.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsetsIterator.hpp>

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

FieldOffsetIterator::FieldOffsetIterator(
    const size_t numberOfFieldsInBuffer,
    const size_t sizeOfFormattedBufferInBytes,
    std::shared_ptr<Memory::AbstractBufferProvider> fieldOffsetBufferProvider)
    : currentIndex(NUMBER_OF_RESERVED_FIELDS)
    , numberOfFieldsInTuple(numberOfFieldsInBuffer)
    , maxNumberOfTuplesInFormattedBuffer(
          (sizeOfFormattedBufferInBytes - NUMBER_OF_RESERVED_BYTES) / ((numberOfFieldsInTuple + 1) * sizeof(FieldOffsetsType)))
    , maxIndex(NUMBER_OF_RESERVED_FIELDS + ((numberOfFieldsInTuple + 1) * maxNumberOfTuplesInFormattedBuffer))
    , totalNumberOfTuples(0)
    , rootFieldOffsetBuffer(fieldOffsetBufferProvider->getBufferBlocking())
    , fieldOffsetBufferProvider(std::move(fieldOffsetBufferProvider))
    , currentFieldOffsetBuffer(this->rootFieldOffsetBuffer)
{
}

void FieldOffsetIterator::write(const FieldOffsetsType value)
{
    if (currentIndex >= maxIndex)
    {
        allocateNewBuffer();
        currentIndex = NUMBER_OF_RESERVED_FIELDS;
    }

    currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>()[currentIndex] = value;
}
FieldOffsetsType FieldOffsetIterator::read()
{
    if (currentIndex >= maxIndex)
    {
        getNextFieldOffsets();
        currentIndex = NUMBER_OF_RESERVED_FIELDS;
    }
    return currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>()[currentIndex];
}
FieldOffsetIterator& FieldOffsetIterator::operator++()
{
    ++currentIndex;
    return *this;
}

void FieldOffsetIterator::allocateNewBuffer()
{
    auto newBuffer = fieldOffsetBufferProvider->getBufferBlocking();
    const auto indexOfNewBuffer = currentFieldOffsetBuffer.storeChildBuffer(newBuffer);
    /// The number of tuples communicate whether the buffer has a child buffer or not. Thus, the index to the child buffer is optional.
    /// We could write it to the last bytes of the buffer, if necessary, but that creates room for error when writing actual offsets.
    /// Overall a few bytes 'lost' is a good trait for better clarity/safety.
    /// (it also does not seem like we can simply assume that the first index is '0' and so on, so we need to store it somewhere)
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

size_t FieldOffsetIterator::finishRead()
{
    /// First, set the number of tuples for the current field offsets buffer
    const auto numberOfTuplesRepresentedInCurrentBuffer = (currentIndex - NUMBER_OF_RESERVED_FIELDS) / (numberOfFieldsInTuple + 1);
    totalNumberOfTuples += numberOfTuplesRepresentedInCurrentBuffer;
    setNumberOfRawTuples(
        numberOfTuplesRepresentedInCurrentBuffer,
        this->currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>(),
        OFFSET_OF_TOTAL_NUMBER_OF_TUPLES);

    this->currentIndex = NUMBER_OF_RESERVED_FIELDS;
    this->getFirstFieldOffsets();
    return totalNumberOfTuples;
}

void FieldOffsetIterator::getFirstFieldOffsets()
{
    this->currentFieldOffsetBuffer = rootFieldOffsetBuffer;
    --this->currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>()[OFFSET_OF_TOTAL_NUMBER_OF_TUPLES];
}
void FieldOffsetIterator::getNextFieldOffsets()
{
    PRECONDITION(
        this->currentFieldOffsetBuffer.getNumberOfChildrenBuffer() > 0,
        "Cannot get next fields if current buffer does not have child buffer.");
    const auto childBufferIndex = this->currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>()[OFFSET_OF_INDEX_TO_CHILD_BUFFER];
    this->currentFieldOffsetBuffer = currentFieldOffsetBuffer.loadChildBuffer(childBufferIndex);
    --this->currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>()[OFFSET_OF_TOTAL_NUMBER_OF_TUPLES];
}
};
