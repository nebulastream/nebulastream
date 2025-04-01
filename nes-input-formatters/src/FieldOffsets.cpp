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
#include <functional>
#include <memory>
#include <utility>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>

namespace NES::InputFormatters
{

FieldOffsets::FieldOffsets(
    const size_t numberOfFieldsInSchema,
    const size_t sizeOfFormattedBufferInBytes,
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider)
    : currentIndex(NUMBER_OF_RESERVED_FIELDS)
    , numberOfFieldsInSchema(numberOfFieldsInSchema)
    , maxNumberOfTuplesInFormattedBuffer(
          (sizeOfFormattedBufferInBytes - NUMBER_OF_RESERVED_BYTES) / ((numberOfFieldsInSchema + 1) * sizeof(FieldOffsetsType)))
    , maxIndex(NUMBER_OF_RESERVED_FIELDS + ((numberOfFieldsInSchema + 1) * maxNumberOfTuplesInFormattedBuffer))
    , totalNumberOfTuples(0)
    , rootFieldOffsetBuffer(bufferProvider->getBufferBlocking())
    , bufferProvider(std::move(bufferProvider))
    , currentFieldOffsetBuffer(this->rootFieldOffsetBuffer)
{
}

FieldOffsetsType* FieldOffsets::writeOffsetsOfNextTuple()
{
    const auto getNextWriteBuffer = [this]() -> void { allocateNewChildBuffer(); };
    return processTuple(getNextWriteBuffer);
}

FieldOffsetsType* FieldOffsets::readOffsetsOfNextTuple()
{
    const auto getNextReadBuffer = [this]() -> void
    {
        INVARIANT(
            this->currentFieldOffsetBuffer.getNumberOfChildBuffers() > 0,
            "Cannot get next fields if current buffer does not have child buffer.");
        const auto childBufferIndex = this->currentFieldOffsetBuffer.getBuffer< ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            FieldOffsetsType>()[OFFSET_OF_INDEX_TO_CHILD_BUFFER];
        this->currentFieldOffsetBuffer = currentFieldOffsetBuffer.loadChildBuffer(childBufferIndex);
        --this->currentFieldOffsetBuffer
              .getBuffer<FieldOffsetsType>()[OFFSET_OF_TOTAL_NUMBER_OF_TUPLES]; ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    };
    return processTuple(getNextReadBuffer);
}

FieldOffsetsType* FieldOffsets::processTuple(const std::function<void()>& handleOutOfSpaceFn)
{
    if (currentIndex + (numberOfFieldsInSchema + 1) > maxIndex)
    {
        handleOutOfSpaceFn();
        currentIndex = NUMBER_OF_RESERVED_FIELDS;
    }
    const auto currentFieldIndex = currentIndex;
    currentIndex += numberOfFieldsInSchema + 1;
    return currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>()
        + currentFieldIndex; ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

void FieldOffsets::allocateNewChildBuffer()
{
    auto newBuffer = bufferProvider->getBufferBlocking();
    const auto indexOfNewBuffer = currentFieldOffsetBuffer.storeChildBuffer(newBuffer);
    INVARIANT(
        (currentIndex - NUMBER_OF_RESERVED_FIELDS) % (numberOfFieldsInSchema + 1) == 0,
        "Number of indexes {} must be a multiple of number of fields in tuple {}",
        currentIndex - NUMBER_OF_RESERVED_FIELDS,
        (numberOfFieldsInSchema + 1));

    setNumberOfRawTuples(maxNumberOfTuplesInFormattedBuffer);
    totalNumberOfTuples += maxNumberOfTuplesInFormattedBuffer;
    this->currentFieldOffsetBuffer
        .getBuffer<FieldOffsetsType>()[OFFSET_OF_INDEX_TO_CHILD_BUFFER] ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        = indexOfNewBuffer; ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    this->currentFieldOffsetBuffer = currentFieldOffsetBuffer.loadChildBuffer(indexOfNewBuffer);
    this->currentIndex = NUMBER_OF_RESERVED_FIELDS;
}

size_t FieldOffsets::finishWrite()
{
    /// Make sure that the number of read fields matches the expected value.
    if ((currentIndex - NUMBER_OF_RESERVED_FIELDS) % (numberOfFieldsInSchema + 1) != 0)
    {
        throw CSVParsingError(
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
    this->currentFieldOffsetBuffer = rootFieldOffsetBuffer;
    --this->currentFieldOffsetBuffer
          .getBuffer<FieldOffsetsType>()[OFFSET_OF_TOTAL_NUMBER_OF_TUPLES]; ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    return totalNumberOfTuples;
}

void FieldOffsets::setNumberOfRawTuples(const FieldOffsetsType numberOfTuples)
{
    this->currentFieldOffsetBuffer
        .getBuffer<FieldOffsetsType>()[OFFSET_OF_TOTAL_NUMBER_OF_TUPLES] ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        = numberOfTuples + 1;
}

};
