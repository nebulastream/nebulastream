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
#include <memory>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::InputFormatters
{

/// TODO #496: Implement as Nautilus data structure
/// TupleBuffer-backed iterator that allows input formatters to write field offsets into a tuple buffer.
/// After the input formatter finishes calculating offsets to fields, the InputFormatterTask finalizes the offset calculation,
/// resets the FieldOffsetIterator, starts reading the field offsets, calculating start and end index of each field and parsing each field.
class FieldOffsets
{
    static constexpr size_t NUMBER_OF_RESERVED_FIELDS = 2; /// layout: [numberOfTuples | indexToChildBuffer | filedOffsets ...]
    static constexpr size_t NUMBER_OF_RESERVED_BYTES = NUMBER_OF_RESERVED_FIELDS * sizeof(FieldOffsetsType);
    static constexpr size_t OFFSET_OF_TOTAL_NUMBER_OF_TUPLES = 0;
    static constexpr size_t OFFSET_OF_INDEX_TO_CHILD_BUFFER = 1;

public:
    /// Gets AbstractBufferProvider* from InputFormatterTask, which constructs the
    /// FieldOffsets object and is guaranteed to outlive it.
    FieldOffsets(
        size_t numberOfFieldsInBuffer, size_t sizeOfFormattedBufferInBytes, Memory::AbstractBufferProvider* fieldOffsetBufferProvider);

    ~FieldOffsets() = default;

    FieldOffsets(const FieldOffsets&) = default;
    FieldOffsets& operator=(const FieldOffsets&) = delete;
    FieldOffsets(FieldOffsets&&) = delete;
    FieldOffsets& operator=(FieldOffsets&&) = delete;

    /// Assures that there is space to write one more tuple and returns a pointer to write the field offsets (of one tuple) to.
    /// @Note expects that users of function write 'number of fields in schema + 1' offsets to pointer, manually incrementing the pointer by one for each offset.
    [[nodiscard]] FieldOffsetsType* writeNextTuple();

    /// Returns a pointer to field offsets for one tuple that are consecutive in memory.
    /// @Note expects that users of function read 'number of fields in schema + 1' offsets from pointer, manually incrementing the pointer by one fo reach offset.
    [[nodiscard]] FieldOffsetsType* readNextTuple();

    /// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
    [[nodiscard]] size_t finishRead();


private:
    size_t currentIndex;
    size_t numberOfFieldsInTuple;
    size_t maxNumberOfTuplesInFormattedBuffer;
    size_t maxIndex;
    size_t totalNumberOfTuples;
    Memory::TupleBuffer rootFieldOffsetBuffer;
    Memory::AbstractBufferProvider* fieldOffsetBufferProvider;
    Memory::TupleBuffer currentFieldOffsetBuffer;

    /// Sets the metadata for the current buffer, uses the buffer provider to get a new buffer.
    void allocateNewChildBuffer();

    /// Called by 'writeNextTuple'/'readNextTuple'
    /// (write) allocates a new buffer, if the current buffer does not have space for one more tuple
    /// (read) moves pointer to next buffer, if the current buffer does not hold another tuple
    template <bool IsWrite>
    FieldOffsetsType* processTuple()
    {
        if (currentIndex + (numberOfFieldsInTuple + 1) > maxIndex)
        {
            if constexpr (IsWrite)
            {
                allocateNewChildBuffer();
            }
            else
            {
                /// Set the next child buffer as the current buffer
                INVARIANT(
                    this->currentFieldOffsetBuffer.getNumberOfChildBuffers() > 0,
                    "Cannot get next fields if current buffer does not have child buffer.");
                const auto childBufferIndex = this->currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>()[OFFSET_OF_INDEX_TO_CHILD_BUFFER];
                this->currentFieldOffsetBuffer = currentFieldOffsetBuffer.loadChildBuffer(childBufferIndex);
                --this->currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>()[OFFSET_OF_TOTAL_NUMBER_OF_TUPLES];
            }
            currentIndex = NUMBER_OF_RESERVED_FIELDS;
        }
        const auto currentFieldIndex = currentIndex;
        currentIndex += numberOfFieldsInTuple + 1;
        return currentFieldOffsetBuffer.getBuffer<FieldOffsetsType>() + currentFieldIndex;
    }
};

}
