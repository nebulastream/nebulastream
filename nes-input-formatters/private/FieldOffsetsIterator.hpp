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
/// After the input formatter finishes calculing offsets to fields, the InputFormatterTask finalizes the offset calculation,
/// resets the FieldOffsetIterator, starts reading the field offsets, calculating start and end index of each field and parsing each field.
class FieldOffsetIterator
{
    static constexpr size_t NUMBER_OF_RESERVED_FIELDS = 2; /// [numberOfTuples, indexToChildBuffer]
    static constexpr size_t NUMBER_OF_RESERVED_BYTES = NUMBER_OF_RESERVED_FIELDS * sizeof(FieldOffsetsType);
    static constexpr size_t OFFSET_OF_TOTAL_NUMBER_OF_TUPLES = 0;
    static constexpr size_t OFFSET_OF_INDEX_TO_CHILD_BUFFER = 1;

public:
    FieldOffsetIterator(
        size_t numberOfFieldsInBuffer,
        size_t sizeOfFormattedBufferInBytes,
        std::shared_ptr<Memory::AbstractBufferProvider> fieldOffsetBufferProvider);

    ~FieldOffsetIterator() = default;

    FieldOffsetIterator(const FieldOffsetIterator&) = default;
    FieldOffsetIterator& operator=(const FieldOffsetIterator&) = delete;
    FieldOffsetIterator(FieldOffsetIterator&&) = delete;
    FieldOffsetIterator& operator=(FieldOffsetIterator&&) = delete;

    FieldOffsetIterator& operator++();

    /// First checks if the next value is in the next buffer, and loads the next buffer if applies. Then, writes offset at current position.
    void write(FieldOffsetsType value);

    /// First checks if the next value is in the next buffer, and loads the next buffer if applies. Then, return offset at current position.
    [[nodiscard]] FieldOffsetsType read();

    /// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
    [[nodiscard]] size_t finishRead();


private:
    size_t currentIndex;
    size_t numberOfFieldsInTuple;
    size_t maxNumberOfTuplesInFormattedBuffer;
    size_t maxIndex;
    size_t totalNumberOfTuples;
    Memory::TupleBuffer rootFieldOffsetBuffer;
    std::shared_ptr<Memory::AbstractBufferProvider> fieldOffsetBufferProvider;
    Memory::TupleBuffer currentFieldOffsetBuffer;

    /// Sets the metadata for the current buffer, uses the buffer provider to get a new buffer.
    void allocateNewBuffer();
    /// Sets the first buffer as the current buffer. Adjusts the number of tuples of the first buffer. Determines if there is a nested buffer.
    void getFirstFieldOffsets();
    /// Attempts to get the next nested buffer.
    /// @Note asserts that the current buffer has a child buffer.
    void getNextFieldOffsets();
};

}
