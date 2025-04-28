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
#include <functional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <FieldAccessFunction.hpp>
#include <InputFormatterTask.hpp>


namespace NES::InputFormatters
{

class FieldOffsets final : public FieldAccessFunction<FieldOffsets>
{
    friend FieldAccessFunction;
    static constexpr size_t NUMBER_OF_RESERVED_FIELDS = 1; /// layout: [numberOfTuples | fieldOffsets ...]
    static constexpr size_t NUMBER_OF_RESERVED_BYTES = NUMBER_OF_RESERVED_FIELDS * sizeof(FieldOffsetsType);
    static constexpr size_t OFFSET_OF_TOTAL_NUMBER_OF_TUPLES = 0;

    /// FieldAccessFunction (CRTP) interface functions
    [[nodiscard]] FieldOffsetsType applyGetOffsetOfFirstTupleDelimiter() const;
    [[nodiscard]] FieldOffsetsType applyGetOffsetOfLastTupleDelimiter() const;
    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const;
    [[nodiscard]] std::string_view applyReadFieldAt(std::string_view bufferView, size_t tupleIdx, size_t fieldIdx) const;

    class FieldOffsetsBuffer
    {
    public:
        explicit FieldOffsetsBuffer(Memory::TupleBuffer tupleBuffer)
            : tupleBuffer(std::move(tupleBuffer))
            , fieldOffsetSpan(this->tupleBuffer.getBuffer<FieldOffsetsType>(), this->tupleBuffer.getBufferSize()) { };
        ~FieldOffsetsBuffer() = default;

        [[nodiscard]] FieldOffsetsType& operator[](const size_t tupleIdx) const { return fieldOffsetSpan[tupleIdx]; }

    private:
        Memory::TupleBuffer tupleBuffer;
        std::span<FieldOffsetsType> fieldOffsetSpan;
    };

public:
    explicit FieldOffsets(Memory::AbstractBufferProvider& bufferProvider) : bufferProvider(bufferProvider) { };
    ~FieldOffsets() = default;

    FieldOffsets(const FieldOffsets&) = default;
    FieldOffsets& operator=(const FieldOffsets&) = delete;
    FieldOffsets(FieldOffsets&&) = delete;
    FieldOffsets& operator=(FieldOffsets&&) = delete;

    /// InputFormatter interface functions:
    void startSetup(size_t numberOfFieldsInSchema, size_t sizeOfFieldDelimiter);
    /// Assures that there is space to write one more tuple and returns a pointer to write the field offsets (of one tuple) to.
    /// @Note expects that users of function write 'number of fields in schema + 1' offsets to pointer, manually incrementing the pointer by one for each offset.
    void writeOffsetsOfNextTuple();
    void writeOffsetAt(FieldOffsetsType offset, FieldOffsetsType idx);

    /// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
    template <bool ContainsOffsets>
    void finishSetup(FieldOffsetsType offsetToFirstTuple, FieldOffsetsType offsetToLastTuple);

private:
    size_t currentIndex{};
    FieldOffsetsType sizeOfFieldDelimiter{};
    size_t numberOfFieldsInSchema{};
    size_t maxNumberOfTuplesInFormattedBuffer{};
    size_t maxIndex{};
    size_t totalNumberOfTuples{};
    FieldOffsetsType offsetOfFirstTuple{};
    FieldOffsetsType offsetOfLastTuple{};
    std::vector<FieldOffsetsBuffer> offsetBuffers;
    /// The InputFormatterTask guarantees that the reference to AbstractBufferProvider (ABP) outlives this FieldOffsets instance, since the
    /// InputFormatterTask constructs and deconstructs the FieldOffsets instance in its 'execute' function, which gets the ABP as an argument
    Memory::AbstractBufferProvider& bufferProvider; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)

    /// Sets the metadata for the current buffer, uses the buffer provider to get a new buffer.
    void allocateNewChildBuffer();

    /// We always add 1 to the number of tuples, because we represent an overfull buffer as the max number of tuples + 1.
    /// When iterating over the index buffers, we always deduct 1 from the number of tuples, yielding the correct number of tuples in both cases
    /// (overfull, not overfull)
    inline void setNumberOfRawTuples(FieldOffsetsType numberOfTuples);
};
}
