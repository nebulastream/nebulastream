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
#include <FieldIndexFunction.hpp>
#include <InputFormatterTask.hpp>


namespace NES::InputFormatters
{

enum class NumRequiredOffsetsPerField : uint8_t
{
    ONE,
    TWO,
};

struct FieldOffsetTypePair
{
    FieldOffsetsType startOfField{};
    FieldOffsetsType endOfField{};
};

/// Determines the in-tuple-buffer representation of the offsets for type-safety reasons
template <NumRequiredOffsetsPerField N>
struct FieldOffsetTypeSelector;
template <>
struct FieldOffsetTypeSelector<NumRequiredOffsetsPerField::ONE>
{
    using type = FieldOffsetsType;
};
template <>
struct FieldOffsetTypeSelector<NumRequiredOffsetsPerField::TWO>
{
    using type = FieldOffsetTypePair;
};

template <NumRequiredOffsetsPerField NumOffsetsPerField>
class FieldOffsets final : public FieldIndexFunction<FieldOffsets<NumOffsetsPerField>>
{
    friend FieldIndexFunction<FieldOffsets>;

    /// FieldIndexFunction (CRTP) interface functions
    [[nodiscard]] FieldOffsetsType applyGetOffsetOfFirstTupleDelimiter() const { return this->offsetOfFirstTuple; }
    [[nodiscard]] FieldOffsetsType applyGetOffsetOfLastTupleDelimiter() const { return this->offsetOfLastTuple; }
    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }
    [[nodiscard]] std::string_view applyReadFieldAt(const std::string_view bufferView, const size_t tupleIdx, const size_t fieldIdx) const
    {
        PRECONDITION(tupleIdx < this->totalNumberOfTuples, "TupleIdx {} is out of range [0-{}].", tupleIdx, this->totalNumberOfTuples);
        PRECONDITION(fieldIdx < this->numberOfFieldsInSchema, "FieldIdx {} is out of range [0-{}].", fieldIdx, numberOfFieldsInSchema);

        const auto tupleIdxForDiv = (tupleIdx == 0) ? 0 : tupleIdx;
        const auto bufferNumber = tupleIdxForDiv / maxNumberOfTuplesInFormattedBuffer;
        PRECONDITION(
            bufferNumber < this->offsetBuffers.size(), "Buffer {} is out of range [0-{}].", bufferNumber, this->offsetBuffers.size());

        const auto numTuplesInPriorBuffers = bufferNumber * maxNumberOfTuplesInFormattedBuffer;
        const auto tuplesAlreadyInCurrentBuffer = tupleIdx - numTuplesInPriorBuffers;
        const auto fieldOffset = (tuplesAlreadyInCurrentBuffer * (numberOfOffsetsPerTuple)) + fieldIdx;

        return createFieldSV(bufferView, bufferNumber, fieldOffset, fieldIdx);
    }

    template <typename OffsetType>
    class FieldOffsetsBuffer
    {
    public:
        explicit FieldOffsetsBuffer(Memory::TupleBuffer tupleBuffer)
            : tupleBuffer(std::move(tupleBuffer))
            , fieldOffsetSpan(this->tupleBuffer.template getBuffer<OffsetType>(), this->tupleBuffer.getBufferSize()) { };
        ~FieldOffsetsBuffer() = default;

        [[nodiscard]] OffsetType& operator[](const size_t tupleIdx) const { return fieldOffsetSpan[tupleIdx]; }

    private:
        Memory::TupleBuffer tupleBuffer;
        std::span<OffsetType> fieldOffsetSpan;
    };


    [[nodiscard]] std::string_view
    createFieldSV(const std::string_view bufferView, const size_t bufferNumber, const size_t fieldOffset, const size_t fieldIdx) const
    requires(NumOffsetsPerField == NumRequiredOffsetsPerField::ONE)
    {
        const auto targetBuffer = this->offsetBuffers[bufferNumber];
        const auto startOfField = targetBuffer[fieldOffset];
        const auto endOfField = targetBuffer[fieldOffset + 1];
        /// Deduct the size of the field delimiter for all fields but the last, since a tuple does not end in a field delimiter
        const auto isLastFiled = fieldIdx == (numberOfFieldsInSchema - 1);
        const auto sizeOfField = endOfField - startOfField - (isLastFiled ? 0 : this->sizeOfFieldDelimiter);
        return bufferView.substr(startOfField, sizeOfField);
    }

    [[nodiscard]] std::string_view
    createFieldSV(const std::string_view bufferView, const size_t bufferNumber, const size_t fieldOffset, const size_t) const
    requires(NumOffsetsPerField == NumRequiredOffsetsPerField::TWO)
    {
        const auto targetBuffer = this->offsetBuffers[bufferNumber];
        const auto& [startOfField, endOfField] = targetBuffer[fieldOffset];
        const auto sizeOfField = endOfField - startOfField;
        return bufferView.substr(startOfField, sizeOfField);
    }

public:
    explicit FieldOffsets(Memory::AbstractBufferProvider& bufferProvider) : bufferProvider(bufferProvider) { };
    ~FieldOffsets() = default;

    /// InputFormatter interface functions:
    void startSetup(const size_t numberOfFieldsInSchema, const size_t sizeOfFieldDelimiter)
    {
        PRECONDITION(
            sizeOfFieldDelimiter <= std::numeric_limits<FieldOffsetsType>::max(),
            "Size of tuple delimiter must be smaller than: {}",
            std::numeric_limits<FieldOffsetsType>::max());
        this->currentIndex = 0;
        this->sizeOfFieldDelimiter = static_cast<FieldOffsetsType>(sizeOfFieldDelimiter);
        this->numberOfFieldsInSchema = numberOfFieldsInSchema;
        if constexpr (NumOffsetsPerField == NumRequiredOffsetsPerField::ONE)
        {
            this->maxNumberOfTuplesInFormattedBuffer
                = (this->bufferProvider.getBufferSize()) / ((numberOfFieldsInSchema + 1) * sizeof(FieldOffsetsType));
            this->numberOfOffsetsPerTuple = this->numberOfFieldsInSchema + 1;
        }
        else
        {
            /// Each field requires two offsets.
            this->maxNumberOfTuplesInFormattedBuffer
                = (this->bufferProvider.getBufferSize()) / ((numberOfFieldsInSchema + 1) * (2 * sizeof(FieldOffsetsType)));
            this->numberOfOffsetsPerTuple = this->numberOfFieldsInSchema;
        }
        PRECONDITION(
            this->maxNumberOfTuplesInFormattedBuffer != 0,
            "The buffer is of size {}, which is not large enough to represent a single tuple.",
            this->bufferProvider.getBufferSize());
        this->maxIndex = ((numberOfOffsetsPerTuple)*maxNumberOfTuplesInFormattedBuffer);
        this->totalNumberOfTuples = 0;
        this->offsetBuffers.emplace_back(this->bufferProvider.getBufferBlocking());
    }

    /// Assures that there is space to write one more tuple and returns a pointer to write the field offsets (of one tuple) to.
    /// @Note expects that users of function write 'number of fields in schema + 1' offsets to pointer, manually incrementing the pointer by one for each offset.
    void writeOffsetsOfNextTuple()
    {
        currentIndex += numberOfOffsetsPerTuple;
        if (currentIndex >= maxIndex)
        {
            allocateNewChildBuffer();
            currentIndex = 0;
        }
    }

    void writeOffsetAt(const FieldOffsetsType offset, const FieldOffsetsType idx)
    requires(NumOffsetsPerField == NumRequiredOffsetsPerField::ONE)
    {
        this->offsetBuffers.back()[currentIndex + idx] = offset;
    }
    void writeOffsetAt(const FieldOffsetTypePair& offset, const FieldOffsetsType idx)
    requires(NumOffsetsPerField == NumRequiredOffsetsPerField::TWO)
    {
        /// It is safe to write, since we always check whether there is space for a new tuple in `writeOffsetsOfNextTuple`
        this->offsetBuffers.back()[currentIndex + idx] = offset;
    }


    /// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
    void markNoTupleDelimiters()
    {
        this->offsetOfFirstTuple = std::numeric_limits<FieldOffsetsType>::max();
        this->offsetOfLastTuple = std::numeric_limits<FieldOffsetsType>::max();
    }
    void markWithTupleDelimiters(const FieldOffsetsType offsetToFirstTuple, const FieldOffsetsType offsetToLastTuple)
    {
        this->offsetOfFirstTuple = offsetToFirstTuple;
        this->offsetOfLastTuple = offsetToLastTuple;

        /// Make sure that the number of read fields matches the expected value.
        if (currentIndex % (numberOfOffsetsPerTuple) != 0)
        {
            throw FormattingError(
                "Number of indexes {} must be a multiple of number of fields in tuple {}", currentIndex, (numberOfOffsetsPerTuple));
        }

        /// First, set the number of tuples for the current field offsets buffer
        const auto numberOfTuplesRepresentedInCurrentBuffer = currentIndex / (numberOfOffsetsPerTuple);
        totalNumberOfTuples += numberOfTuplesRepresentedInCurrentBuffer;

        this->currentIndex = 0;
    }

private:
    size_t currentIndex{};
    FieldOffsetsType sizeOfFieldDelimiter{};
    size_t numberOfFieldsInSchema{};
    size_t numberOfOffsetsPerTuple{};
    size_t maxNumberOfTuplesInFormattedBuffer{};
    size_t maxIndex{};
    size_t totalNumberOfTuples{};
    FieldOffsetsType offsetOfFirstTuple{};
    FieldOffsetsType offsetOfLastTuple{};
    std::vector<FieldOffsetsBuffer<typename FieldOffsetTypeSelector<NumOffsetsPerField>::type>> offsetBuffers;
    /// The InputFormatterTask guarantees that the reference to AbstractBufferProvider (ABP) outlives this FieldOffsets instance, since the
    /// InputFormatterTask constructs and deconstructs the FieldOffsets instance in its 'execute' function, which gets the ABP as an argument
    Memory::AbstractBufferProvider& bufferProvider; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)

    /// Sets the metadata for the current buffer, uses the buffer provider to get a new buffer.
    void allocateNewChildBuffer()
    {
        INVARIANT(
            currentIndex % numberOfOffsetsPerTuple == 0,
            "Number of indexes {} must be a multiple of number of fields in tuple {}",
            currentIndex,
            (numberOfOffsetsPerTuple));

        totalNumberOfTuples += maxNumberOfTuplesInFormattedBuffer;
        this->offsetBuffers.emplace_back(bufferProvider.getBufferBlocking());
        this->currentIndex = 0;
    }
};
}
