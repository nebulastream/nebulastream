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

enum class NumRequiredOffsetsPerField : uint8_t
{
    ONE,
    TWO,
};

template <NumRequiredOffsetsPerField N>
struct OffsetTypeSelector;

// Explicit specialization for ONE
template <>
struct OffsetTypeSelector<NumRequiredOffsetsPerField::ONE> {
    using type = FieldOffsetsType;
};

// Explicit specialization for TWO
template <>
struct OffsetTypeSelector<NumRequiredOffsetsPerField::TWO> {
    using type = std::pair<FieldOffsetsType, FieldOffsetsType>;
};

template <NumRequiredOffsetsPerField NumOffsetsPerField>
class FieldOffsets final : public FieldAccessFunction<FieldOffsets<NumOffsetsPerField>>
{
    friend FieldAccessFunction<FieldOffsets>;
    static constexpr size_t NUMBER_OF_RESERVED_FIELDS = 0; /// layout: [numberOfTuples | fieldOffsets ...]
    static constexpr size_t NUMBER_OF_RESERVED_BYTES = NUMBER_OF_RESERVED_FIELDS * sizeof(FieldOffsetsType);
    // static constexpr size_t OFFSET_OF_TOTAL_NUMBER_OF_TUPLES = 0;

    /// FieldAccessFunction (CRTP) interface functions
    [[nodiscard]] FieldOffsetsType applyGetOffsetOfFirstTupleDelimiter() const { return this->offsetOfFirstTuple; }
    [[nodiscard]] FieldOffsetsType applyGetOffsetOfLastTupleDelimiter() const { return this->offsetOfLastTuple; }

    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }

    [[nodiscard]] std::string_view applyReadFieldAt(const std::string_view bufferView, const size_t tupleIdx, const size_t fieldIdx) const
    requires (NumOffsetsPerField ==
    NumRequiredOffsetsPerField::TWO)
    {
        // Todo: read two indexes at a time if required
        PRECONDITION(tupleIdx < this->totalNumberOfTuples, "TupleIdx {} is out of range [0-{}].", tupleIdx, this->totalNumberOfTuples);
        PRECONDITION(fieldIdx < this->numberOfFieldsInSchema, "FieldIdx {} is out of range [0-{}].", fieldIdx, numberOfFieldsInSchema);
        // Todo: read two offsets at a time using fieldIdx
        const auto tupleIdxForDiv = (tupleIdx == 0) ? 0 : tupleIdx;
        const auto bufferNumber = tupleIdxForDiv / maxNumberOfTuplesInFormattedBuffer;
        PRECONDITION(
            bufferNumber < this->offsetBuffers.size(), "Buffer {} is out of range [0-{}].", bufferNumber, this->offsetBuffers.size());
        const auto targetBuffer = this->offsetBuffers[bufferNumber];

        const auto numTuplesInPriorBuffers = bufferNumber * maxNumberOfTuplesInFormattedBuffer;
        const auto tuplesAlreadyInCurrentBuffer = tupleIdx - numTuplesInPriorBuffers;
        const auto fieldOffset = (tuplesAlreadyInCurrentBuffer * (numberOfOffsetsPerTuple)) + fieldIdx;

        const auto& [startOfField, endOfField] = targetBuffer[NUMBER_OF_RESERVED_FIELDS + fieldOffset];
        /// There are 'numberOfOffsetsPerTuple' offsets for each tuple, so it is safe to use 'fieldIdx + 1'
        const auto sizeOfField = endOfField - startOfField;
        return bufferView.substr(startOfField, sizeOfField);
    }

    [[nodiscard]] std::string_view applyReadFieldAt(const std::string_view bufferView, const size_t tupleIdx, const size_t fieldIdx) const
    requires (NumOffsetsPerField ==
    NumRequiredOffsetsPerField::ONE)
    {
        /// If this function becomes bottleneck, we could template on whether we support tuple-based indexing or not
        /// Without tuple-based indexing, we can significantly simplify the offset calculation
        PRECONDITION(tupleIdx < this->totalNumberOfTuples, "TupleIdx {} is out of range [0-{}].", tupleIdx, this->totalNumberOfTuples);
        PRECONDITION(fieldIdx < this->numberOfFieldsInSchema, "FieldIdx {} is out of range [0-{}].", fieldIdx, numberOfFieldsInSchema);

        /// Calculate buffer from which to read field
        const auto tupleIdxForDiv = (tupleIdx == 0) ? 0 : tupleIdx;
        const auto bufferNumber = tupleIdxForDiv / maxNumberOfTuplesInFormattedBuffer;
        PRECONDITION(
            bufferNumber < this->offsetBuffers.size(), "Buffer {} is out of range [0-{}].", bufferNumber, this->offsetBuffers.size());
        const auto targetBuffer = this->offsetBuffers[bufferNumber];

        const auto numTuplesInPriorBuffers = bufferNumber * maxNumberOfTuplesInFormattedBuffer;
        const auto tuplesAlreadyInCurrentBuffer = tupleIdx - numTuplesInPriorBuffers;
        const auto fieldOffset = (tuplesAlreadyInCurrentBuffer * (numberOfOffsetsPerTuple)) + fieldIdx;

        const auto startOfField = targetBuffer[NUMBER_OF_RESERVED_FIELDS + fieldOffset];
        /// There are 'numberOfOffsetsPerTuple' offsets for each tuple, so it is safe to use 'fieldIdx + 1'
        const auto endOfField = targetBuffer[NUMBER_OF_RESERVED_FIELDS + fieldOffset + 1];
        /// The last field does not end in a tuple delimiter, so we can't deduct
        const auto sizeOfField
            = (fieldIdx != numberOfFieldsInSchema - 1) ? endOfField - startOfField - this->sizeOfFieldDelimiter : endOfField - startOfField;
        return bufferView.substr(startOfField, sizeOfField);
    }

    template<typename OffsetType>
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

public:
    explicit FieldOffsets(Memory::AbstractBufferProvider& bufferProvider) : bufferProvider(bufferProvider) { };
    ~FieldOffsets() = default;

    FieldOffsets(const FieldOffsets&) = default;
    FieldOffsets& operator=(const FieldOffsets&) = delete;
    FieldOffsets(FieldOffsets&&) = delete;
    FieldOffsets& operator=(FieldOffsets&&) = delete;

    /// InputFormatter interface functions:
    void startSetup(const size_t numberOfFieldsInSchema, const size_t sizeOfFieldDelimiter)
    {
        PRECONDITION(
            sizeOfFieldDelimiter <= std::numeric_limits<FieldOffsetsType>::max(),
            "Size of tuple delimiter must be smaller than: {}",
            std::numeric_limits<FieldOffsetsType>::max());
        this->currentIndex = NUMBER_OF_RESERVED_FIELDS;
        this->sizeOfFieldDelimiter = static_cast<FieldOffsetsType>(sizeOfFieldDelimiter);
        this->numberOfFieldsInSchema = numberOfFieldsInSchema;
        if constexpr (NumOffsetsPerField == NumRequiredOffsetsPerField::ONE)
        {
            this->maxNumberOfTuplesInFormattedBuffer = (this->bufferProvider.getBufferSize() - NUMBER_OF_RESERVED_BYTES)
                / ((numberOfFieldsInSchema + 1) * sizeof(FieldOffsetsType));
            this->numberOfOffsetsPerTuple = this->numberOfFieldsInSchema + 1;
        }
        else
        {
            /// Each field requires two offsets.
            this->maxNumberOfTuplesInFormattedBuffer = (this->bufferProvider.getBufferSize() - NUMBER_OF_RESERVED_BYTES)
                / ((numberOfFieldsInSchema + 1) * (2 * sizeof(FieldOffsetsType)));
            this->numberOfOffsetsPerTuple = this->numberOfFieldsInSchema;
        }
        PRECONDITION(
            this->maxNumberOfTuplesInFormattedBuffer != 0,
            "The buffer is of size {}, which is not large enough to represent a single tuple.",
            this->bufferProvider.getBufferSize());
        this->maxIndex = NUMBER_OF_RESERVED_FIELDS + ((numberOfOffsetsPerTuple) * maxNumberOfTuplesInFormattedBuffer);
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
            currentIndex = NUMBER_OF_RESERVED_FIELDS;
        }
    }

    // Todo: make dependent on template arg
    void writeOffsetAt(const FieldOffsetsType offset, const FieldOffsetsType idx)
    requires (NumOffsetsPerField ==
    NumRequiredOffsetsPerField::ONE)
    {
        this->offsetBuffers.back()[currentIndex + idx] = offset;
    }
    void writeOffsetAt(const std::pair<FieldOffsetsType, FieldOffsetsType>& offset, const FieldOffsetsType idx)
    requires (NumOffsetsPerField ==
    NumRequiredOffsetsPerField::TWO)
    {
        /// It is safe to write, since we always check whether there is space for a new tuple in `writeOffsetsOfNextTuple`
        this->offsetBuffers.back()[currentIndex + idx] = offset;
    }


    /// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
    template <bool ContainsOffsets>
    void finishSetup(const FieldOffsetsType offsetToFirstTuple, const FieldOffsetsType offsetToLastTuple)
    {
        this->offsetOfFirstTuple = offsetToFirstTuple;
        this->offsetOfLastTuple = offsetToLastTuple;

        if constexpr (ContainsOffsets)
        {
            /// Make sure that the number of read fields matches the expected value.
            if ((currentIndex - NUMBER_OF_RESERVED_FIELDS) % (numberOfOffsetsPerTuple) != 0)
            {
                throw FormattingError(
                    "Number of indexes {} must be a multiple of number of fields in tuple {}",
                    currentIndex - NUMBER_OF_RESERVED_FIELDS,
                    (numberOfOffsetsPerTuple));
            }

            /// First, set the number of tuples for the current field offsets buffer
            const auto numberOfTuplesRepresentedInCurrentBuffer = (currentIndex - NUMBER_OF_RESERVED_FIELDS) / (numberOfOffsetsPerTuple);
            totalNumberOfTuples += numberOfTuplesRepresentedInCurrentBuffer;
            // setNumberOfRawTuples(numberOfTuplesRepresentedInCurrentBuffer);

            this->currentIndex = NUMBER_OF_RESERVED_FIELDS;
            /// Set the first buffer as the current buffer. Adjusts the number of tuples of the first buffer. Determines if there is a child buffer.
            // --this->offsetBuffers.front()[OFFSET_OF_TOTAL_NUMBER_OF_TUPLES];
        }
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
    std::vector<FieldOffsetsBuffer<typename OffsetTypeSelector<NumOffsetsPerField>::type>> offsetBuffers;
    /// The InputFormatterTask guarantees that the reference to AbstractBufferProvider (ABP) outlives this FieldOffsets instance, since the
    /// InputFormatterTask constructs and deconstructs the FieldOffsets instance in its 'execute' function, which gets the ABP as an argument
    Memory::AbstractBufferProvider& bufferProvider; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)

    /// Sets the metadata for the current buffer, uses the buffer provider to get a new buffer.
    void allocateNewChildBuffer()
    {
        INVARIANT(
            (currentIndex - NUMBER_OF_RESERVED_FIELDS) % (numberOfOffsetsPerTuple) == 0,
            "Number of indexes {} must be a multiple of number of fields in tuple {}",
            currentIndex - NUMBER_OF_RESERVED_FIELDS,
            (numberOfOffsetsPerTuple));

        // setNumberOfRawTuples(maxNumberOfTuplesInFormattedBuffer);
        totalNumberOfTuples += maxNumberOfTuplesInFormattedBuffer;
        this->offsetBuffers.emplace_back(bufferProvider.getBufferBlocking());
        this->currentIndex = NUMBER_OF_RESERVED_FIELDS;
    }


    /// We always add 1 to the number of tuples, because we represent an overfull buffer as the max number of tuples + 1.
    /// When iterating over the index buffers, we always deduct 1 from the number of tuples, yielding the correct number of tuples in both cases
    /// (overfull, not overfull)
    // void setNumberOfRawTuples(const FieldOffsetsType)
    // {
    //     PRECONDITION(false, "Never call this");
    //     // this->totalNumberOfTuples = numberOfTuples + 1;
    //     // this->offsetBuffers.back()[OFFSET_OF_TOTAL_NUMBER_OF_TUPLES] = numberOfTuples + 1;
    // }
};
}
