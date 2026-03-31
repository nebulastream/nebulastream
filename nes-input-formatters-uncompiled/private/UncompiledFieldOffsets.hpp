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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include <Concepts.hpp>
#include <ErrorHandling.hpp>
#include <UncompiledFieldIndexFunction.hpp>
#include <UncompiledInputFormatterTask.hpp>

// inline bool
// includesField(const std::vector<NES::Record::RecordFieldIdentifier>& projections, const NES::Record::RecordFieldIdentifier& fieldIndex)
// {
//     return std::ranges::find(projections, fieldIndex) != projections.end();
// }

namespace NES
{

enum class UncompiledNumRequiredOffsetsPerField : uint8_t
{
    ONE,
    TWO,
};

template <UncompiledNumRequiredOffsetsPerField NumOffsetsPerField>
class UncompiledFieldOffsets final : public UncompiledFieldIndexFunction<UncompiledFieldOffsets<NumOffsetsPerField>>
{
    friend UncompiledFieldIndexFunction<UncompiledFieldOffsets>;

    /// UncompiledFieldIndexFunction (CRTP) interface functions
    [[nodiscard]] UncompiledFieldIndex applyGetByteOffsetOfFirstTuple() const { return this->offsetOfFirstTuple; }

    [[nodiscard]] UncompiledFieldIndex applyGetByteOffsetOfLastTuple() const { return this->offsetOfLastTuple; }

    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }

    [[nodiscard]] std::string_view applyReadFieldAt(const std::string_view bufferView, const size_t tupleIdx, const size_t fieldIdx) const
    {
        const size_t numberOfPriorFields = tupleIdx * numberOfOffsetsPerTuple + fieldIdx;
        const auto startOfCurrentField = this->indexValues.at(numberOfPriorFields);
        const auto endOfCurrentField = this->indexValues.at(numberOfPriorFields + 1);
        const auto sizeOfCurrentField = endOfCurrentField - startOfCurrentField - (sizeOfFieldDelimiter - static_cast<size_t>(fieldIdx + 1 == numberOfFieldsInSchema));
        return std::string_view(bufferView.substr(startOfCurrentField, sizeOfCurrentField));
    }

public:
    UncompiledFieldOffsets() = default;
    ~UncompiledFieldOffsets() = default;

    /// InputFormatter interface functions:
    void startSetup(size_t numberOfFieldsInSchema, size_t sizeOfFieldDelimiter)
    {
        PRECONDITION(
            sizeOfFieldDelimiter <= std::numeric_limits<UncompiledFieldIndex>::max(),
            "Size of tuple delimiter must be smaller than: {}",
            std::numeric_limits<UncompiledFieldIndex>::max());
        this->sizeOfFieldDelimiter = static_cast<UncompiledFieldIndex>(sizeOfFieldDelimiter);
        this->numberOfFieldsInSchema = numberOfFieldsInSchema;
        this->numberOfOffsetsPerTuple
            = this->numberOfFieldsInSchema + static_cast<size_t>(NumOffsetsPerField == UncompiledNumRequiredOffsetsPerField::ONE);
        this->totalNumberOfTuples = 0;
    }

    /// Assures that there is space to write one more tuple and returns a pointer to write the field offsets (of one tuple) to.
    /// @Note expects that users of function write 'number of fields in schema + 1' offsets to pointer, manually incrementing the pointer by one for each offset.
    void emplaceFieldOffset(UncompiledFieldIndex offset)
    requires(NumOffsetsPerField == UncompiledNumRequiredOffsetsPerField::ONE)
    {
        this->indexValues.emplace_back(offset);
    }

    /// Ensures (vs std::pair) that values lie consecutively in memory
    struct __attribute__((packed)) IndexPairs
    {
        UncompiledFieldIndex fieldValueStart;
        UncompiledFieldIndex fieldValueEnd;
    };

    std::span<IndexPairs> emplaceTupleOffsets(const size_t numberOfFieldsInSchema)
    requires(NumOffsetsPerField == UncompiledNumRequiredOffsetsPerField::TWO)
    {
        const size_t numberOfRequiredOffsets = numberOfFieldsInSchema * 2;
        const size_t startIdx = indexValues.size();
        indexValues.resize(startIdx + numberOfRequiredOffsets);
        auto fieldIndexPairSpan = std::span(indexValues).subspan(startIdx, numberOfRequiredOffsets);
        return std::span(std::bit_cast<IndexPairs*>(fieldIndexPairSpan.data()), fieldIndexPairSpan.size());
    }

    /// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
    void markNoTupleDelimiters()
    {
        this->offsetOfFirstTuple = std::numeric_limits<UncompiledFieldIndex>::max();
        this->offsetOfLastTuple = std::numeric_limits<UncompiledFieldIndex>::max();
    }

    [[nodiscard]] size_t getNumberOfTuples() const
    requires(NumOffsetsPerField == UncompiledNumRequiredOffsetsPerField::ONE)
    {
        return indexValues.size();
    }

    [[nodiscard]] size_t getNumberOfTuples() const
    requires(NumOffsetsPerField == UncompiledNumRequiredOffsetsPerField::TWO)
    {
        return indexValues.size() / 2;
    }

    void markWithTupleDelimiters(UncompiledFieldIndex offsetToFirstTuple, UncompiledFieldIndex offsetToLastTuple)
    {
        /// Make sure that the number of read fields matches the expected value.
        const auto [totalNumberOfTuples, remainder] = std::lldiv(getNumberOfTuples(), numberOfOffsetsPerTuple);
        INVARIANT(
            remainder == 0, "Number of indexes {} must be a multiple of number of fields in tuple {}", remainder, numberOfOffsetsPerTuple);
        /// Finalize the state of the UncompiledFieldOffsets object
        this->totalNumberOfTuples = totalNumberOfTuples;
        this->offsetOfFirstTuple = offsetToFirstTuple;
        this->offsetOfLastTuple = offsetToLastTuple;
    }

private:
    UncompiledFieldIndex sizeOfFieldDelimiter{};
    size_t numberOfFieldsInSchema{};
    size_t numberOfOffsetsPerTuple{};
    size_t totalNumberOfTuples{};
    UncompiledFieldIndex offsetOfFirstTuple{};
    UncompiledFieldIndex offsetOfLastTuple{};
    std::vector<UncompiledFieldIndex> indexValues;
};

}
