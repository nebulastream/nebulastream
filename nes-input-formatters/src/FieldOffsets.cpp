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

#include <FieldOffsets.hpp>

#include <cstddef>
#include <limits>
#include <string_view>

#include <Runtime/AbstractBufferProvider.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterTask.hpp>

namespace NES::InputFormatters
{

void FieldOffsets::startSetup(const size_t numberOfFieldsInSchema, const size_t sizeOfFieldDelimiter)
{
    PRECONDITION(
            sizeOfFieldDelimiter <= std::numeric_limits<FieldIndex>::max(),
            "Size of tuple delimiter must be smaller than: {}",
            std::numeric_limits<FieldIndex>::max());
    this->sizeOfFieldDelimiter = static_cast<FieldIndex>(sizeOfFieldDelimiter);
    this->numberOfFieldsInSchema = numberOfFieldsInSchema;
    this->numberOfOffsetsPerTuple = this->numberOfFieldsInSchema + 1;
    this->totalNumberOfTuples = 0;
}

FieldIndex FieldOffsets::applyGetOffsetOfFirstTupleDelimiter() const
{
    return this->offsetOfFirstTuple;
}

FieldIndex FieldOffsets::applyGetOffsetOfLastTupleDelimiter() const
{
    return this->offsetOfLastTuple;
}

size_t FieldOffsets::applyGetTotalNumberOfTuples() const
{
    return this->totalNumberOfTuples;
}

void FieldOffsets::writeOffsetAt(const FieldIndex offset, const FieldIndex)
{
    this->indexValues.emplace_back(offset);
}

void FieldOffsets::markNoTupleDelimiters()
{
    this->offsetOfFirstTuple = std::numeric_limits<FieldIndex>::max();
    this->offsetOfLastTuple = std::numeric_limits<FieldIndex>::max();
}

void FieldOffsets::markWithTupleDelimiters(const FieldIndex offsetToFirstTuple, const FieldIndex offsetToLastTuple)
{
    /// Make sure that the number of read fields matches the expected value.
    const auto finalIndex = indexValues.size();
    const auto [totalNumberOfTuples, remainder] = std::lldiv(finalIndex, numberOfOffsetsPerTuple);
    if (remainder != 0)
    {
        throw FormattingError(
            "Number of indexes {} must be a multiple of number of fields in tuple {}", finalIndex, (numberOfOffsetsPerTuple));
    }
    /// Finalize the state of the FieldOffsets object
    this->totalNumberOfTuples = totalNumberOfTuples;
    this->offsetOfFirstTuple = offsetToFirstTuple;
    this->offsetOfLastTuple = offsetToLastTuple;
}


}
