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
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <CSVInputFormatter.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatterRegistry.hpp>

namespace NES::InputFormatters
{

CSVInputFormatter::CSVInputFormatter(InputFormatterRegistryArguments args)
    : config(std::move(args.inputFormatterConfig)), numberOfFieldsInSchema(args.numberOfFieldsInSchema)
{
}

void CSVInputFormatter::indexTuple(
    const std::string_view tuple, FieldOffsetsType* fieldOffsets, const FieldOffsetsType startIdxOfTuple) const
{
    PRECONDITION(fieldOffsets != nullptr, "FieldOffsets cannot be null.");

    /// The start of the tuple is the offset of the first field of the tuple
    *fieldOffsets = startIdxOfTuple;
    size_t fieldIdx = 1;
    /// Find field delimiters, until reaching the end of the tuple
    /// The position of the field delimiter (+ size of field delimiter) is the beginning of the next field
    for (size_t nextFieldOffset = tuple.find(this->config.fieldDelimiter, 0); nextFieldOffset != std::string_view::npos;
         nextFieldOffset = tuple.find(this->config.fieldDelimiter, nextFieldOffset))
    {
        nextFieldOffset += this->config.fieldDelimiter.size();
        *(fieldOffsets + fieldIdx) = startIdxOfTuple + nextFieldOffset;
        ++fieldIdx;
    }
    if (fieldIdx != numberOfFieldsInSchema)
    {
        throw FormattingError(
            "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema", fieldIdx, numberOfFieldsInSchema);
    }
}

InputFormatter::FirstAndLastTupleDelimiterOffsets
CSVInputFormatter::indexBuffer(std::string_view bufferView, FieldOffsets& fieldOffsets) const
{
    const auto sizeOfTupleDelimiter = this->config.tupleDelimiter.size();
    const auto offsetOfFirstTupleDelimiter = static_cast<FieldOffsetsType>(bufferView.find(this->config.tupleDelimiter));

    /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTupleDelimiter' to a value larger than the buffer size to tell
    /// the InputFormatterTask that there was no tuple delimiter in the buffer and return
    if (offsetOfFirstTupleDelimiter == static_cast<FieldOffsetsType>(std::string::npos))
    {
        return {
            .offsetOfFirstTupleDelimiter = std::numeric_limits<FieldOffsetsType>::max(),
            .offsetOfLastTupleDelimiter = std::numeric_limits<FieldOffsetsType>::max()};
    }

    /// If the buffer contains at least one delimiter, check if it contains more and index all tuples between the tuple delimiters
    size_t startIdxOfCurrentTuple = offsetOfFirstTupleDelimiter + sizeOfTupleDelimiter;
    size_t endIdxOfCurrentTuple = bufferView.find(this->config.tupleDelimiter, startIdxOfCurrentTuple);
    while (endIdxOfCurrentTuple != std::string::npos)
    {
        INVARIANT(startIdxOfCurrentTuple <= endIdxOfCurrentTuple, "The start index of a tuple cannot be larger than the end index.");
        auto* tupleOffsetPtr = fieldOffsets.writeOffsetsOfNextTuple();
        const auto sizeOfCurrentTuple = endIdxOfCurrentTuple - startIdxOfCurrentTuple;
        /// WE ALWAYS skip the first partial tuple and start with the first full tuple delimiter, thus, we can ALWAYS add the size of the tuple delimiter
        const auto currentTuple = std::string_view(bufferView.begin() + startIdxOfCurrentTuple, sizeOfCurrentTuple);

        indexTuple(currentTuple, tupleOffsetPtr, startIdxOfCurrentTuple);
        /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
        *(tupleOffsetPtr + numberOfFieldsInSchema) = endIdxOfCurrentTuple;

        startIdxOfCurrentTuple = endIdxOfCurrentTuple + sizeOfTupleDelimiter;
        endIdxOfCurrentTuple = bufferView.find(this->config.tupleDelimiter, startIdxOfCurrentTuple);
    }
    const auto offsetOfLastTupleDelimiter = static_cast<FieldOffsetsType>(startIdxOfCurrentTuple - sizeOfTupleDelimiter);
    return {.offsetOfFirstTupleDelimiter = offsetOfFirstTupleDelimiter, .offsetOfLastTupleDelimiter = offsetOfLastTupleDelimiter};
}

std::ostream& CSVInputFormatter::toString(std::ostream& os) const
{
    return os << fmt::format(
               "CSVInputFormatter(tupleDelimiter: {}, fieldDelimiter: {})", this->config.tupleDelimiter, this->config.fieldDelimiter);
}

InputFormatterRegistryReturnType
InputFormatterGeneratedRegistrar::RegisterCSVInputFormatter(const InputFormatterRegistryArguments& arguments)
{
    return std::make_unique<CSVInputFormatter>(arguments);
}

}
