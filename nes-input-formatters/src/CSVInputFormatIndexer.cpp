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

#include <CSVInputFormatIndexer.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

#include <InputFormatters/InputFormatIndexer.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatterTask.hpp>

namespace
{

void initializeIndexFunctionForTuple(
    NES::InputFormatters::FieldOffsets<NES::InputFormatters::CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
    const std::string_view tuple,
    const NES::InputFormatters::FieldOffsetsType startIdxOfTuple,
    const NES::ParserConfig& config,
    size_t numberOfFieldsInSchema)
{
    /// The start of the tuple is the offset of the first field of the tuple
    size_t fieldIdx = 0;
    fieldOffsets.writeOffsetAt(startIdxOfTuple, fieldIdx++);
    /// Find field delimiters, until reaching the end of the tuple
    /// The position of the field delimiter (+ size of field delimiter) is the beginning of the next field
    for (size_t nextFieldOffset = tuple.find(config.fieldDelimiter, 0); nextFieldOffset != std::string_view::npos;
         nextFieldOffset = tuple.find(config.fieldDelimiter, nextFieldOffset))
    {
        nextFieldOffset += config.fieldDelimiter.size();
        fieldOffsets.writeOffsetAt(startIdxOfTuple + nextFieldOffset, fieldIdx); ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        ++fieldIdx;
    }
    /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
    fieldOffsets.writeOffsetAt(startIdxOfTuple + tuple.size(), numberOfFieldsInSchema);
    if (fieldIdx != numberOfFieldsInSchema)
    {
        throw NES::CannotFormatSourceData(
            "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema", fieldIdx, numberOfFieldsInSchema);
    }
}
}

namespace NES::InputFormatters
{

CSVInputFormatIndexer::CSVInputFormatIndexer(ParserConfig config, const size_t numberOfFieldsInSchema)
    : config(std::move(config)), numberOfFieldsInSchema(numberOfFieldsInSchema)
{
}

void CSVInputFormatIndexer::indexRawBuffer(
    FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const CSVMetaData&) const
{
    fieldOffsets.startSetup(numberOfFieldsInSchema, this->config.fieldDelimiter.size());

    const auto sizeOfTupleDelimiter = this->config.tupleDelimiter.size();
    const auto offsetOfFirstTupleDelimiter = static_cast<FieldOffsetsType>(rawBuffer.getBufferView().find(this->config.tupleDelimiter));

    /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTupleDelimiter' to a value larger than the buffer size to tell
    /// the InputFormatIndexerTask that there was no tuple delimiter in the buffer and return
    if (offsetOfFirstTupleDelimiter == static_cast<FieldOffsetsType>(std::string::npos))
    {
        fieldOffsets.markNoTupleDelimiters();
        return;
    }

    /// If the buffer contains at least one delimiter, check if it contains more and index all tuples between the tuple delimiters
    auto startIdxOfNextTuple = offsetOfFirstTupleDelimiter + sizeOfTupleDelimiter;
    size_t endIdxOfNextTuple = rawBuffer.getBufferView().find(this->config.tupleDelimiter, startIdxOfNextTuple);

    while (endIdxOfNextTuple != std::string::npos)
    {
        /// Get a string_view for the next tuple, by using the start and the size of the next tuple
        INVARIANT(startIdxOfNextTuple <= endIdxOfNextTuple, "The start index of a tuple cannot be larger than the end index.");
        const auto sizeOfNextTuple = endIdxOfNextTuple - startIdxOfNextTuple;
        const auto nextTuple = rawBuffer.getBufferView().substr(startIdxOfNextTuple, sizeOfNextTuple);

        /// Determine the offsets to the individual fields of the next tuple, including the start of the first and the end of the last field
        initializeIndexFunctionForTuple(fieldOffsets, nextTuple, startIdxOfNextTuple, this->config, this->numberOfFieldsInSchema);
        fieldOffsets.writeOffsetsOfNextTuple(); ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)

        /// Update the start and the end index for the next tuple (if no more tuples in buffer, endIdx is 'std::string::npos')
        startIdxOfNextTuple = endIdxOfNextTuple + sizeOfTupleDelimiter;
        endIdxOfNextTuple = rawBuffer.getBufferView().find(this->config.tupleDelimiter, startIdxOfNextTuple);
    }
    /// Since 'endIdxOfNextTuple == std::string::npos', we use the startIdx to determine the offset of the last tuple
    const auto offsetOfLastTupleDelimiter = static_cast<FieldOffsetsType>(startIdxOfNextTuple - sizeOfTupleDelimiter);
    fieldOffsets.markWithTupleDelimiters(offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter);
}

std::ostream& CSVInputFormatIndexer::toString(std::ostream& os) const
{
    return os << fmt::format(
               "CSVInputFormatIndexer(tupleDelimiter: {}, fieldDelimiter: {})", this->config.tupleDelimiter, this->config.fieldDelimiter);
}

InputFormatIndexerRegistryReturnType InputFormatIndexerGeneratedRegistrar::RegisterCSVInputFormatIndexer(
    InputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    auto inputFormatter
        = std::make_unique<CSVInputFormatIndexer>(arguments.inputFormatIndexerConfig, arguments.getNumberOfFieldsInSchema());
    return arguments.createInputFormatterTaskPipeline<CSVInputFormatIndexer, FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>, CSVMetaData, true>(
        std::move(inputFormatter), RawValueParser::QuotationType::NONE);
}

}
