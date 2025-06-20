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

#include <JSONInputFormatIndexer.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <InputFormatters/InputFormatIndexer.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatterTask.hpp>
#include <RawValueParser.hpp>

namespace
{
void setupFieldAccessFunctionForTuple(
    NES::InputFormatters::FieldOffsets<NES::InputFormatters::JSON_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
    const std::string_view tuple,
    const NES::InputFormatters::FieldOffsetsType startIdxOfTuple,
    const size_t numberOfFieldsInSchema)
{
    using JSONFormatter = NES::InputFormatters::JSONInputFormatter;
    size_t fieldIdx = 0;
    const auto endIdxOfTuple = startIdxOfTuple + tuple.size();
    size_t nextFieldStart = tuple.find(JSONFormatter::KEY_VALUE_DELIMITER, 0) + 1;
    for (size_t nextFieldEnd = tuple.find(JSONFormatter::FIELD_DELIMITER, 0); nextFieldEnd != std::string_view::npos;
         nextFieldEnd = tuple.find(JSONFormatter::FIELD_DELIMITER, nextFieldStart))
    {
        const NES::InputFormatters::FieldOffsetsType startOfFieldInBuffer = startIdxOfTuple + nextFieldStart;
        const NES::InputFormatters::FieldOffsetsType endOfFieldInBuffer = startIdxOfTuple + nextFieldEnd;
        fieldOffsets.writeOffsetAt({startOfFieldInBuffer, endOfFieldInBuffer}, fieldIdx);
        ++fieldIdx;
        nextFieldStart = tuple.find(JSONFormatter::KEY_VALUE_DELIMITER, nextFieldEnd) + 1;
    }
    /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
    const NES::InputFormatters::FieldOffsetsType startOfLastFieldInBuffer = startIdxOfTuple + nextFieldStart;
    const NES::InputFormatters::FieldOffsetsType endOfLastFieldInBuffer = endIdxOfTuple - 1;
    fieldOffsets.writeOffsetAt({startOfLastFieldInBuffer, endOfLastFieldInBuffer}, fieldIdx);
    if (fieldIdx + 1 != numberOfFieldsInSchema)
    {
        throw NES::FormattingError(
            "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema", fieldIdx, numberOfFieldsInSchema);
    }
}
}

namespace NES::InputFormatters
{

JSONInputFormatter::JSONInputFormatter(const ParserConfig&, const size_t numberOfFieldsInSchema)
    : numberOfFieldsInSchema(numberOfFieldsInSchema)
{
}

void JSONInputFormatter::indexRawBuffer(
    FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const JSONMetaData&) const
{
    fieldOffsets.startSetup(numberOfFieldsInSchema, FIELD_DELIMITER);

    const auto offsetOfFirstTupleDelimiter = static_cast<FieldOffsetsType>(rawBuffer.getBufferView().find(TUPLE_DELIMITER));

    /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTupleDelimiter' to a value larger than the buffer size to tell
    /// the InputFormatterTask that there was no tuple delimiter in the buffer and return
    if (offsetOfFirstTupleDelimiter == static_cast<FieldOffsetsType>(std::string::npos))
    {
        fieldOffsets.markNoTupleDelimiters();
        return;
    }

    /// If the buffer contains at least one delimiter, check if it contains more and index all tuples between the tuple delimiters
    auto startIdxOfNextTuple = offsetOfFirstTupleDelimiter + DELIMITER_SIZE;
    size_t endIdxOfNextTuple = rawBuffer.getBufferView().find(TUPLE_DELIMITER, startIdxOfNextTuple);

    while (endIdxOfNextTuple != std::string::npos)
    {
        /// Get a string_view for the next tuple, by using the start and the size of the next tuple
        INVARIANT(startIdxOfNextTuple <= endIdxOfNextTuple, "The start index of a tuple cannot be larger than the end index.");
        const auto sizeOfNextTuple = endIdxOfNextTuple - startIdxOfNextTuple;
        const auto nextTuple = std::string_view(rawBuffer.getBufferView().begin() + startIdxOfNextTuple, sizeOfNextTuple);

        /// Determine the offsets to the individual fields of the next tuple, including the start of the first and the end of the last field
        setupFieldAccessFunctionForTuple(fieldOffsets, nextTuple, startIdxOfNextTuple, this->numberOfFieldsInSchema);
        fieldOffsets.writeOffsetsOfNextTuple();

        /// Update the start and the end index for the next tuple (if no more tuples in buffer, endIdx is 'std::string::npos')
        startIdxOfNextTuple = endIdxOfNextTuple + DELIMITER_SIZE;
        endIdxOfNextTuple = rawBuffer.getBufferView().find(TUPLE_DELIMITER, startIdxOfNextTuple);
    }
    /// Since 'endIdxOfNextTuple == std::string::npos', we use the startIdx to determine the offset of the last tuple
    const auto offsetOfLastTupleDelimiter = startIdxOfNextTuple - DELIMITER_SIZE;
    fieldOffsets.markWithTupleDelimiters(offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter);
}

std::ostream& JSONInputFormatter::toString(std::ostream& os) const
{
    return os << fmt::format("JSONInputFormatter(tupleDelimiter: {}, fieldDelimiter: {})", TUPLE_DELIMITER, FIELD_DELIMITER);
}


NES::Configurations::DescriptorConfig::Config JSONInputFormatter::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersJSON>(std::move(config), NAME);
}

InputFormatIndexerRegistryReturnType
InputFormatIndexerGeneratedRegistrar::RegisterJSONInputFormatIndexer(InputFormatIndexerRegistryArguments arguments)
{
    auto inputFormatter = std::make_unique<JSONInputFormatter>(arguments.inputFormatIndexerConfig, arguments.getNumberOfFieldsInSchema());
    return arguments.createInputFormatterTaskPipeline<JSONInputFormatter, FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>, JSONMetaData, true>(
        std::move(inputFormatter), RawValueParser::QuotationType::DOUBLE_QUOTE);
}

}
