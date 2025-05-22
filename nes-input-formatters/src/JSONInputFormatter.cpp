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
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterDescriptor.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <JSONInputFormatter.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatterRegistry.hpp>
#include <InputFormatterTask.hpp>
#include <InputFormatterValidationRegistry.hpp>

namespace
{
void setupFieldAccessFunctionForTuple(
    NES::InputFormatters::FieldOffsets<NES::InputFormatters::JSON_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
    const std::string_view tuple,
    const NES::InputFormatters::FieldOffsetsType startIdxOfTuple,
    const std::string_view,
    const size_t numberOfFieldsInSchema)
{
    // Todo:
    // static constexpr std::string_view input = R"({"name":"John","age":30}
    //                                 {"name":"Alice","active":false}
    //                                 {"id":123,"status":"pending","null":null})";
    // Todo: Iterate over number of fields. handle last field differently
    size_t fieldIdx = 0;
    const auto endIdxOfTuple = startIdxOfTuple + tuple.size();
    size_t nextFieldStart = tuple.find(':', 0) + 1; // size of ':' //fieldDelimiter.size()
    for (size_t nextFieldEnd = tuple.find(',', 0); nextFieldEnd != std::string_view::npos;
         nextFieldEnd = tuple.find(',', nextFieldStart))
    {
        // write start and end of field into field offsets
        const auto startOfFieldInBuffer = startIdxOfTuple + nextFieldStart;
        const auto endOfFieldInBuffer = startIdxOfTuple + nextFieldEnd;
        fieldOffsets.writeOffsetAt({startOfFieldInBuffer, endOfFieldInBuffer}, fieldIdx);
        ++fieldIdx;
        nextFieldStart = tuple.find(':', nextFieldEnd) + 1; // size of ':' //fieldDelimiter.size()
    }
    /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
    // Todo: actually assume not (+1 numberOfFields in FiledOffsets)
    const auto startOfLastFieldInBuffer = startIdxOfTuple + nextFieldStart;
    const auto endOfLastFieldInBuffer = endIdxOfTuple - 1;
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

JSONInputFormatter::JSONInputFormatter(const InputFormatterDescriptor& descriptor, const size_t numberOfFieldsInSchema)
    : tupleDelimiter(descriptor.getFromConfig(ConfigParametersJSON::TUPLE_DELIMITER))
    , fieldDelimiter(descriptor.getFromConfig(ConfigParametersJSON::FIELD_DELIMITER))
    , numberOfFieldsInSchema(numberOfFieldsInSchema)
{
}

void JSONInputFormatter::setupFieldAccessFunctionForBuffer(
    FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const TupleMetaData&) const
{
    fieldOffsets.startSetup(numberOfFieldsInSchema, this->fieldDelimiter.size());

    const auto sizeOfTupleDelimiter = this->tupleDelimiter.size();
    const auto offsetOfFirstTupleDelimiter = static_cast<FieldOffsetsType>(rawBuffer.getBufferView().find(this->tupleDelimiter));

    /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTupleDelimiter' to a value larger than the buffer size to tell
    /// the InputFormatterTask that there was no tuple delimiter in the buffer and return
    if (offsetOfFirstTupleDelimiter == static_cast<FieldOffsetsType>(std::string::npos))
    {
        fieldOffsets.finishSetup<false>(std::numeric_limits<FieldOffsetsType>::max(), std::numeric_limits<FieldOffsetsType>::max());
        return;
    }

    /// If the buffer contains at least one delimiter, check if it contains more and index all tuples between the tuple delimiters
    auto startIdxOfNextTuple = offsetOfFirstTupleDelimiter + sizeOfTupleDelimiter;
    size_t endIdxOfNextTuple = rawBuffer.getBufferView().find(this->tupleDelimiter, startIdxOfNextTuple);

    while (endIdxOfNextTuple != std::string::npos)
    {
        /// Get a string_view for the next tuple, by using the start and the size of the next tuple
        INVARIANT(startIdxOfNextTuple <= endIdxOfNextTuple, "The start index of a tuple cannot be larger than the end index.");
        const auto sizeOfNextTuple = endIdxOfNextTuple - startIdxOfNextTuple;
        const auto nextTuple = std::string_view(rawBuffer.getBufferView().begin() + startIdxOfNextTuple, sizeOfNextTuple);

        /// Determine the offsets to the individual fields of the next tuple, including the start of the first and the end of the last field
        setupFieldAccessFunctionForTuple(fieldOffsets, nextTuple, startIdxOfNextTuple, this->fieldDelimiter, this->numberOfFieldsInSchema);
        fieldOffsets.writeOffsetsOfNextTuple();

        /// Update the start and the end index for the next tuple (if no more tuples in buffer, endIdx is 'std::string::npos')
        startIdxOfNextTuple = endIdxOfNextTuple + sizeOfTupleDelimiter;
        endIdxOfNextTuple = rawBuffer.getBufferView().find(this->tupleDelimiter, startIdxOfNextTuple);
    }
    /// Since 'endIdxOfNextTuple == std::string::npos', we use the startIdx to determine the offset of the last tuple
    const auto offsetOfLastTupleDelimiter = static_cast<FieldOffsetsType>(startIdxOfNextTuple - sizeOfTupleDelimiter);
    fieldOffsets.finishSetup<true>(offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter);
}

std::ostream& JSONInputFormatter::toString(std::ostream& os) const
{
    return os << fmt::format("JSONInputFormatter(tupleDelimiter: {}, fieldDelimiter: {})", this->tupleDelimiter, this->fieldDelimiter);
}


NES::Configurations::DescriptorConfig::Config JSONInputFormatter::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersJSON>(std::move(config), NAME);
}

InputFormatterValidationRegistryReturnType
NES::InputFormatters::InputFormatterValidationGeneratedRegistrar::RegisterJSONInputFormatterValidation(
    InputFormatterValidationRegistryArguments inputFormatterConfig)
{
    return JSONInputFormatter::validateAndFormat(std::move(inputFormatterConfig.config));
}

InputFormatterRegistryReturnType InputFormatterGeneratedRegistrar::RegisterJSONInputFormatter(InputFormatterRegistryArguments arguments)
{
    constexpr bool hasSpanningTuples = true;
    const auto tupleDelimiter = arguments.inputFormatterConfig.getFromConfig(ConfigParametersJSON::TUPLE_DELIMITER);
    PRECONDITION(
        arguments.inputFormatterConfig.getHasSpanningTuples() == hasSpanningTuples,
        "The JSONInputFormatter does not support parsing buffers without spanning tuples.");
    auto inputFormatter = std::make_unique<JSONInputFormatter>(arguments.inputFormatterConfig, arguments.numberOfFieldsInSchema);
    return arguments.createInputFormatterTaskPipeline<JSONInputFormatter, FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>, hasSpanningTuples>(
        std::move(inputFormatter), QuotationType::DOUBLE_QUOTE, tupleDelimiter);
}

}
