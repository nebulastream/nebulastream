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
#include <CSVInputFormatter.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatterRegistry.hpp>
#include <InputFormatterTask.hpp>
#include <InputFormatterValidationRegistry.hpp>

namespace
{

void setupFieldAccessFunctionForTuple(
    NES::InputFormatters::FieldOffsets<NES::InputFormatters::CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
    const std::string_view tuple,
    const NES::InputFormatters::FieldOffsetsType startIdxOfTuple,
    const std::string_view fieldDelimiter,
    size_t numberOfFieldsInSchema)
{
    /// The start of the tuple is the offset of the first field of the tuple
    size_t fieldIdx = 0;
    fieldOffsets.writeOffsetAt(startIdxOfTuple, fieldIdx++);
    /// Find field delimiters, until reaching the end of the tuple
    /// The position of the field delimiter (+ size of field delimiter) is the beginning of the next field
    for (size_t nextFieldOffset = tuple.find(fieldDelimiter, 0); nextFieldOffset != std::string_view::npos;
         nextFieldOffset = tuple.find(fieldDelimiter, nextFieldOffset))
    {
        nextFieldOffset += fieldDelimiter.size();
        fieldOffsets.writeOffsetAt(startIdxOfTuple + nextFieldOffset, fieldIdx);
        ++fieldIdx;
    }
    /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
    fieldOffsets.writeOffsetAt(startIdxOfTuple + tuple.size(), numberOfFieldsInSchema);
    if (fieldIdx != numberOfFieldsInSchema)
    {
        throw NES::FormattingError(
            "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema", fieldIdx, numberOfFieldsInSchema);
    }
}
}

namespace NES::InputFormatters
{

CSVInputFormatter::CSVInputFormatter(const InputFormatterDescriptor& descriptor, const size_t numberOfFieldsInSchema)
    : tupleDelimiter(descriptor.getFromConfig(ConfigParametersCSV::TUPLE_DELIMITER))
    , fieldDelimiter(descriptor.getFromConfig(ConfigParametersCSV::FIELD_DELIMITER))
    , numberOfFieldsInSchema(numberOfFieldsInSchema)
{
}

void CSVInputFormatter::setupFieldAccessFunctionForBuffer(
    FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const TupleMetaData&) const
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

std::ostream& CSVInputFormatter::toString(std::ostream& os) const
{
    return os << fmt::format("CSVInputFormatter(tupleDelimiter: {}, fieldDelimiter: {})", this->tupleDelimiter, this->fieldDelimiter);
}


NES::Configurations::DescriptorConfig::Config CSVInputFormatter::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
}

InputFormatterValidationRegistryReturnType
NES::InputFormatters::InputFormatterValidationGeneratedRegistrar::RegisterCSVInputFormatterValidation(
    InputFormatterValidationRegistryArguments inputFormatterConfig)
{
    return CSVInputFormatter::validateAndFormat(std::move(inputFormatterConfig.config));
}

InputFormatterRegistryReturnType InputFormatterGeneratedRegistrar::RegisterCSVInputFormatter(InputFormatterRegistryArguments arguments)
{
    constexpr bool hasSpanningTuples = true;
    const auto tupleDelimiter = arguments.inputFormatterConfig.getFromConfig(ConfigParametersCSV::TUPLE_DELIMITER);
    PRECONDITION(
        arguments.inputFormatterConfig.getHasSpanningTuples() == hasSpanningTuples,
        "The CSVInputFormatter does not support parsing buffers without spanning tuples.");
    auto inputFormatter = std::make_unique<CSVInputFormatter>(arguments.inputFormatterConfig, arguments.numberOfFieldsInSchema);
    return arguments.createInputFormatterTaskPipeline<CSVInputFormatter, FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>, hasSpanningTuples>(
        std::move(inputFormatter), tupleDelimiter);
}

}
