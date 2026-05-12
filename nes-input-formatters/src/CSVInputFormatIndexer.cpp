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
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <FieldOffsetRawBufferIndex.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatter.hpp>
#include <InputFormatterValidationRegistry.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>

namespace
{


void initializeIndexFunctionForTupleWithCommasInStrings(
    NES::FieldOffsetRawBufferIndex& fieldOffsetRawBufferIndex,
    const std::string_view tuple,
    const NES::FieldIndex startIdxOfTuple,
    const char fieldDelimiter,
    const size_t numberOfFields)
{
    /// The start of the tuple is the offset of the first field of the tuple
    fieldOffsetRawBufferIndex.emplaceFieldOffset(startIdxOfTuple);
    size_t fieldIdx = 1;

    bool isQuoted = false;
    for (const auto [i, character] : tuple | NES::views::enumerate)
    {
        if (not isQuoted and character == fieldDelimiter)
        {
            constexpr size_t sizeOfFieldDelimiter = 1;
            fieldOffsetRawBufferIndex.emplaceFieldOffset(startIdxOfTuple + i + sizeOfFieldDelimiter);
            ++fieldIdx;
        }
        isQuoted = isQuoted xor (character == '"');
    }

    /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
    fieldOffsetRawBufferIndex.emplaceFieldOffset(startIdxOfTuple + tuple.size());
    if (fieldIdx != numberOfFields)
    {
        throw NES::CannotFormatSourceData(
            "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema", fieldIdx, numberOfFields);
    }
}

void initializeIndexFunctionForTuple(
    NES::FieldOffsetRawBufferIndex& fieldOffsetRawBufferIndex,
    const std::string_view tuple,
    const NES::FieldIndex startIdxOfTuple,
    const char fieldDelimiter,
    const size_t numberOfFields)
{
    /// The start of the tuple is the offset of the first field of the tuple
    fieldOffsetRawBufferIndex.emplaceFieldOffset(startIdxOfTuple);
    size_t fieldIdx = 1;
    /// Find field delimiters, until reaching the end of the tuple
    /// The position of the field delimiter (+ size of field delimiter) is the beginning of the next field
    for (size_t nextFieldOffset = tuple.find(fieldDelimiter, 0); nextFieldOffset != std::string_view::npos;
         nextFieldOffset = tuple.find(fieldDelimiter, nextFieldOffset))
    {
        nextFieldOffset += NES::CSVInputFormatIndexer::SIZE_OF_FIELD_DELIMITER;
        fieldOffsetRawBufferIndex.emplaceFieldOffset(startIdxOfTuple + nextFieldOffset);
        ++fieldIdx;
    }
    /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
    fieldOffsetRawBufferIndex.emplaceFieldOffset(startIdxOfTuple + tuple.size());
    if (fieldIdx != numberOfFields)
    {
        throw NES::CannotFormatSourceData(
            "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema", fieldIdx, numberOfFields);
    }
}
}

namespace NES
{

std::unique_ptr<RawBufferIndex> CSVInputFormatIndexer::indexRawBuffer(const std::string_view rawBuffer) const
{
    auto fieldOffsets = std::make_unique<FieldOffsetRawBufferIndex>();

    fieldOffsets->startSetup(this->numberOfFields);

    const auto offsetOfFirstTupleDelimiter = static_cast<FieldIndex>(rawBuffer.find(this->tupleDelimiter));

    /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTupleDelimiter' to a value larger than the buffer size to tell
    /// the InputFormatIndexerTask that there was no tuple delimiter in the buffer and return
    if (offsetOfFirstTupleDelimiter == static_cast<FieldIndex>(std::string::npos))
    {
        fieldOffsets->markNoTupleDelimiters();
        return fieldOffsets;
    }

    /// If the buffer contains at least one delimiter, check if it contains more and index all tuples between the tuple delimiters
    auto startIdxOfNextTuple = offsetOfFirstTupleDelimiter + SIZE_OF_TUPLE_DELIMITER;
    size_t endIdxOfNextTuple = rawBuffer.find(this->tupleDelimiter, startIdxOfNextTuple);

    while (endIdxOfNextTuple != std::string::npos)
    {
        /// Get a string_view for the next tuple, by using the start and the size of the next tuple
        INVARIANT(startIdxOfNextTuple <= endIdxOfNextTuple, "The start index of a tuple cannot be larger than the end index.");
        const auto sizeOfNextTuple = endIdxOfNextTuple - startIdxOfNextTuple;
        const auto nextTuple = rawBuffer.substr(startIdxOfNextTuple, sizeOfNextTuple);

        /// Determine the offsets to the individual fields of the next tuple, including the start of the first and the end of the last field
        if (this->allowCommasInStrings)
        {
            initializeIndexFunctionForTupleWithCommasInStrings(
                *fieldOffsets, nextTuple, startIdxOfNextTuple, this->fieldDelimiter, this->numberOfFields);
        }
        else
        {
            initializeIndexFunctionForTuple(*fieldOffsets, nextTuple, startIdxOfNextTuple, this->fieldDelimiter, this->numberOfFields);
        }

        /// Update the start and the end index for the next tuple (if no more tuples in buffer, endIdx is 'std::string::npos')
        startIdxOfNextTuple = endIdxOfNextTuple + SIZE_OF_TUPLE_DELIMITER;
        endIdxOfNextTuple = rawBuffer.find(this->tupleDelimiter, startIdxOfNextTuple);
    }
    /// Since 'endIdxOfNextTuple == std::string::npos', we use the startIdx to determine the offset of the last tuple
    const auto offsetOfLastTupleDelimiter = static_cast<FieldIndex>(startIdxOfNextTuple - SIZE_OF_TUPLE_DELIMITER);
    fieldOffsets->markWithTupleDelimiters(offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter);
    return fieldOffsets;
}

DescriptorConfig::Config CSVInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCSVInputFormatIndexer>(std::move(config), NAME);
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterCSVInputFormatterValidation(InputFormatterValidationRegistryArguments arguments)
{
    return CSVInputFormatIndexer::validateAndFormat(arguments.config);
}

InputFormatIndexerRegistryReturnType
RegisterCSVInputFormatIndexer(InputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createInputFormatterWithIndexer(
        CSVInputFormatIndexer::create(arguments.getInputFormatterConfig(), arguments.getInputMemoryProvider()));
}

std::ostream& CSVInputFormatIndexer::toString(std::ostream& str) const
{
    return str << fmt::format("CSVInputFormatIndexer()");
}
}
