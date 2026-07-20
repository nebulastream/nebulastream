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

#include <SequentialUncompiledOldCSVInputFormatIndexer.hpp>

#include <cstddef>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

#include <Sources/SourceDescriptor.hpp>
#include <UncompiledInputFormatters/UncompiledInputFormatterTaskPipeline.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <InputFormatterValidationRegistry.hpp>
#include <UncompiledFieldOffsets.hpp>
#include <UncompiledInputFormatIndexerRegistry.hpp>
#include <UncompiledInputFormatterTask.hpp>

namespace
{
using OldCsvFieldOffsets = NES::UncompiledFieldOffsets<NES::UncompiledNumRequiredOffsetsPerField::ONE>;

void checkFieldCount(const size_t fieldIdx, const size_t numberOfFieldsInSchema)
{
    if (fieldIdx != numberOfFieldsInSchema)
    {
        throw NES::CannotFormatSourceData(
            "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema", fieldIdx, numberOfFieldsInSchema);
    }
}

/// Plain scalar per-tuple field scan (`allow_commas_in_strings == false`): `find` the next field
/// delimiter, emit the offset just past it. The closing offset is the tuple's end, which lets the parse
/// phase size the last field without a special case. This is the shape the SIMD band replaced -- one
/// branchy `find` per field versus one branchless 64-byte block per 8 delimiters.
void initializeIndexFunctionForTuple(
    OldCsvFieldOffsets& fieldOffsets,
    const std::string_view tuple,
    const NES::UncompiledFieldIndex startIdxOfTuple,
    const char fieldDelimiter,
    const size_t numberOfFieldsInSchema)
{
    /// The start of the tuple is the offset of the first field of the tuple
    fieldOffsets.emplaceFieldOffset(startIdxOfTuple);
    size_t fieldIdx = 1;
    /// Find field delimiters, until reaching the end of the tuple
    /// The position of the field delimiter (+ size of field delimiter) is the beginning of the next field
    for (size_t nextFieldOffset = tuple.find(fieldDelimiter, 0); nextFieldOffset != std::string_view::npos;
         nextFieldOffset = tuple.find(fieldDelimiter, nextFieldOffset))
    {
        constexpr size_t sizeOfFieldDelimiter = 1;
        nextFieldOffset += sizeOfFieldDelimiter;
        fieldOffsets.emplaceFieldOffset(startIdxOfTuple + nextFieldOffset);
        ++fieldIdx;
    }
    /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
    fieldOffsets.emplaceFieldOffset(startIdxOfTuple + tuple.size());
    checkFieldCount(fieldIdx, numberOfFieldsInSchema);
}

/// Quote-aware variant (`allow_commas_in_strings == true`), mirroring main's
/// initializeIndexFunctionForTupleWithCommasInStrings: a field delimiter inside a quoted run is data,
/// not a separator, so the scan carries an `isQuoted` toggle and cannot use `find`. That per-character
/// walk is exactly the cost the no-quote kernel avoids -- keep this OFF for ladder runs.
void initializeIndexFunctionForTupleWithCommasInStrings(
    OldCsvFieldOffsets& fieldOffsets,
    const std::string_view tuple,
    const NES::UncompiledFieldIndex startIdxOfTuple,
    const char fieldDelimiter,
    const size_t numberOfFieldsInSchema)
{
    /// The start of the tuple is the offset of the first field of the tuple
    fieldOffsets.emplaceFieldOffset(startIdxOfTuple);
    size_t fieldIdx = 1;

    bool isQuoted = false;
    for (size_t i = 0; i < tuple.size(); ++i)
    {
        const char character = tuple[i];
        if (not isQuoted and character == fieldDelimiter)
        {
            constexpr size_t sizeOfFieldDelimiter = 1;
            fieldOffsets.emplaceFieldOffset(static_cast<NES::UncompiledFieldIndex>(startIdxOfTuple + i + sizeOfFieldDelimiter));
            ++fieldIdx;
        }
        isQuoted = isQuoted xor (character == '"');
    }

    /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
    fieldOffsets.emplaceFieldOffset(startIdxOfTuple + tuple.size());
    checkFieldCount(fieldIdx, numberOfFieldsInSchema);
}
}

namespace NES
{

SequentialUncompiledOldCSVInputFormatIndexer::SequentialUncompiledOldCSVInputFormatIndexer(
    InputFormatterDescriptor config, const size_t numberOfFieldsInSchema)
    : tupleDelimiter(config.getFromConfig(ConfigParametersSequentialUncompiledOldCSVInputFormatIndexer::TUPLE_DELIMITER).front())
    , fieldDelimiter(config.getFromConfig(ConfigParametersSequentialUncompiledOldCSVInputFormatIndexer::FIELD_DELIMITER).front())
    , numberOfFieldsInSchema(numberOfFieldsInSchema)
    , allowCommasInStrings(config.getFromConfig(ConfigParametersSequentialUncompiledOldCSVInputFormatIndexer::ALLOW_COMMAS_IN_STRINGS))
{
}

void SequentialUncompiledOldCSVInputFormatIndexer::indexRawBuffer(
    UncompiledFieldOffsets<UncompiledNumRequiredOffsetsPerField::ONE>& fieldOffsets,
    const UncompiledRawTupleBuffer& rawBuffer,
    const SequentialUncompiledOldCSVMetaData&) const
{
    constexpr size_t sizeOfFieldDelimiter = 1;
    fieldOffsets.startSetup(numberOfFieldsInSchema, sizeOfFieldDelimiter);

    constexpr size_t sizeOfTupleDelimiter = 1;
    const auto offsetOfFirstTupleDelimiter = static_cast<UncompiledFieldIndex>(rawBuffer.getBufferView().find(this->tupleDelimiter));

    /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTupleDelimiter' to a value larger than the buffer size to tell
    /// the InputFormatIndexerTask that there was no tuple delimiter in the buffer and return
    if (offsetOfFirstTupleDelimiter == static_cast<UncompiledFieldIndex>(std::string::npos))
    {
        fieldOffsets.markNoTupleDelimiters();
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
        const auto nextTuple = rawBuffer.getBufferView().substr(startIdxOfNextTuple, sizeOfNextTuple);

        /// Determine the offsets to the individual fields of the next tuple, including the start of the first and the end of the last field
        if (allowCommasInStrings)
        {
            initializeIndexFunctionForTupleWithCommasInStrings(
                fieldOffsets, nextTuple, startIdxOfNextTuple, fieldDelimiter, this->numberOfFieldsInSchema);
        }
        else
        {
            initializeIndexFunctionForTuple(fieldOffsets, nextTuple, startIdxOfNextTuple, fieldDelimiter, this->numberOfFieldsInSchema);
        }

        /// Update the start and the end index for the next tuple (if no more tuples in buffer, endIdx is 'std::string::npos')
        startIdxOfNextTuple = endIdxOfNextTuple + sizeOfTupleDelimiter;
        endIdxOfNextTuple = rawBuffer.getBufferView().find(this->tupleDelimiter, startIdxOfNextTuple);
    }
    /// Since 'endIdxOfNextTuple == std::string::npos', we use the startIdx to determine the offset of the last tuple
    const auto offsetOfLastTupleDelimiter = static_cast<UncompiledFieldIndex>(startIdxOfNextTuple - sizeOfTupleDelimiter);
    fieldOffsets.markWithTupleDelimiters(offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter);
}

DescriptorConfig::Config
SequentialUncompiledOldCSVInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersSequentialUncompiledOldCSVInputFormatIndexer>(std::move(config), NAME);
}

UncompiledInputFormatIndexerRegistryReturnType RegisterSequentialUncompiledOldCSVUncompiledInputFormatIndexer(
    UncompiledInputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createUncompiledInputFormatterTaskPipeline(
        SequentialUncompiledOldCSVInputFormatIndexer(arguments.inputFormatIndexerConfig, arguments.getNumberOfFieldsInSchema()),
        UncompiledQuotationType::NONE);
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterSequentialUncompiledOldCSVInputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return SequentialUncompiledOldCSVInputFormatIndexer::validateAndFormat(arguments.config);
}

std::ostream& operator<<(std::ostream& os, const SequentialUncompiledOldCSVInputFormatIndexer& obj)
{
    return os << fmt::format(
               "SequentialUncompiledOldCSVInputFormatIndexer(tupleDelimiter: {}, fieldDelimiter: {}, allowCommasInStrings: {})",
               obj.tupleDelimiter,
               obj.fieldDelimiter,
               obj.allowCommasInStrings);
}
}
