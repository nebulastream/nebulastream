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
#include <expected>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/Strings.hpp>
#include <Util/Variant.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <FieldOffsetRawBufferIndex.hpp>
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

namespace
{

/// ConfigField factory for a single-byte delimiter parameter; unescapes textual escape sequences
/// such as "\n" or "\t" first.
std::expected<char, Exception> parseDelimiter(const ConfigLiteral& literal)
{
    return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
        .and_then(
            [](const std::string& value) -> std::expected<char, Exception>
            {
                const auto unescaped = unescapeSpecialCharacters(value);
                if (unescaped.size() != 1)
                {
                    return std::unexpected{InvalidConfigParameter("Expected a single (possibly escaped) character, got {}", value)};
                }
                return unescaped.front();
            });
}

/// Config fields of the CSV input formatter, shared by getConfigSchema (declaration) and
/// CSVInputFormatterConfig::fromConfig (typed extraction).
/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<bool> ALLOW_COMMAS_IN_STRINGS{
    "ALLOW_COMMAS_IN_STRINGS", [](const ConfigLiteral& literal) { return NES::tryGetOr<bool>(literal, expectedType<bool>()); }, true};

const ConfigField<char> TUPLE_DELIMITER{"TUPLE_DELIMITER", parseDelimiter, '\n'};

const ConfigField<char> FIELD_DELIMITER{"FIELD_DELIMITER", parseDelimiter, ','};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> CSVInputFormatIndexer::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("CSV_INPUT_FORMATTER"), ALLOW_COMMAS_IN_STRINGS, TUPLE_DELIMITER, FIELD_DELIMITER);
}

std::expected<CSVInputFormatterConfig, Exception> CSVInputFormatterConfig::fromConfig(const InstantiatedConfig& config)
{
    return CSVInputFormatterConfig{
        .allowCommasInStrings = config.get(ALLOW_COMMAS_IN_STRINGS),
        .tupleDelimiter = config.get(TUPLE_DELIMITER),
        .fieldDelimiter = config.get(FIELD_DELIMITER)};
}

std::ostream& CSVInputFormatIndexer::toString(std::ostream& str) const
{
    return str << fmt::format("CSVInputFormatIndexer()");
}
}
