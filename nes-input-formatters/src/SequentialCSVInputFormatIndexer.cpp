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

#include <SequentialCSVInputFormatIndexer.hpp>

#include <cstddef>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

#include <Sources/SourceDescriptor.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatter.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <InputFormatterValidationRegistry.hpp>

namespace
{

void initializeIndexFunctionForTupleWithCommasInStrings(
    NES::FieldOffsets<NES::CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
    const std::string_view tuple,
    const NES::FieldIndex startIdxOfTuple,
    const NES::CSVMetaData& metaData)
{
    fieldOffsets.emplaceFieldOffset(startIdxOfTuple);
    size_t fieldIdx = 1;

    bool isQuoted = false;
    for (const auto [i, character] : tuple | NES::views::enumerate)
    {
        if (not isQuoted and character == metaData.getFieldDelimiter())
        {
            constexpr size_t sizeOfFieldDelimiter = 1;
            fieldOffsets.emplaceFieldOffset(startIdxOfTuple + i + sizeOfFieldDelimiter);
            ++fieldIdx;
        }
        isQuoted = isQuoted xor (character == '"');
    }

    fieldOffsets.emplaceFieldOffset(startIdxOfTuple + tuple.size());
    if (fieldIdx != metaData.getNumberOfFields())
    {
        throw NES::CannotFormatSourceData(
            "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema",
            fieldIdx,
            metaData.getNumberOfFields());
    }
}

void initializeIndexFunctionForTuple(
    NES::FieldOffsets<NES::CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
    const std::string_view tuple,
    const NES::FieldIndex startIdxOfTuple,
    const NES::CSVMetaData& metaData)
{
    fieldOffsets.emplaceFieldOffset(startIdxOfTuple);
    size_t fieldIdx = 1;
    for (size_t nextFieldOffset = tuple.find(metaData.getFieldDelimiter(), 0); nextFieldOffset != std::string_view::npos;
         nextFieldOffset = tuple.find(metaData.getFieldDelimiter(), nextFieldOffset))
    {
        nextFieldOffset += NES::CSVMetaData::SIZE_OF_FIELD_DELIMITER;
        fieldOffsets.emplaceFieldOffset(startIdxOfTuple + nextFieldOffset);
        ++fieldIdx;
    }
    fieldOffsets.emplaceFieldOffset(startIdxOfTuple + tuple.size());
    if (fieldIdx != metaData.getNumberOfFields())
    {
        throw NES::CannotFormatSourceData(
            "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema",
            fieldIdx,
            metaData.getNumberOfFields());
    }
}
}

namespace NES
{

SequentialCSVInputFormatIndexer::SequentialCSVInputFormatIndexer(const InputFormatterDescriptor& config)
    : allowCommasInStrings(config.getFromConfig(ConfigParametersCSVInputFormatIndexer::ALLOW_COMMAS_IN_STRINGS))
{
}

void SequentialCSVInputFormatIndexer::indexRawBuffer(
    FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const CSVMetaData& metaData) const
{
    fieldOffsets.startSetup(metaData.getNumberOfFields(), NES::CSVMetaData::SIZE_OF_TUPLE_DELIMITER);

    const auto offsetOfFirstTupleDelimiter = static_cast<FieldIndex>(rawBuffer.getBufferView().find(metaData.getTupleDelimiter()));

    if (offsetOfFirstTupleDelimiter == static_cast<FieldIndex>(std::string::npos))
    {
        fieldOffsets.markNoTupleDelimiters();
        return;
    }

    /// Sequential mode: buffer is aligned, so the first tuple starts at position 0
    const auto firstTuple = rawBuffer.getBufferView().substr(0, offsetOfFirstTupleDelimiter);
    if (allowCommasInStrings)
    {
        initializeIndexFunctionForTupleWithCommasInStrings(fieldOffsets, firstTuple, 0, metaData);
    }
    else
    {
        initializeIndexFunctionForTuple(fieldOffsets, firstTuple, 0, metaData);
    }

    /// Index remaining tuples between consecutive delimiters (same as concurrent version)
    auto startIdxOfNextTuple = offsetOfFirstTupleDelimiter + NES::CSVMetaData::SIZE_OF_TUPLE_DELIMITER;
    size_t endIdxOfNextTuple = rawBuffer.getBufferView().find(metaData.getTupleDelimiter(), startIdxOfNextTuple);

    while (endIdxOfNextTuple != std::string::npos)
    {
        INVARIANT(startIdxOfNextTuple <= endIdxOfNextTuple, "The start index of a tuple cannot be larger than the end index.");
        const auto sizeOfNextTuple = endIdxOfNextTuple - startIdxOfNextTuple;
        const auto nextTuple = rawBuffer.getBufferView().substr(startIdxOfNextTuple, sizeOfNextTuple);

        if (allowCommasInStrings)
        {
            initializeIndexFunctionForTupleWithCommasInStrings(fieldOffsets, nextTuple, startIdxOfNextTuple, metaData);
        }
        else
        {
            initializeIndexFunctionForTuple(fieldOffsets, nextTuple, startIdxOfNextTuple, metaData);
        }

        startIdxOfNextTuple = endIdxOfNextTuple + NES::CSVMetaData::SIZE_OF_TUPLE_DELIMITER;
        endIdxOfNextTuple = rawBuffer.getBufferView().find(metaData.getTupleDelimiter(), startIdxOfNextTuple);
    }

    const auto offsetOfLastTupleDelimiter = static_cast<FieldIndex>(startIdxOfNextTuple - NES::CSVMetaData::SIZE_OF_TUPLE_DELIMITER);
    fieldOffsets.markWithTupleDelimiters(offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter);
}

DescriptorConfig::Config SequentialCSVInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCSVInputFormatIndexer>(std::move(config), NAME);
}

InputFormatIndexerRegistryReturnType
RegisterSequentialCSVInputFormatIndexer(InputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createInputFormatterWithIndexer(SequentialCSVInputFormatIndexer{arguments.getInputFormatterConfig()});
}

InputFormatterValidationRegistryReturnType InputFormatterValidationGeneratedRegistrar::RegisterSequentialCSVInputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return SequentialCSVInputFormatIndexer::validateAndFormat(arguments.config);
}

std::ostream& operator<<(std::ostream& os, const SequentialCSVInputFormatIndexer&)
{
    return os << fmt::format("SequentialCSVInputFormatIndexer()");
}
}
