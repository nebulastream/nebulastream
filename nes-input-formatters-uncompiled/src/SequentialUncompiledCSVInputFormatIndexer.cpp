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

#include <SequentialUncompiledCSVInputFormatIndexer.hpp>

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

/// Reuses the same field-offset logic as the concurrent CSV indexer
void initializeIndexFunctionForTuple(
    NES::UncompiledFieldOffsets<NES::UNCOMPILED_CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
    const std::string_view tuple,
    const NES::UncompiledFieldIndex startIdxOfTuple,
    const char fieldDelimiter,
    size_t numberOfFieldsInSchema)
{
    fieldOffsets.emplaceFieldOffset(startIdxOfTuple);
    size_t fieldIdx = 1;
    for (size_t nextFieldOffset = tuple.find(fieldDelimiter, 0); nextFieldOffset != std::string_view::npos;
         nextFieldOffset = tuple.find(fieldDelimiter, nextFieldOffset))
    {
        nextFieldOffset += 1;
        fieldOffsets.emplaceFieldOffset(startIdxOfTuple + nextFieldOffset);
        ++fieldIdx;
    }
    fieldOffsets.emplaceFieldOffset(startIdxOfTuple + tuple.size());
    if (fieldIdx != numberOfFieldsInSchema)
    {
        throw NES::CannotFormatSourceData(
            "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema", fieldIdx, numberOfFieldsInSchema);
    }
}
}

namespace NES
{

SequentialUncompiledCSVInputFormatIndexer::SequentialUncompiledCSVInputFormatIndexer(
    InputFormatterDescriptor config, const size_t numberOfFieldsInSchema)
    : tupleDelimiter(config.getFromConfig(ConfigParametersUncompiledCSVInputFormatIndexer::TUPLE_DELIMITER).front())
    , fieldDelimiter(config.getFromConfig(ConfigParametersUncompiledCSVInputFormatIndexer::FIELD_DELIMITER).front())
    , numberOfFieldsInSchema(numberOfFieldsInSchema)
{
}

void SequentialUncompiledCSVInputFormatIndexer::indexRawBuffer(
    UncompiledFieldOffsets<UNCOMPILED_CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
    const UncompiledRawTupleBuffer& rawBuffer,
    const UncompiledCSVMetaData&) const
{
    fieldOffsets.startSetup(numberOfFieldsInSchema, 1);

    constexpr size_t sizeOfTupleDelimiter = 1;
    const auto offsetOfFirstTupleDelimiter = static_cast<UncompiledFieldIndex>(rawBuffer.getBufferView().find(this->tupleDelimiter));

    if (offsetOfFirstTupleDelimiter == static_cast<UncompiledFieldIndex>(std::string::npos))
    {
        fieldOffsets.markNoTupleDelimiters();
        return;
    }

    /// Sequential mode: buffer is aligned, so the first tuple starts at position 0
    const auto firstTuple = rawBuffer.getBufferView().substr(0, offsetOfFirstTupleDelimiter);
    initializeIndexFunctionForTuple(fieldOffsets, firstTuple, 0, fieldDelimiter, this->numberOfFieldsInSchema);

    /// Index remaining tuples between consecutive delimiters (same as concurrent version)
    auto startIdxOfNextTuple = offsetOfFirstTupleDelimiter + sizeOfTupleDelimiter;
    size_t endIdxOfNextTuple = rawBuffer.getBufferView().find(this->tupleDelimiter, startIdxOfNextTuple);

    while (endIdxOfNextTuple != std::string::npos)
    {
        INVARIANT(startIdxOfNextTuple <= endIdxOfNextTuple, "The start index of a tuple cannot be larger than the end index.");
        const auto sizeOfNextTuple = endIdxOfNextTuple - startIdxOfNextTuple;
        const auto nextTuple = rawBuffer.getBufferView().substr(startIdxOfNextTuple, sizeOfNextTuple);

        initializeIndexFunctionForTuple(fieldOffsets, nextTuple, startIdxOfNextTuple, fieldDelimiter, this->numberOfFieldsInSchema);

        startIdxOfNextTuple = endIdxOfNextTuple + sizeOfTupleDelimiter;
        endIdxOfNextTuple = rawBuffer.getBufferView().find(this->tupleDelimiter, startIdxOfNextTuple);
    }

    const auto offsetOfLastTupleDelimiter = static_cast<UncompiledFieldIndex>(startIdxOfNextTuple - sizeOfTupleDelimiter);
    fieldOffsets.markWithTupleDelimiters(offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter);
}

DescriptorConfig::Config SequentialUncompiledCSVInputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersUncompiledCSVInputFormatIndexer>(std::move(config), NAME);
}

UncompiledInputFormatIndexerRegistryReturnType RegisterSequentialUncompiledCSVUncompiledInputFormatIndexer(
    UncompiledInputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createUncompiledInputFormatterTaskPipeline(
        SequentialUncompiledCSVInputFormatIndexer(arguments.inputFormatIndexerConfig, arguments.getNumberOfFieldsInSchema()),
        UncompiledQuotationType::NONE);
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterSequentialUncompiledCSVInputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return SequentialUncompiledCSVInputFormatIndexer::validateAndFormat(arguments.config);
}

std::ostream& operator<<(std::ostream& os, const SequentialUncompiledCSVInputFormatIndexer& obj)
{
    return os << fmt::format(
               "SequentialUncompiledCSVInputFormatIndexer(tupleDelimiter: {}, fieldDelimiter: {})", obj.tupleDelimiter, obj.fieldDelimiter);
}
}
