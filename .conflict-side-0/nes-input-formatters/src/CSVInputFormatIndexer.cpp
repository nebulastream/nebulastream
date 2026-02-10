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

namespace
{

void initializeIndexFunctionForTuple(
    NES::FieldOffsets<NES::CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
    const std::string_view tuple,
    const NES::FieldIndex startIdxOfTuple,
    const NES::CSVMetaData& metaData)
{
    /// The start of the tuple is the offset of the first field of the tuple
    fieldOffsets.emplaceFieldOffset(startIdxOfTuple);
    size_t fieldIdx = 1;
    /// Find field delimiters, until reaching the end of the tuple
    /// The position of the field delimiter (+ size of field delimiter) is the beginning of the next field
    for (size_t nextFieldOffset = tuple.find(metaData.getFieldDelimiter(), 0); nextFieldOffset != std::string_view::npos;
         nextFieldOffset = tuple.find(metaData.getFieldDelimiter(), nextFieldOffset))
    {
        nextFieldOffset += NES::CSVMetaData::SIZE_OF_FIELD_DELIMITER;
        fieldOffsets.emplaceFieldOffset(startIdxOfTuple + nextFieldOffset);
        ++fieldIdx;
    }
    /// The last delimiter is the size of the tuple itself, which allows the next phase to determine the last field without any extra calculations
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

void CSVInputFormatIndexer::indexRawBuffer(
    FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const CSVMetaData& metaData) const
{
    fieldOffsets.startSetup(metaData.getNumberOfFields(), NES::CSVMetaData::SIZE_OF_TUPLE_DELIMITER);

    const auto offsetOfFirstTupleDelimiter = static_cast<FieldIndex>(rawBuffer.getBufferView().find(metaData.getTupleDelimiter()));

    /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTupleDelimiter' to a value larger than the buffer size to tell
    /// the InputFormatIndexerTask that there was no tuple delimiter in the buffer and return
    if (offsetOfFirstTupleDelimiter == static_cast<FieldIndex>(std::string::npos))
    {
        fieldOffsets.markNoTupleDelimiters();
        return;
    }

    /// If the buffer contains at least one delimiter, check if it contains more and index all tuples between the tuple delimiters
    auto startIdxOfNextTuple = offsetOfFirstTupleDelimiter + NES::CSVMetaData::SIZE_OF_TUPLE_DELIMITER;
    size_t endIdxOfNextTuple = rawBuffer.getBufferView().find(metaData.getTupleDelimiter(), startIdxOfNextTuple);

    while (endIdxOfNextTuple != std::string::npos)
    {
        /// Get a string_view for the next tuple, by using the start and the size of the next tuple
        INVARIANT(startIdxOfNextTuple <= endIdxOfNextTuple, "The start index of a tuple cannot be larger than the end index.");
        const auto sizeOfNextTuple = endIdxOfNextTuple - startIdxOfNextTuple;
        const auto nextTuple = rawBuffer.getBufferView().substr(startIdxOfNextTuple, sizeOfNextTuple);

        /// Determine the offsets to the individual fields of the next tuple, including the start of the first and the end of the last field
        initializeIndexFunctionForTuple(fieldOffsets, nextTuple, startIdxOfNextTuple, metaData);

        /// Update the start and the end index for the next tuple (if no more tuples in buffer, endIdx is 'std::string::npos')
        startIdxOfNextTuple = endIdxOfNextTuple + NES::CSVMetaData::SIZE_OF_TUPLE_DELIMITER;
        endIdxOfNextTuple = rawBuffer.getBufferView().find(metaData.getTupleDelimiter(), startIdxOfNextTuple);
    }
    /// Since 'endIdxOfNextTuple == std::string::npos', we use the startIdx to determine the offset of the last tuple
    const auto offsetOfLastTupleDelimiter = static_cast<FieldIndex>(startIdxOfNextTuple - NES::CSVMetaData::SIZE_OF_TUPLE_DELIMITER);
    fieldOffsets.markWithTupleDelimiters(offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter);
}

InputFormatIndexerRegistryReturnType
RegisterCSVInputFormatIndexer(InputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createInputFormatterWithIndexer(CSVInputFormatIndexer{});
}

std::ostream& operator<<(std::ostream& os, const CSVInputFormatIndexer&)
{
    return os << fmt::format("CSVInputFormatIndexer()");
}
}
