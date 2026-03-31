// /*
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at
//
//         https://www.apache.org/licenses/LICENSE-2.0
//
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.
// */
//
// #include <UncompiledJSONInputFormatIndexer.hpp>
//
// #include <cstddef>
// #include <cstdint>
// #include <limits>
// #include <memory>
// #include <ostream>
// #include <ranges>
// #include <string>
// #include <string_view>
// #include <unordered_map>
// #include <utility>
//
// #include <Configurations/Descriptor.hpp>
// #include <InputFormatters/UncompiledInputFormatterTaskPipeline.hpp>
// #include <Sources/SourceDescriptor.hpp>
// #include <Util/Logger/Logger.hpp>
// #include <fmt/format.h>
// #include <fmt/ranges.h>
// #include <ErrorHandling.hpp>
// #include <UncompiledFieldOffsets.hpp>
// #include <UncompiledInputFormatIndexer.hpp>
// #include <UncompiledInputFormatIndexerRegistry.hpp>
// #include <UncompiledInputFormatterTask.hpp>
// #include <RawValueParser.hpp>
//
// namespace
// {
// void setupFieldAccessFunctionForTuple(
//     NES::UncompiledFieldOffsets<NES::UNCOMPILED_JSON_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
//     const std::string_view tuple,
//     const NES::UncompiledFieldIndex startIdxOfTuple,
//     const size_t numberOfFieldsInSchema,
//     const NES::UncompiledJSONMetaData& metaData)
// {
//     using JSONFormatter = NES::UncompiledJSONInputFormatIndexer;
//
//     size_t numFields = 0;
//     NES::UncompiledFieldIndex endOfFieldInTuple = 0;
//     while (numFields < numberOfFieldsInSchema)
//     {
//         const auto keyStartPos = tuple.find(JSONFormatter::KEY_QUOTE, endOfFieldInTuple) + 1;
//         const auto keyEndPos = tuple.find(JSONFormatter::KEY_QUOTE, keyStartPos);
//         const auto key = tuple.substr(keyStartPos, keyEndPos - keyStartPos);
//         if (const auto fieldIdx = metaData.getFieldNameToIndexOffset().find(std::string(key));
//             fieldIdx != metaData.getFieldNameToIndexOffset().end())
//         {
//             ++numFields;
//             const NES::UncompiledFieldIndex startOfFieldInTuple = tuple.find(JSONFormatter::KEY_VALUE_DELIMITER, keyEndPos) + 1;
//             endOfFieldInTuple = tuple.find(JSONFormatter::FIELD_DELIMITER, startOfFieldInTuple);
//
//             if (endOfFieldInTuple == static_cast<NES::UncompiledFieldIndex>(std::string_view::npos))
//             {
//                 const NES::UncompiledFieldIndex endIdxOfTuple = startIdxOfTuple + tuple.size() - 1;
//                 fieldOffsets.writeOffsetAt(
//                     {.startOfField = startIdxOfTuple + startOfFieldInTuple, .endOfField = endIdxOfTuple}, fieldIdx->second);
//                 break;
//             }
//             fieldOffsets.writeOffsetAt(
//                 {.startOfField = startIdxOfTuple + startOfFieldInTuple, .endOfField = startIdxOfTuple + endOfFieldInTuple},
//                 fieldIdx->second);
//         }
//         else
//         {
//             throw NES::FormattingError(
//                 "Field '{}' is not part of expected schema('{}')",
//                 key,
//                 fmt::join((metaData.getFieldNameToIndexOffset() | std::views::keys), "','"));
//         }
//     }
//     if (numFields != numberOfFieldsInSchema)
//     {
//         throw NES::FormattingError(
//             "Number of parsed fields ({}) does not match number of fields in schema ({})", numFields, numberOfFieldsInSchema);
//     }
// }
// }
//
// namespace NES
// {
//
// UncompiledJSONInputFormatIndexer::UncompiledJSONInputFormatIndexer(const ParserConfig&, const size_t numberOfFieldsInSchema)
//     : numberOfFieldsInSchema(numberOfFieldsInSchema)
// {
// }
//
// void UncompiledJSONInputFormatIndexer::indexRawBuffer(
//     UncompiledFieldOffsets<UNCOMPILED_JSON_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const UncompiledRawTupleBuffer& rawBuffer, const UncompiledJSONMetaData& metaData) const
// {
//     fieldOffsets.startSetup(numberOfFieldsInSchema, FIELD_DELIMITER);
//
//     const auto offsetOfFirstTupleDelimiter = static_cast<UncompiledFieldIndex>(rawBuffer.getBufferView().find(TUPLE_DELIMITER));
//
//     /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTupleDelimiter' to a value larger than the buffer size to tell
//     /// the UncompiledInputFormatterTask that there was no tuple delimiter in the buffer and return
//     if (offsetOfFirstTupleDelimiter == static_cast<UncompiledFieldIndex>(std::string::npos))
//     {
//         fieldOffsets.markNoTupleDelimiters();
//         return;
//     }
//
//     /// If the buffer contains at least one delimiter, check if it contains more and index all tuples between the tuple delimiters
//     auto startIdxOfNextTuple = offsetOfFirstTupleDelimiter + DELIMITER_SIZE;
//     size_t endIdxOfNextTuple = rawBuffer.getBufferView().find(TUPLE_DELIMITER, startIdxOfNextTuple);
//
//     while (endIdxOfNextTuple != std::string::npos)
//     {
//         /// Get a string_view for the next tuple, by using the start and the size of the next tuple
//         INVARIANT(startIdxOfNextTuple <= endIdxOfNextTuple, "The start index of a tuple cannot be larger than the end index.");
//         const auto sizeOfNextTuple = endIdxOfNextTuple - startIdxOfNextTuple;
//         const auto nextTuple = std::string_view(rawBuffer.getBufferView().begin() + startIdxOfNextTuple, sizeOfNextTuple);
//
//         /// Determine the offsets to the individual fields of the next tuple, including the start of the first and the end of the last field
//         setupFieldAccessFunctionForTuple(fieldOffsets, nextTuple, startIdxOfNextTuple, this->numberOfFieldsInSchema, metaData);
//         fieldOffsets.writeOffsetsOfNextTuple();
//
//         /// Update the start and the end index for the next tuple (if no more tuples in buffer, endIdx is 'std::string::npos')
//         startIdxOfNextTuple = endIdxOfNextTuple + DELIMITER_SIZE;
//         endIdxOfNextTuple = rawBuffer.getBufferView().find(TUPLE_DELIMITER, startIdxOfNextTuple);
//     }
//     /// Since 'endIdxOfNextTuple == std::string::npos', we use the startIdx to determine the offset of the last tuple
//     const auto offsetOfLastTupleDelimiter = startIdxOfNextTuple - DELIMITER_SIZE;
//     fieldOffsets.markWithTupleDelimiters(offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter);
// }
//
// std::ostream& operator<<(std::ostream& os, const UncompiledJSONInputFormatIndexer&)
// {
//     return os << fmt::format(
//                "UncompiledJSONInputFormatIndexer(tupleDelimiter: {}, fieldDelimiter: {})",
//                UncompiledJSONInputFormatIndexer::TUPLE_DELIMITER,
//                UncompiledJSONInputFormatIndexer::FIELD_DELIMITER);
// }
//
// UncompiledInputFormatIndexerRegistryReturnType RegisterJSONInputFormatUncompiledIndexer(UncompiledInputFormatIndexerRegistryArguments arguments)
// {
//     return arguments.createUncompiledInputFormatterTaskPipeline(
//         UncompiledJSONInputFormatIndexer(arguments.inputFormatIndexerConfig, arguments.getNumberOfFieldsInSchema()), UncompiledQuotationType::DOUBLE_QUOTE);
// }
//
// }
