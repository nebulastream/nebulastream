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

#include <HL7InputFormatIndexer.hpp>

#include <ostream>
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <HL7FIF.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatterTupleBufferRef.hpp>

#include "InputFormatterValidationRegistry.hpp"

namespace NES
{

/// This function counts the fields of the HL7 message contained in the message string by counting the contained field Delimiters.
/// The message should start at the field delimiter at index 8 of the message, since we ignore the first 2 delimiter fields.
/// Afterward, it checks if the counted fields match the expected count.
bool countFieldsAndMatch(
    const std::string_view message,
    const char fieldDelim,
    const char subFieldDelim,
    const char subSubFieldDelim,
    const size_t expectedCount)
{
    const size_t fieldDelimCount = std::ranges::count(message, fieldDelim);
    const size_t subFieldDelimCount = std::ranges::count(message, subFieldDelim);
    const size_t subSubFieldDelimCount = std::ranges::count(message, subSubFieldDelim);
    return expectedCount == (fieldDelimCount + subFieldDelimCount + subSubFieldDelimCount);
}

/// Sets up the fieldAccessFunction for a single segment of a HL7 message
void setupFieldAccessFunctionForSegment(
    std::span<FieldOffsets<HL7InputFormatIndexer::HL7_NUM_OFFSETS_PER_FIELD>::IndexPairs> indexPairs,
    const std::string_view segment,
    const FieldIndex startIdxOfSegment,
    size_t& fieldIdx,
    const char fieldDelim,
    const char subFieldDelim,
    const char subSubFieldDelim)
{
    /// First field starts at offset 0.
    FieldIndex startIdxOfNextField = 0;
    /// Get the end of this field by searching for the closest out of the next field, subField and subSubField delimiters.
    size_t idxOfNextFieldDelim = segment.find(fieldDelim, startIdxOfNextField);
    size_t idxOfNextSubFieldDelim = segment.find(subFieldDelim, startIdxOfNextField);
    size_t idxOfNextSubSubFieldDelim = segment.find(subSubFieldDelim, startIdxOfNextField);
    size_t endIdxOfNextField = std::min({idxOfNextFieldDelim, idxOfNextSubFieldDelim, idxOfNextSubSubFieldDelim});

    while (endIdxOfNextField != std::string_view::npos)
    {
        /// Write the position of the next field
        indexPairs[fieldIdx] = {startIdxOfSegment + startIdxOfNextField, startIdxOfSegment + static_cast<FieldIndex>(endIdxOfNextField)};
        // Todo: swapped position of filedIdx after field-pair initialization
        fieldIdx++;
        /// Update startIdxOfNextField and search for the end of the next field
        startIdxOfNextField = endIdxOfNextField + 1;
        idxOfNextFieldDelim = segment.find(fieldDelim, startIdxOfNextField);
        idxOfNextSubFieldDelim = segment.find(subFieldDelim, startIdxOfNextField);
        idxOfNextSubSubFieldDelim = segment.find(subSubFieldDelim, startIdxOfNextField);
        endIdxOfNextField = std::min({idxOfNextFieldDelim, idxOfNextSubFieldDelim, idxOfNextSubSubFieldDelim});
    }
    /// The very last field of a segment is delimited by the segment delimiter itself, so the last endIdx lies at segment.size()
    fieldIdx++;
    indexPairs[fieldIdx] = {startIdxOfSegment + startIdxOfNextField, startIdxOfSegment + static_cast<FieldIndex>(segment.size())};
}

/// Creates field offset entries for the whole message in "message".
/// If checkIfCompleteMessage is set to true, check if the message string_view contains a whole message by counting the fields.
/// Returns true if the message was formatted successfully. Otherwise, return false.
template <bool CheckIfCompleteMessage>
bool setupFieldAccessFunctionForMessage(
    FieldOffsets<HL7InputFormatIndexer::HL7_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
    const std::string_view message,
    const FieldIndex startIdxOfMessage,
    const std::string_view segmentDelimiter,
    const size_t numberOfFieldsInSchema)
{
    if (message.size() <= HL7InputFormatIndexer::HL7_INDEX_OF_FIRST_NON_DELIMITER_VALUE_IN_MESSAGE)
    {
        return false;
    }

    /// Get the field delimiters.
    /// Todo: Get the escape char from the delimiter field (default is \, at offset 6), and implement the replacement of escaped special characters with the special character after formatting.
    /// Todo: Get the repitition delimiter (default is ~, at offset 5), and implement the formatting of repeated fields.
    /// Todo: HL7 supports empty fields. Implement formatting with empty field checks.
    /// Todo: HL7 supports the repetition of possibly nested and optional segment groups. After we support nested schemas, implement formatting of these types of structures.
    // const auto fieldDelim = std::string_view(message.begin() + 3, 1);
    // const auto subFieldDelim = std::string_view(message.begin() + 4, 1);
    // const auto subSubFieldDelim = std::string_view(message.begin() + 7, 1);
    constexpr char fieldDelim = '|';
    constexpr char subFieldDelim = '^';
    constexpr char subSubFieldDelim = '&';
    constexpr char repetitionChar = '~';
    constexpr char escapeChar = '\\';

    if (CheckIfCompleteMessage)
    {
        /// Count the non delimiter fields in the message and check if there are exactly numberOfFieldsInSchema - 2 of them.
        /// We exclude the first 2 fields, which contain the field delimiter and the other delimiters, since these definitetly exist (PRECONDITION in RegisterHL7InputFormatter)
        const auto remainingMessage
            = std::string_view(message.begin() + HL7InputFormatIndexer::HL7_INDEX_OF_FIRST_NON_DELIMITER_VALUE_IN_MESSAGE - 1);
        if (!countFieldsAndMatch(remainingMessage, fieldDelim, subFieldDelim, subSubFieldDelim, numberOfFieldsInSchema - 2))
        {
            return false;
        }
    }

    /// It is now validated, that the message contains the right amount of fields. Now we can start filling the FieldAccessFunction
    /// Add the field of the field delimiter and the field of the other delimiters. These are always at the same offset.
    /// The first "|" is a field itself and not a delimiter.
    /// MSH|^~\&|...
    size_t fieldIdx = 0;
    fieldOffsets.writeOffsetAt({startIdxOfMessage + 3, startIdxOfMessage + 4}, fieldIdx);
    fieldIdx++;
    fieldOffsets.writeOffsetAt(
        {startIdxOfMessage + 4, startIdxOfMessage + HL7InputFormatIndexer::HL7_INDEX_OF_FIRST_NON_DELIMITER_VALUE_IN_MESSAGE - 1},
        fieldIdx);

    /// From now on, we format segment per segment by looking for the next segment delimiter. We ignore the 3 byte segment identifiers like "PID"
    /// since these are not to be considered as fields according to the HL7 standard.
    size_t startIdxOfNextSegment = HL7InputFormatIndexer::HL7_INDEX_OF_FIRST_NON_DELIMITER_VALUE_IN_MESSAGE;
    size_t endIdxOfNextSegment = message.find(segmentDelimiter, startIdxOfNextSegment);
    while (endIdxOfNextSegment != std::string_view::npos)
    {
        /// Get string view of next segment
        const auto sizeOfNextSegment = endIdxOfNextSegment - startIdxOfNextSegment;
        const auto nextSegment = std::string_view(message.begin() + startIdxOfNextSegment, sizeOfNextSegment);
        const auto startIdxOfSegmentWholeBuffer = startIdxOfMessage + startIdxOfNextSegment;
        /// Fill FieldOffsets with offsets of fields in this segment
        setupFieldAccessFunctionForSegment(
            fieldOffsets, nextSegment, startIdxOfSegmentWholeBuffer, fieldIdx, fieldDelim, subFieldDelim, subSubFieldDelim);

        /// Get start and end of next segment
        /// We add 4 to the new start because we we ignore the 3 byte segment identifier and the first field delimiter.
        startIdxOfNextSegment = endIdxOfNextSegment + segmentDelimiter.size() + 4;
        endIdxOfNextSegment = message.find(segmentDelimiter, startIdxOfNextSegment);
    }
    /// The very last segment is not terminated by a segment delimiter since this segment delimiter is also part of the message delimiter \nMSH.
    /// We format the very last segment of this message
    const auto sizeOfNextSegment = message.size() - startIdxOfNextSegment;
    const auto nextSegment = std::string_view(message.begin() + startIdxOfNextSegment, sizeOfNextSegment);
    const auto startIdxOfSegmentWholeBuffer = startIdxOfMessage + startIdxOfNextSegment;
    setupFieldAccessFunctionForSegment(
        fieldOffsets, nextSegment, startIdxOfSegmentWholeBuffer, fieldIdx, fieldDelim, subFieldDelim, subSubFieldDelim);

    /// Sanity check
    if (fieldIdx + 1 != numberOfFieldsInSchema)
    {
        throw NES::FormattingError(
            "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema", fieldIdx, numberOfFieldsInSchema);
    }
    return true;
}

void HL7InputFormatIndexer::indexRawBuffer(
    FieldOffsets<HL7_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const HL7MetaData& metaData)
{
    // Todo: think about how to index the specific HL7 provided by the charite
    // -> first grab message again (look at generator by @leoraasch)
    // ->
    fieldOffsets.startSetup(metaData.getNumberOfFields(), HL7MetaData::SEGMENT_DELIMITER_SIZE);

    /// Try to find beginning of a new message
    const auto offsetOfFirstMessageDelimiter = rawBuffer.getBufferView().find(HL7MetaData::MESSAGE_DELIMITER_SIZE, 0);

    /// If the buffer does not contain a delimiter, set the 'offsetOfFirstTupleDelimiter' to a value larger than the buffer size to tell
    /// the InputFormatterTask that there was no tuple delimiter in the buffer and return
    if (offsetOfFirstMessageDelimiter == static_cast<FieldIndex>(std::string::npos))
    {
        fieldOffsets.finishSetup<false>(std::numeric_limits<FieldIndex>::max(), std::numeric_limits<FieldIndex>::max());
        return;
    }

    /// We are keeping the MSH in the message for now, since the very first buffer will start with MSH but not \nMSH, and will therefore be staged like this.
    /// The InputFormatterTask will pad this staged buffer with the segment delimiter (default is \n).
    /// If we decided to remove the MSH part from every message start, and therefore pad with \nMSH instead, this will lead to the very first staged buffer looking like
    /// this: "\MSHMSH", which will break the formatter methods, since we assume the offsets where the individual delimiters of each message are stored.
    ///
    /// Sadly, looking for \nMSH is currently the only way for us to safely assume the start of a new message, since \n itself also serves
    /// as the segment delimiter for a messages segments.
    ///
    /// Alternatively, we need to give the task an extra padding character for the first buffer
    /// or implement a new input formatter method, which sets up the field access function for a single message and we stop padding the buffers.
    auto startIdxOfNextMessage = offsetOfFirstMessageDelimiter + HL7MetaData::SEGMENT_DELIMITER_SIZE;
    size_t endIdxOfNextMessage = rawBuffer.getBufferView().find(metaData.getMessageDelimiter(), startIdxOfNextMessage);

    while (endIdxOfNextMessage != std::string_view::npos)
    {
        /// Get a string_view of the next message
        INVARIANT(startIdxOfNextMessage <= endIdxOfNextMessage, "The start index of a tuple cannot be larger than the end index.");
        const auto sizeOfNextMessage = endIdxOfNextMessage - startIdxOfNextMessage;
        const auto nextMessage = std::string_view(rawBuffer.getBufferView().begin() + startIdxOfNextMessage, sizeOfNextMessage);

        /// Determine offsets of this message
        /// Since we found a whole message delimiter as the end of this message, we can safely assume that the message is complete.
        setupFieldAccessFunctionForMessage<false>(
            fieldOffsets, nextMessage, startIdxOfNextMessage, metaData.getSegmentDelimiter(), metaData.getNumberOfFields());

        fieldOffsets.writeOffsetsOfNextTuple();
        ///Update start end end index of next message
        startIdxOfNextMessage = endIdxOfNextMessage + HL7MetaData::SEGMENT_DELIMITER_SIZE;
        endIdxOfNextMessage = rawBuffer.getBufferView().find(metaData.getMessageDelimiter(), startIdxOfNextMessage);
    }

    /// Handle possible cut message delimiter. If startIdxOfNextMessage != npos, we can check, if the last messageDelimiter.size() - 1 bytes of
    /// the buffer contain the start of a message Delimiter. If that is the case, call setupFieldAccessFunctionForMessage with the true template,
    /// to check beforehand, if the message is complete by counting all the fields.
    /// Set offsetOfLastMessageDelimiter according to the function result.
    ///
    /// !DANGER ALERT! If startIdxOfNextMessage == npos, we do not know how many fields of the message are in the "parent" buffers, which contain the remaining fields.
    /// In this case, we currently have to assume that the potential delimiter candidate is not a message delimiter and therefore
    /// need to wait until the next message delimiter after the cut off one is found to parse this message.
    /// Worst case szenario, this happens to every message we encounter and we never get to process any message.
    ///
    /// Our 4 byte message delimiter could be cut off in the last 3 bytes of the buffer.
    size_t offsetOfPotentialMessageDelim
        = rawBuffer.getBufferView().find(metaData.getSegmentDelimiter(), rawBuffer.getBufferView().size() - 3);
    if (offsetOfPotentialMessageDelim != std::string_view::npos
        && rawBuffer.getBufferView()[offsetOfPotentialMessageDelim] == metaData.getMessageDelimiter())
    {
        std::string_view potentialMessage = std::string_view(
            rawBuffer.getBufferView().begin() + startIdxOfNextMessage, offsetOfPotentialMessageDelim - startIdxOfNextMessage);

        if (setupFieldAccessFunctionForMessage<true>(
                fieldOffsets, potentialMessage, startIdxOfNextMessage, metaData.getSegmentDelimiter(), metaData.getNumberOfFields()))
        {
            const auto offsetOfLastMessageDelimiter = static_cast<FieldIndex>(offsetOfPotentialMessageDelim);
            fieldOffsets.writeOffsetsOfNextTuple();
            fieldOffsets.finishSetup<true>(offsetOfFirstMessageDelimiter, offsetOfLastMessageDelimiter);
        }
        else
        {
            const auto offsetOfLastMessageDelimiter = static_cast<FieldIndex>(startIdxOfNextMessage - HL7MetaData::SEGMENT_DELIMITER_SIZE);
            fieldOffsets.finishSetup<true>(offsetOfFirstMessageDelimiter, offsetOfLastMessageDelimiter);
        }
    }
    else
    {
        const auto offsetOfLastMessageDelimiter = static_cast<FieldIndex>(startIdxOfNextMessage - HL7MetaData::SEGMENT_DELIMITER_SIZE);
        fieldOffsets.finishSetup<true>(offsetOfFirstMessageDelimiter, offsetOfLastMessageDelimiter);
    }
}

std::ostream& operator<<(std::ostream& os, const HL7InputFormatIndexer&)
{
    return os << fmt::format("HL7InputFormatIndexer()");
}

DescriptorConfig::Config HL7InputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersHL7>(std::move(config), NAME);
}

InputFormatIndexerRegistryReturnType RegisterHL7InputFormatIndexer(InputFormatIndexerRegistryArguments arguments)
{
    return arguments.createInputFormatterWithIndexer(HL7InputFormatIndexer{});
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterHL7InputFormatterValidation(InputFormatterValidationRegistryArguments arguments)
{
    return HL7InputFormatIndexer::validateAndFormat(arguments.config);
}


}
