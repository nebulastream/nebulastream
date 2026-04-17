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

#include <cstddef>
#include <limits>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <InputFormatterValidationRegistry.hpp>

namespace NES
{

namespace
{

/// Indexes one complete ORU_LTE message whose 'MSH' segment prefix has already been consumed by the
/// tuple delimiter. `message` therefore starts at the first byte of MSH.1 — i.e. with '|' followed
/// by the encoding characters '^~\&', followed by '|' before MSH.3, and so on.
///
/// The layout is:
///   |^~\&|MSH.3|MSH.4|...|MSH.N\nPID|PID.1|...|PID.M\nPV1|...\n...
///
/// This function writes one (start, end) offset pair per field into `pairs`. Offsets are absolute
/// byte positions in the underlying raw buffer, hence the `startIdxInBuffer` base offset.
void indexSingleMessage(
    std::span<FieldOffsets<HL7_NUM_OFFSETS_PER_FIELD>::IndexPairs> pairs,
    std::string_view message,
    FieldIndex startIdxInBuffer,
    const size_t expectedNumberOfFields)
{
    /// Messages between two '\nMSH' delimiters have their 'MSH' prefix consumed by the delimiter
    /// and therefore start with '|'. The single exception is the leading spanning tuple of the
    /// very first buffer in a stream: the framework wraps its content with a sentinel delimiter
    /// whose synthetic trailing bytes are empty, so the first message keeps its 'MSH' prefix.
    /// Strip it here so the rest of the parsing is uniform.
    if (message.size() >= 3 && message.substr(0, 3) == "MSH")
    {
        message.remove_prefix(3);
        startIdxInBuffer += 3;
    }

    /// MSH.1 is the field separator '|' itself, and MSH.2 is the 4-byte encoding characters '^~\&'.
    /// After these, the layout follows the ordinary '|'-separated, '\n'-segmented pattern.
    INVARIANT(
        message.size() >= 1 + HL7MetaData::MSH_ENCODING_CHARS_SIZE,
        "HL7 message after tuple delimiter must contain at least the field separator and encoding characters, got size {}",
        message.size());
    INVARIANT(message[0] == HL7MetaData::FIELD_DELIMITER, "Expected '|' as first byte of message content");

    size_t fieldIdx = 0;

    /// MSH.1 = "|" (1 byte starting at offset 0 of message)
    pairs[fieldIdx++] = {startIdxInBuffer, static_cast<FieldIndex>(startIdxInBuffer + 1)};

    /// MSH.2 = "^~\&" (4 bytes starting at offset 1 of message)
    pairs[fieldIdx++]
        = {static_cast<FieldIndex>(startIdxInBuffer + 1),
           static_cast<FieldIndex>(startIdxInBuffer + 1 + HL7MetaData::MSH_ENCODING_CHARS_SIZE)};

    /// Continue at offset 5: the '|' delimiter that precedes MSH.3.
    size_t pos = 1 + HL7MetaData::MSH_ENCODING_CHARS_SIZE;
    INVARIANT(
        pos >= message.size() || message[pos] == HL7MetaData::FIELD_DELIMITER,
        "Expected '|' after MSH encoding characters");

    size_t startOfNextField = pos + 1;
    pos = pos + 1;

    while (pos < message.size() && fieldIdx < expectedNumberOfFields)
    {
        const char c = message[pos];
        if (c == HL7MetaData::FIELD_DELIMITER)
        {
            pairs[fieldIdx++]
                = {static_cast<FieldIndex>(startIdxInBuffer + startOfNextField), static_cast<FieldIndex>(startIdxInBuffer + pos)};
            startOfNextField = pos + 1;
            ++pos;
        }
        else if (c == HL7MetaData::SEGMENT_DELIMITER)
        {
            /// End of current field and current segment. Skip the segment's 3-byte identifier plus
            /// the '|' that follows it (e.g. '\nPID|'), so the next iteration starts at the first
            /// field of the next segment.
            pairs[fieldIdx++]
                = {static_cast<FieldIndex>(startIdxInBuffer + startOfNextField), static_cast<FieldIndex>(startIdxInBuffer + pos)};
            pos += 1 + HL7MetaData::SEGMENT_HEADER_SIZE;
            startOfNextField = pos;
        }
        else
        {
            ++pos;
        }
    }

    /// The last field is terminated by end-of-message, possibly with a trailing '\n' from the
    /// generator output. Trim that trailing segment delimiter if present.
    if (fieldIdx < expectedNumberOfFields)
    {
        size_t endOfLastField = message.size();
        if (endOfLastField > startOfNextField && message[endOfLastField - 1] == HL7MetaData::SEGMENT_DELIMITER)
        {
            --endOfLastField;
        }
        pairs[fieldIdx++]
            = {static_cast<FieldIndex>(startIdxInBuffer + startOfNextField),
               static_cast<FieldIndex>(startIdxInBuffer + endOfLastField)};
    }

    if (fieldIdx != expectedNumberOfFields)
    {
        throw CannotFormatSourceData(
            "Number of parsed HL7 fields ({}) does not match number of fields in schema ({})", fieldIdx, expectedNumberOfFields);
    }
}

}

void HL7InputFormatIndexer::indexRawBuffer(
    FieldOffsets<HL7_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const HL7MetaData& metaData)
{
    fieldOffsets.startSetup(metaData.getNumberOfFields(), HL7MetaData::TUPLE_DELIMITER.size());

    const auto bufferView = rawBuffer.getBufferView();
    const auto tupleDelimiter = HL7MetaData::TUPLE_DELIMITER;

    const auto offsetOfFirstTupleDelimiterPos = bufferView.find(tupleDelimiter);
    if (offsetOfFirstTupleDelimiterPos == std::string_view::npos)
    {
        fieldOffsets.markNoTupleDelimiters();
        return;
    }
    const auto offsetOfFirstTupleDelimiter = static_cast<FieldIndex>(offsetOfFirstTupleDelimiterPos);

    /// Messages BETWEEN two '\nMSH' delimiters are emitted as tuples directly. The content BEFORE
    /// the first delimiter and AFTER the last delimiter are handled by the InputFormatter
    /// framework's spanning-tuple mechanism: it synthesizes a buffer of the form
    /// '\nMSH<content>\nMSH' (wrapping the content with the tuple delimiter on both sides) and
    /// re-invokes this function, which then parses <content> as a between-delimiter message.
    ///
    /// INPUT REQUIREMENT: the stream must end with the 4-byte sentinel '\nMSH' so that the trailing
    /// content of the final buffer is empty. The HL7-v2-data-generator output does not naturally
    /// include this sentinel — appending '\nMSH' to the file is the caller's responsibility. Inline
    /// test data achieves this by adding a final 'MSH' line (the inline writer appends '\n' to each
    /// line, so the file ends with '...\nMSH\n' and the last '\nMSH' is the final tuple delimiter).
    auto startIdxOfNextMessage = offsetOfFirstTupleDelimiter + tupleDelimiter.size();
    auto endIdxOfNextMessage = bufferView.find(tupleDelimiter, startIdxOfNextMessage);

    while (endIdxOfNextMessage != std::string_view::npos)
    {
        INVARIANT(startIdxOfNextMessage <= endIdxOfNextMessage, "Start index of message must not exceed end index");
        const auto nextMessage = bufferView.substr(startIdxOfNextMessage, endIdxOfNextMessage - startIdxOfNextMessage);
        auto pairs = fieldOffsets.emplaceTupleOffsets(metaData.getNumberOfFields());
        indexSingleMessage(pairs, nextMessage, static_cast<FieldIndex>(startIdxOfNextMessage), metaData.getNumberOfFields());

        startIdxOfNextMessage = endIdxOfNextMessage + tupleDelimiter.size();
        endIdxOfNextMessage = bufferView.find(tupleDelimiter, startIdxOfNextMessage);
    }

    const auto offsetOfLastTupleDelimiter = static_cast<FieldIndex>(startIdxOfNextMessage - tupleDelimiter.size());
    fieldOffsets.markWithTupleDelimiters(offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter);
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
