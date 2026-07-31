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

#pragma once

#include <cstdint>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Util/Logger/Formatter.hpp>
#include <static.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// Serializes each record as ONE MLLP-framed HL7 message with a constant header and a single
/// data segment (a consciously simplified HL7 producer -- see the design notes below):
///
///   <frame_start> MESSAGE_HEADER <segment_delimiter> SEGMENT_NAME |f1|f2|...|fn <segment_delimiter> <frame_end>
///
/// with the MLLP defaults frame_start = <VT> (0x0B) and frame_end = <FS><CR> (0x1C 0x0D).
/// The message header and the segment name come from the sink config; the record's schema fields
/// fill the single data segment in declaration order. NULL fields emit zero bytes (HL7's empty
/// field convention, `||`).
///
/// Simplifications (deliberate -- this exists for format benchmarking, not clinical interop):
/// - One message type per sink: the header is a byte-constant, folded into the JIT'd pipeline.
/// - No HL7 escape-sequence handling (\F\, \S\, ...): values are forwarded verbatim. A value that
///   contains a structural delimiter byte (|, ^, &, <CR>) produces a message that re-parses with
///   the wrong arity -- the HL7 input indexer rejects such messages, so violations are caught loudly.
/// - No repeating fields (~), no components in data fields: every field is a flat leaf.
class HL7OutputFormatter : public OutputFormatter
{
public:
    explicit HL7OutputFormatter(const std::vector<Record::RecordFieldIdentifier>& fieldNames, const OutputFormatterDescriptor& descriptor);

    [[nodiscard]] nautilus::val<uint64_t> writeFormattedValue(
        const VarVal& value,
        const DataType& fieldType,
        const nautilus::static_val<uint64_t>& fieldIndex,
        const nautilus::val<int8_t*>& fieldPointer,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    std::ostream& toString(std::ostream& os) const override { return os << *this; }

    friend std::ostream& operator<<(std::ostream& out, const HL7OutputFormatter& format);

private:
    /// Per-field pre-value glue, precomputed once at construction: field 0 gets the whole message
    /// prologue (frame_start + message header + segment delimiter + segment name + field delimiter),
    /// every other field just the field delimiter. The strings live here so their bytes can be
    /// embedded as compile-time constants into the JIT'd pipeline (same pattern as the JSON
    /// formatter's fieldPrefixes).
    std::vector<std::string> fieldPrefixes;
    /// Emitted after the last field: segment delimiter + frame_end (the MLLP <FS><CR> trailer).
    std::string recordSuffix;
    /// Config echoes for operator<<.
    std::string messageHeader;
    std::string segmentName;
};
}

namespace NES::OutputFormatterConfig
{
struct ConfigParametersHL7
{
    /// The constant message header emitted verbatim at the start of every message. Defaults to a
    /// minimal plausible ORU^R01 MSH segment. May itself contain segment delimiters (via '\r'
    /// escapes in the config value) to prepend additional constant segments.
    static inline const DescriptorConfig::ConfigParameter<std::string> MESSAGE_HEADER{
        "message_header",
        "MSH|^~\\&|NES|NES|BENCH|BENCH|20260101000000||ORU^R01|1|P|2.5",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MESSAGE_HEADER, config); }};

    /// The name of the single data segment that carries the record's fields.
    static inline const DescriptorConfig::ConfigParameter<std::string> SEGMENT_NAME{
        "segment_name",
        "SEG",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SEGMENT_NAME, config); }};

    /// MLLP message leader <VT>.
    static inline const DescriptorConfig::ConfigParameter<std::string> FRAME_START{
        "frame_start",
        "\x0B",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FRAME_START, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> SEGMENT_DELIMITER{
        "segment_delimiter",
        "\r",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SEGMENT_DELIMITER, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> FIELD_DELIMITER{
        "field_delimiter",
        "|",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FIELD_DELIMITER, config); }};

    /// MLLP message trailer <FS><CR>. The HL7 input indexer splits messages on exactly these bytes,
    /// so every message (including the last one in a file) ends with a complete trailer.
    static inline const DescriptorConfig::ConfigParameter<std::string> FRAME_END{
        "frame_end",
        "\x1C"
        "\r",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FRAME_END, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            OutputFormatterDescriptor::parameterMap,
            MESSAGE_HEADER,
            SEGMENT_NAME,
            FRAME_START,
            SEGMENT_DELIMITER,
            FIELD_DELIMITER,
            FRAME_END);
};
}

FMT_OSTREAM(NES::OutputFormatter);
