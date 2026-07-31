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

#include <array>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexer.hpp>
#include <RawTupleBuffer.hpp>
#include <RawValueParser.hpp>
#include <static.hpp>

namespace NES
{

/// HL7 -- a consciously simplified indexer for MLLP-framed HL7 v2 message streams
/// ("HL7 as CSV with a delimiter class").
///
/// Framing: a message is <VT=0x0B> segments <FS=0x1C><CR>. The MESSAGE (tuple) delimiter is the
/// MLLP TRAILER "\x1C\r" -- not the <VT> leader -- because the engine has no end-of-stream flush:
/// bytes after the last tuple delimiter of a stream are dropped, so the delimiter must be the bytes
/// every message (including the stream's last) ENDS with. A well-formed stream therefore ends
/// exactly with the delimiter (zero-length trailing fragment), and every tuple body is uniformly
///     \x0B MSH-header <CR> ... segments ... <CR>
/// including the stream's first (the sequence shredder seeds a virtual delimiter before it).
///
/// Indexing model: within a message body, ALL structural bytes -- segment <CR>, field |, component
/// ^, subcomponent & -- form ONE delimiter class. Every "leaf" between consecutive structural bytes
/// is one schema field, in document order, INCLUDING the junk leaves that structure produces (the
/// leading "\x0BMSH", the splits inside the MSH header's `^~\&`, the segment names, the empty leaf
/// after each message's final <CR>). The schema covers ALL leaves; declare junk leaves VARSIZED and
/// never project them -- lazy parsing makes them free. The per-message leaf count is validated
/// against the schema arity (ERROR 4000 on mismatch), which is also what catches values that
/// contain structural bytes (this indexer implements no HL7 escape sequences).
///
/// Simplifications (deliberate): one fixed-shape message type per stream, no repeated fields (~),
/// no omitted trailing fields/segments, no escape sequences (\F\, \S\, ...), no MSH-driven dynamic
/// delimiter discovery (delimiters come from the config). Optional fields are nullable: an empty
/// leaf (`||`, or HL7's explicit-null `""`) parses as NULL for nullable schema fields.
struct ConfigParametersHL7InputFormatIndexer
{
    /// The message (tuple) delimiter: the MLLP trailer <FS><CR>. Configurable for non-MLLP streams
    /// (e.g. "\\nMSH"-delimited legacy files -- note that with such a LEADING delimiter the last
    /// message of a finite stream is dropped, see the class comment). Escape sequences (\r, \xNN)
    /// are unescaped at validation.
    static inline const DescriptorConfig::ConfigParameter<std::string> MESSAGE_DELIMITER{
        "message_delimiter",
        "\x1C"
        "\r",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MESSAGE_DELIMITER, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> SEGMENT_DELIMITER{
        "segment_delimiter",
        "\r",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SEGMENT_DELIMITER, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> FIELD_DELIMITER{
        "field_delimiter",
        "|",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FIELD_DELIMITER, config); }};

    /// Component / subcomponent delimiters may be set to the empty string to EXCLUDE them from the
    /// structural class (fields then keep their ^ / & bytes as part of the leaf value).
    static inline const DescriptorConfig::ConfigParameter<std::string> COMPONENT_DELIMITER{
        "component_delimiter",
        "^",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(COMPONENT_DELIMITER, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> SUBCOMPONENT_DELIMITER{
        "subcomponent_delimiter",
        "&",
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(SUBCOMPONENT_DELIMITER, config); }};

    /// Same contract as the CSV indexer's assume_clean_strings (see ConfigParametersCSVInputFormatIndexer):
    /// string fields are assumed free of bytes that any text sink would have to escape, enabling verbatim
    /// varsized passthrough (e.g. hl7->json without a per-row escape scan). Opt out for dirty sources.
    static inline const DescriptorConfig::ConfigParameter<bool> ASSUME_CLEAN_STRINGS{
        "assume_clean_strings",
        true,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(ASSUME_CLEAN_STRINGS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            InputFormatterDescriptor::parameterMap,
            MESSAGE_DELIMITER,
            SEGMENT_DELIMITER,
            FIELD_DELIMITER,
            COMPONENT_DELIMITER,
            SUBCOMPONENT_DELIMITER,
            ASSUME_CLEAN_STRINGS);
};

constexpr auto HL7_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::ONE;

struct HL7MetaData
{
    explicit HL7MetaData(const InputFormatterDescriptor& config, const TupleBufferRef& tupleBufferRef)
        : messageDelimiter(config.getFromConfig(ConfigParametersHL7InputFormatIndexer::MESSAGE_DELIMITER))
        , assumeCleanStrings(config.getFromConfig(ConfigParametersHL7InputFormatIndexer::ASSUME_CLEAN_STRINGS))
        , fieldNames(tupleBufferRef.getAllFieldNames())
        , fieldDataTypes(tupleBufferRef.getAllDataTypes())
        , nullValues({"", "\"\""})
    {
        const auto segment = config.getFromConfig(ConfigParametersHL7InputFormatIndexer::SEGMENT_DELIMITER);
        const auto field = config.getFromConfig(ConfigParametersHL7InputFormatIndexer::FIELD_DELIMITER);
        const auto component = config.getFromConfig(ConfigParametersHL7InputFormatIndexer::COMPONENT_DELIMITER);
        const auto subcomponent = config.getFromConfig(ConfigParametersHL7InputFormatIndexer::SUBCOMPONENT_DELIMITER);
        PRECONDITION(
            segment.size() == 1 && field.size() == 1,
            "The HL7 segment and field delimiters must be exactly 1 byte (got segment '{}', field '{}')",
            segment,
            field);
        PRECONDITION(
            component.size() <= 1 && subcomponent.size() <= 1,
            "The HL7 component/subcomponent delimiters must be 1 byte, or empty to disable splitting");
        PRECONDITION(
            not messageDelimiter.empty() && messageDelimiter.size() <= 16,
            "The HL7 message delimiter must be 1..16 bytes (got size {})",
            messageDelimiter.size());
        fieldDelimiter = field.front();
        structuralBytes = segment + field + component + subcomponent;
        for (size_t i = 0; i < structuralBytes.size(); ++i)
        {
            PRECONDITION(
                structuralBytes.find(structuralBytes[i], i + 1) == std::string::npos,
                "The HL7 structural delimiters (segment/field/component/subcomponent) must be pairwise distinct");
        }
        /// Fixed-arity view of the structural class for the SIMD kernel's four broadcast compares;
        /// disabled component/subcomponent slots duplicate the segment delimiter (a duplicate
        /// compare ORs to the same mask).
        structuralChars
            = {segment.front(),
               field.front(),
               component.empty() ? segment.front() : component.front(),
               subcomponent.empty() ? segment.front() : subcomponent.front()};
    }

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const { return messageDelimiter; }

    /// All structural delimiters are enforced to be 1 byte, so this single value sizes every
    /// interior leaf in FieldOffsets<ONE>'s reader regardless of WHICH structural byte ended it.
    [[nodiscard]] std::string_view getFieldDelimitingBytes() const { return {&fieldDelimiter, 1}; }

    /// The delimiter CLASS the per-message scan splits leaves on (segment/field/component/subcomponent).
    [[nodiscard]] std::string_view getStructuralBytes() const { return structuralBytes; }

    /// The structural class as exactly four chars (disabled slots duplicated) -- the SIMD kernel's shape.
    [[nodiscard]] const std::array<char, 4>& getStructuralChars() const { return structuralChars; }

    static QuotationType getQuotationType() { return QuotationType::NONE; }

    /// Same semantics as CSVMetaData::getVarSizedCharacteristics (the assume_clean_strings contract).
    [[nodiscard]] Characteristic getVarSizedCharacteristics() const
    {
        return assumeCleanStrings ? (Characteristic::CLEAN | Characteristic::JSON_ESCAPED) : Characteristic::NONE;
    }

    [[nodiscard]] const std::vector<std::string>& getNullValues() const { return nullValues; }

    [[nodiscard]] const Record::RecordFieldIdentifier& getFieldNameAt(const nautilus::static_val<uint64_t>& i) const
    {
        PRECONDITION(i < fieldNames.size(), "Trying to access position, larger than the size of fieldNames {}", fieldNames.size());
        return fieldNames[i];
    }

    [[nodiscard]] const DataType& getFieldDataTypeAt(const nautilus::static_val<uint64_t>& i) const
    {
        PRECONDITION(
            i < fieldDataTypes.size(), "Trying to access position, larger than the size of fieldDataTypes {}", fieldDataTypes.size());
        return fieldDataTypes[i];
    }

    [[nodiscard]] uint64_t getNumberOfFields() const
    {
        INVARIANT(fieldNames.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        return fieldNames.size();
    }

private:
    std::string messageDelimiter;
    std::string structuralBytes;
    std::array<char, 4> structuralChars{};
    char fieldDelimiter{};
    bool assumeCleanStrings;
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    std::vector<DataType> fieldDataTypes;
    std::vector<std::string> nullValues;
};

class HL7InputFormatIndexer final : public InputFormatIndexer<HL7InputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "HL7";
    static constexpr bool IsSequential = false;

    using IndexerMetaData = HL7MetaData;
    using FieldIndexFunctionType = FieldOffsets<HL7_NUM_OFFSETS_PER_FIELD>;

    HL7InputFormatIndexer() = default;
    ~HL7InputFormatIndexer() = default;

    void indexRawBuffer(
        FieldOffsets<HL7_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const HL7MetaData& metaData) const;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const HL7InputFormatIndexer& obj);
};

}
