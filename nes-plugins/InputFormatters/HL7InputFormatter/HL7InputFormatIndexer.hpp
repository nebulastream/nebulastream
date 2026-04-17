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

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Configurations/Descriptor.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexer.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <RawValueParser.hpp>
#include <static.hpp>

namespace NES
{

/// Minimal HL7 v2 parser tailored for ORU_LTE messages produced by the HL7-v2-data-generator.
///
/// Assumptions:
/// - Segments within a message are separated by '\n' (the generator's segment delimiter).
/// - A new ORU_LTE message always begins with the 3-byte segment prefix 'MSH'. We therefore use
///   the 4-byte pattern '\nMSH' as the tuple (message) delimiter. The InputFormatter framework
///   wraps spanning-tuple content with this delimiter on both sides before re-calling
///   indexRawBuffer, so message content between delimiters is symmetric: it never contains the
///   leading 'MSH' bytes of its own segment header.
/// - The schema has exactly one column per top-level HL7 field, in the order the fields appear in
///   the message (MSH, PID, PV1, ORC, OBR, OBX, NTE, ...). Sub-components ('^') and
///   sub-sub-components ('&') are NOT split; they land in the top-level column verbatim.
/// - The first MSH field (the field separator itself, '|') and the second MSH field
///   (the encoding characters, '^~\&') are treated as regular fields at fixed offsets within the
///   MSH header.

constexpr auto HL7_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::TWO;

struct HL7MetaData
{
    /// Generator writes '\n' between segments. A new message always starts with the literal 'MSH',
    /// so '\nMSH' uniquely marks the boundary between two consecutive messages.
    static constexpr std::string_view TUPLE_DELIMITER = "\nMSH";
    static constexpr char FIELD_DELIMITER = '|';
    static constexpr char SEGMENT_DELIMITER = '\n';
    /// Every segment after MSH is prefixed with a 3-byte identifier (e.g. 'PID') followed by '|'.
    static constexpr size_t SEGMENT_HEADER_SIZE = 4;
    /// HL7 defines the fixed MSH.2 encoding characters as '^~\&'.
    static constexpr size_t MSH_ENCODING_CHARS_SIZE = 4;

    explicit HL7MetaData(const InputFormatterDescriptor&, const TupleBufferRef& tupleBufferRef)
        : fieldNames(tupleBufferRef.getAllFieldNames())
        , fieldDataTypes(tupleBufferRef.getAllDataTypes())
    {
        INVARIANT(fieldNames.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        INVARIANT(
            fieldNames.size() >= 2,
            "HL7 schema must include at least MSH.1 (field separator) and MSH.2 (encoding characters)");
    }

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const { return TUPLE_DELIMITER; }

    static QuotationType getQuotationType() { return QuotationType::NONE; }

    [[nodiscard]] const Record::RecordFieldIdentifier& getFieldNameAt(const nautilus::static_val<uint64_t>& i) const
    {
        return fieldNames[i];
    }

    [[nodiscard]] const DataType& getFieldDataTypeAt(const nautilus::static_val<uint64_t>& i) const { return fieldDataTypes[i]; }

    [[nodiscard]] uint64_t getNumberOfFields() const { return fieldNames.size(); }

    [[nodiscard]] const std::vector<std::string>& getNullValues() const { return nullValues; }

private:
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    std::vector<DataType> fieldDataTypes;
    std::vector<std::string> nullValues{""};
};

class HL7InputFormatIndexer final : public InputFormatIndexer<HL7InputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "HL7";
    static constexpr bool IsSequential = false;
    /// HL7's 4-byte '\nMSH' message delimiter can split across a buffer boundary and reunite
    /// inside the synthetic spanning tuple, producing two complete records in one spanning
    /// buffer. Opt into the leading/trailing spanning-tuple loop to emit both records.
    static constexpr bool SpanningTupleMayContainMultipleRecords = true;

    using IndexerMetaData = HL7MetaData;
    using FieldIndexFunctionType = FieldOffsets<HL7_NUM_OFFSETS_PER_FIELD>;

    HL7InputFormatIndexer() = default;
    ~HL7InputFormatIndexer() = default;

    static void indexRawBuffer(
        FieldOffsets<HL7_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const HL7MetaData& metaData);

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const HL7InputFormatIndexer& indexer);
};

struct ConfigParametersHL7
{
    static inline const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(InputFormatterDescriptor::parameterMap);
};

}
