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

#include <ostream>
#include <string>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <HL7FIF.hpp>
#include <InputFormatIndexer.hpp>
#include <InputFormatterTupleBufferRef.hpp>

#include <Nautilus/Interface/Record.hpp>
#include <FieldOffsets.hpp>

namespace NES
{

struct ConfigParametersHL7
{
    static inline const DescriptorConfig::ConfigParameter<std::string> MESSAGE_DELIMITER{
        "message_delimiter",
        "\n",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MESSAGE_DELIMITER, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> SEGMENT_DELIMITER{
        "segment_delimiter",
        "|",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SEGMENT_DELIMITER, config); }};

    static inline const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(InputFormatterDescriptor::parameterMap, MESSAGE_DELIMITER, SEGMENT_DELIMITER);
};

struct HL7MetaData
{
    static constexpr char TUPLE_DELIMITER = '\n';
    static constexpr size_t DELIMITER_SIZE = 1;
    static constexpr size_t SEGMENT_DELIMITER_SIZE = 1;
    static constexpr size_t MESSAGE_DELIMITER_SIZE = 1;

    explicit HL7MetaData(const InputFormatterDescriptor& config, const TupleBufferRef& tupleBufferRef)
        : segmentDelimiter(config.getFromConfig(ConfigParametersHL7::SEGMENT_DELIMITER)[0])
        , messageDelimiter(config.getFromConfig(ConfigParametersHL7::MESSAGE_DELIMITER)[0])
        , dataTypes(tupleBufferRef.getAllDataTypes())
        , fieldNames(tupleBufferRef.getAllFieldNames())
    {
        INVARIANT(fieldNames.size() == dataTypes.size(), "No. fields must be equal to no. data types");
    };

    std::string_view getTupleDelimitingBytes() const { return std::to_string(TUPLE_DELIMITER); }

    [[nodiscard]] char getTupleDelimiter() const { return TUPLE_DELIMITER; }

    [[nodiscard]] char getSegmentDelimiter() const { return segmentDelimiter; }

    [[nodiscard]] char getMessageDelimiter() const { return messageDelimiter; }

    static QuotationType getQuotationType() { return QuotationType::DOUBLE_QUOTE; }

    [[nodiscard]] const Record::RecordFieldIdentifier& getFieldNameAt(const nautilus::static_val<uint64_t>& i) const
    {
        return fieldNames[i];
    }

    [[nodiscard]] const DataType& getFieldDataTypeAt(const nautilus::static_val<uint64_t>& i) const { return dataTypes[i]; }

    [[nodiscard]] static const std::vector<std::string>& getNullValues()
    {
        static std::vector<std::string> nullValues{""};
        return nullValues;
    }

    [[nodiscard]] uint64_t getNumberOfFields() const { return fieldNames.size(); }

private:
    /// Any segment starts with this
    char segmentDelimiter;
    /// segmentDelimiter + MSH
    char messageDelimiter;
    std::vector<DataType> dataTypes;
    std::vector<Record::RecordFieldIdentifier> fieldNames;
};

/// If we consider the MSH header of the message to be at offset = 0 (e.g. MSH|^~\&|1)


class HL7InputFormatIndexer final : public InputFormatIndexer<HL7InputFormatIndexer>
{
public:
    static constexpr auto HL7_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::TWO;
    static constexpr size_t HL7_INDEX_OF_FIRST_NON_DELIMITER_VALUE_IN_MESSAGE = 9;

    static constexpr std::string_view NAME = "HL7";
    static constexpr bool IsSequential = false;

    using IndexerMetaData = HL7MetaData;
    using FieldIndexFunctionType = FieldOffsets<HL7_NUM_OFFSETS_PER_FIELD>;

    HL7InputFormatIndexer() = default;
    ~HL7InputFormatIndexer() = default;

    static void
    indexRawBuffer(FieldOffsets<HL7_NUM_OFFSETS_PER_FIELD>& fieldIndexFunction, const RawTupleBuffer& rawBuffer, const HL7MetaData&);
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const HL7InputFormatIndexer& sonInputFormatIndexer);
};

}
