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
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexer.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <RawValueParser.hpp>
#include <static.hpp>

namespace NES
{

struct ConfigParametersCSVInputFormatIndexer
{
    static inline const DescriptorConfig::ConfigParameter<bool> ALLOW_COMMAS_IN_STRINGS{
        "allow_commas_in_strings",
        true,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(ALLOW_COMMAS_IN_STRINGS, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> TUPLE_DELIMITER{
        "tuple_delimiter",
        "\n",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TUPLE_DELIMITER, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> FIELD_DELIMITER{
        "field_delimiter",
        ",",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FIELD_DELIMITER, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            InputFormatterDescriptor::parameterMap, ALLOW_COMMAS_IN_STRINGS, TUPLE_DELIMITER, FIELD_DELIMITER);
};

constexpr auto CSV_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::ONE;

struct CSVMetaData
{
    static constexpr size_t SIZE_OF_TUPLE_DELIMITER = 1;
    static constexpr size_t SIZE_OF_FIELD_DELIMITER = 1;

    explicit CSVMetaData(const InputFormatterDescriptor& config, const TupleBufferRef& tupleBufferRef)
        : tupleDelimiter(config.getFromConfig(ConfigParametersCSVInputFormatIndexer::TUPLE_DELIMITER).front())
        , fieldDelimiter(config.getFromConfig(ConfigParametersCSVInputFormatIndexer::FIELD_DELIMITER).front())
        , fieldNames(tupleBufferRef.getAllFieldNames())
        , fieldDataTypes(tupleBufferRef.getAllDataTypes())
        , nullValues({""})
    {
        PRECONDITION(
            config.getFromConfig(ConfigParametersCSVInputFormatIndexer::TUPLE_DELIMITER).size() == SIZE_OF_TUPLE_DELIMITER,
            "Delimiters must be of size '1 byte', but the tuple delimiter was {} (size {})",
            config.getFromConfig(ConfigParametersCSVInputFormatIndexer::TUPLE_DELIMITER),
            config.getFromConfig(ConfigParametersCSVInputFormatIndexer::TUPLE_DELIMITER).size());
        PRECONDITION(
            config.getFromConfig(ConfigParametersCSVInputFormatIndexer::FIELD_DELIMITER).size() == SIZE_OF_FIELD_DELIMITER,
            "Delimiters must be of size '1 byte', but the field delimiter was {} (size {})",
            config.getFromConfig(ConfigParametersCSVInputFormatIndexer::FIELD_DELIMITER),
            config.getFromConfig(ConfigParametersCSVInputFormatIndexer::FIELD_DELIMITER).size());
    };

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const { return {&tupleDelimiter, SIZE_OF_TUPLE_DELIMITER}; }

    [[nodiscard]] std::string_view getFieldDelimitingBytes() const { return {&fieldDelimiter, SIZE_OF_FIELD_DELIMITER}; }

    [[nodiscard]] char getTupleDelimiter() const { return tupleDelimiter; }

    [[nodiscard]] char getFieldDelimiter() const { return fieldDelimiter; }

    static QuotationType getQuotationType() { return QuotationType::NONE; }

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
    char tupleDelimiter;
    char fieldDelimiter;
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    std::vector<DataType> fieldDataTypes;
    std::vector<std::string> nullValues;
};

class CSVInputFormatIndexer : public InputFormatIndexer<CSVInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "CSV";

    using IndexerMetaData = CSVMetaData;
    using FieldIndexFunctionType = FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>;

    explicit CSVInputFormatIndexer(const InputFormatterDescriptor& config);
    ~CSVInputFormatIndexer() = default;


    void indexRawBuffer(
        FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const CSVMetaData& metaData) const;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const CSVInputFormatIndexer& obj);

private:
    bool allowCommasInStrings{};
};

}
