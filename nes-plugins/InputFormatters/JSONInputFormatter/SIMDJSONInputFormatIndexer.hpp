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
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ranges>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <InputFormatterDescriptor.hpp>
#include <RawBufferIndex.hpp>
#include <RawValueParser.hpp>
#include <static.hpp>

namespace NES
{
struct ConfigParametersSIMDJSON
{
    static inline const DescriptorConfig::ConfigParameter<char> TUPLE_DELIMITER{
        "TUPLE_DELIMITER",
        '\n',
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<char>
        {
            const auto it = config.find("TUPLE_DELIMITER");
            if (it == config.end())
            {
                return '\n';
            }
            const auto unescaped = unescapeSpecialCharacters(it->second);
            return (unescaped.size() == 1) ? std::optional<char>{unescaped.front()} : std::nullopt;
        }};

    static inline const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(InputFormatterDescriptor::parameterMap, TUPLE_DELIMITER);
};

class SIMDJSONInputFormatIndexer final : public InputFormatIndexer
{
    /// Passkey idiom (to enforce checks before calling the constructor)
    struct Private
    {
        explicit Private() = default;
    };

public:
    static constexpr std::string_view NAME = "JSON";
    static constexpr char DELIMITER_SIZE = sizeof(char);
    static constexpr char TUPLE_DELIMITER = '\n';
    static constexpr char KEY_VALUE_DELIMITER = ':';
    static constexpr char KEY_QUOTE = '"';

    explicit SIMDJSONInputFormatIndexer(
        Private,
        const char tupleDelimiter,
        std::vector<Identifier> fieldNamesInJson,
        std::vector<Record::RecordFieldIdentifier> fieldNamesOutput,
        std::vector<DataType> fieldDataTypes)
        : tupleDelimiter(tupleDelimiter)
        , fieldNamesInJson(std::move(fieldNamesInJson))
        , fieldNamesOutput(std::move(fieldNamesOutput))
        , fieldDataTypes(std::move(fieldDataTypes))
        , nullValues({})
    {
    }

    /// Delegate constructor that applies preconditions before safely calling the constructor
    static std::unique_ptr<SIMDJSONInputFormatIndexer> create(const InputFormatterDescriptor& config, const TupleBufferRef& tupleBufferRef)
    {
        /// JSON keys are unqualified — take the trailing identifier of each (possibly source-qualified) name.
        std::vector<Identifier> fieldNamesInJson;
        for (const auto& fieldName : tupleBufferRef.getAllFieldNames())
        {
            fieldNamesInJson.emplace_back(*std::ranges::rbegin(fieldName));
        }

        auto fieldNamesOutput = tupleBufferRef.getAllFieldNames();
        auto fieldDataTypes = tupleBufferRef.getAllDataTypes();
        PRECONDITION(fieldNamesInJson.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        PRECONDITION(fieldNamesOutput.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");

        return std::make_unique<SIMDJSONInputFormatIndexer>(
            Private{},
            config.getFromConfig(ConfigParametersSIMDJSON::TUPLE_DELIMITER),
            std::move(fieldNamesInJson),
            std::move(fieldNamesOutput),
            std::move(fieldDataTypes));
    }

    ~SIMDJSONInputFormatIndexer() override = default;

    [[nodiscard]] std::unique_ptr<RawBufferIndex> indexRawBuffer(std::string_view rawBuffer) const override;

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const override { return {&tupleDelimiter, 1}; }

    [[nodiscard]] std::string_view getFieldDelimitingBytes() const override { return ""; }

    [[nodiscard]] QuotationType getQuotationType() const override { return QuotationType::DOUBLE_QUOTE; }

    [[nodiscard]] const std::vector<std::string>& getNullValues() const override { return nullValues; }

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] const Record::RecordFieldIdentifier& getFieldNameAt(const nautilus::static_val<uint64_t>& fieldIndex) const
    {
        return fieldNamesOutput[fieldIndex];
    }

    [[nodiscard]] const Identifier& getFieldNameInJsonAt(const nautilus::static_val<uint64_t>& fieldIndex) const
    {
        return fieldNamesInJson[fieldIndex];
    }

    [[nodiscard]] const DataType& getFieldDataTypeAt(const nautilus::static_val<uint64_t>& fieldIndex) const
    {
        return fieldDataTypes[fieldIndex];
    }

    [[nodiscard]] uint64_t getNumberOfFields() const
    {
        INVARIANT(fieldNamesOutput.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        return fieldNamesOutput.size();
    }

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    char tupleDelimiter;
    std::vector<Identifier> fieldNamesInJson;
    std::vector<Record::RecordFieldIdentifier> fieldNamesOutput;
    std::vector<DataType> fieldDataTypes;
    std::vector<std::string> nullValues;
};
}
