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
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>
#include <static.hpp>

namespace NES
{
struct ConfigParametersNestedJSON
{
    static inline const DescriptorConfig::ConfigParameter<char> TUPLE_DELIMITER{
        "tuple_delimiter",
        '\n',
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<char>
        {
            const auto it = config.find("tuple_delimiter");
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

class NestedJSONInputFormatIndexer final : public InputFormatIndexer
{
    /// Passkey idiom (to enforce checks before calling the constructor)
    struct Private
    {
        explicit Private() = default;
    };

public:
    static constexpr std::string_view NAME = "NestedJSON";
    static constexpr char DELIMITER_SIZE = sizeof(char);
    static constexpr char TUPLE_DELIMITER = '\n';
    static constexpr char KEY_VALUE_DELIMITER = ':';
    static constexpr char KEY_QUOTE = '"';

    explicit NestedJSONInputFormatIndexer(
        Private,
        const char tupleDelimiter,
        std::vector<Record::RecordFieldIdentifier> fieldNamesInJson,
        std::vector<Record::RecordFieldIdentifier> fieldNamesOutput,
        std::vector<DataType> fieldDataTypes)
        : tupleDelimiter(tupleDelimiter)
        , fieldNamesInJson(std::move(fieldNamesInJson))
        , fieldNamesOutput(std::move(fieldNamesOutput))
        , fieldDataTypes(std::move(fieldDataTypes))
        , nullValues({""})
    {
    }

    /// Delegate constructor that applies preconditions before safely calling the constructor
    static std::unique_ptr<NestedJSONInputFormatIndexer>
    create(const InputFormatterDescriptor& config, const TupleBufferRef& tupleBufferRef)
    {
        /// We expect the names in the json file to not be source qualified.
        /// The remaining (unqualified) field name encodes the JSON path; '/' separates nesting levels (e.g. "user/name").
        std::vector<Record::RecordFieldIdentifier> fieldNamesInJson;
        for (const auto& fieldName : tupleBufferRef.getAllFieldNames())
        {
            if (const auto& qualifierPosition = fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR); qualifierPosition != std::string::npos)
            {
                fieldNamesInJson.emplace_back(fieldName.substr(qualifierPosition + 1));
            }
            else
            {
                fieldNamesInJson.emplace_back(fieldName);
            }
        }

        auto fieldNamesOutput = tupleBufferRef.getAllFieldNames();
        auto fieldDataTypes = tupleBufferRef.getAllDataTypes();
        PRECONDITION(fieldNamesInJson.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        PRECONDITION(fieldNamesOutput.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");

        return std::make_unique<NestedJSONInputFormatIndexer>(
            Private{},
            config.getFromConfig(ConfigParametersNestedJSON::TUPLE_DELIMITER),
            std::move(fieldNamesInJson),
            std::move(fieldNamesOutput),
            std::move(fieldDataTypes));
    }

    ~NestedJSONInputFormatIndexer() override = default;

    [[nodiscard]] std::unique_ptr<RawBufferIndex> indexRawBuffer(const RawTupleBuffer& rawBuffer) const override;

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const override { return {&tupleDelimiter, 1}; }

    [[nodiscard]] std::string_view getFieldDelimitingBytes() const override { return ""; }

    [[nodiscard]] const std::vector<std::string>& getNullValues() const override { return nullValues; }

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const NestedJSONInputFormatIndexer& nestedJsonInputFormatIndexer);

    [[nodiscard]] const Record::RecordFieldIdentifier& getFieldNameAt(const nautilus::static_val<uint64_t>& i) const
    {
        return fieldNamesOutput[i];
    }

    [[nodiscard]] const Record::RecordFieldIdentifier& getFieldNameInJsonAt(const nautilus::static_val<uint64_t>& i) const
    {
        return fieldNamesInJson[i];
    }

    [[nodiscard]] const DataType& getFieldDataTypeAt(const nautilus::static_val<uint64_t>& i) const { return fieldDataTypes[i]; }

    [[nodiscard]] uint64_t getNumberOfFields() const
    {
        INVARIANT(fieldNamesOutput.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        return fieldNamesOutput.size();
    }

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    char tupleDelimiter;
    std::vector<Record::RecordFieldIdentifier> fieldNamesInJson{};
    std::vector<Record::RecordFieldIdentifier> fieldNamesOutput{};
    std::vector<DataType> fieldDataTypes{};
    std::vector<std::string> nullValues;
};
}
