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
#include <optional>
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
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <InputFormatterDescriptor.hpp>
#include <RawBufferIndex.hpp>
#include <RawValueParser.hpp>
#include <static.hpp>

namespace NES
{

/// Resolves a single-byte delimiter parameter, unescaping textual escape sequences such as "\n" or "\t" first.
inline std::optional<char>
tryGetDelimiter(const std::unordered_map<std::string, std::string>& config, const std::string_view key, const char defaultValue)
{
    const auto it = config.find(std::string{key});
    if (it == config.end())
    {
        return defaultValue;
    }
    const auto unescaped = unescapeSpecialCharacters(it->second);
    return (unescaped.size() == 1) ? std::optional<char>{unescaped.front()} : std::nullopt;
}

struct ConfigParametersCSVInputFormatIndexer
{
    static inline const DescriptorConfig::ConfigParameter<bool> ALLOW_COMMAS_IN_STRINGS{
        "allow_commas_in_strings",
        true,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(ALLOW_COMMAS_IN_STRINGS, config); }};

    static inline const DescriptorConfig::ConfigParameter<char> TUPLE_DELIMITER{
        "tuple_delimiter",
        '\n',
        [](const std::unordered_map<std::string, std::string>& config) { return tryGetDelimiter(config, "tuple_delimiter", '\n'); }};

    static inline const DescriptorConfig::ConfigParameter<char> FIELD_DELIMITER{
        "field_delimiter",
        ',',
        [](const std::unordered_map<std::string, std::string>& config) { return tryGetDelimiter(config, "field_delimiter", ','); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            InputFormatterDescriptor::parameterMap, ALLOW_COMMAS_IN_STRINGS, TUPLE_DELIMITER, FIELD_DELIMITER);
};

class CSVInputFormatIndexer : public InputFormatIndexer
{
    /// Passkey idiom (to enforce checks before calling the constructor)
    struct Private
    {
        explicit Private() = default;
    };

public:
    static constexpr std::string_view NAME = "CSV";
    static constexpr size_t SIZE_OF_TUPLE_DELIMITER = 1;
    static constexpr size_t SIZE_OF_FIELD_DELIMITER = 1;

    explicit CSVInputFormatIndexer(
        Private, const char tupleDelimiter, const char fieldDelimiter, const bool allowCommasInStrings, const size_t numberOfFields)
        : tupleDelimiter(tupleDelimiter)
        , fieldDelimiter(fieldDelimiter)
        , allowCommasInStrings(allowCommasInStrings)
        , numberOfFields(numberOfFields)
        , nullValues({""})
    {
    }

    /// Delegate constructor that applies preconditions before safely calling the constructor
    static std::unique_ptr<CSVInputFormatIndexer> create(const InputFormatterDescriptor& config, const TupleBufferRef& tupleBufferRef)
    {
        return std::make_unique<CSVInputFormatIndexer>(
            Private{},
            config.getFromConfig(ConfigParametersCSVInputFormatIndexer::TUPLE_DELIMITER),
            config.getFromConfig(ConfigParametersCSVInputFormatIndexer::FIELD_DELIMITER),
            config.getFromConfig(ConfigParametersCSVInputFormatIndexer::ALLOW_COMMAS_IN_STRINGS),
            tupleBufferRef.getAllDataTypes().size());
    }

    ~CSVInputFormatIndexer() override = default;

    [[nodiscard]] std::unique_ptr<RawBufferIndex> indexRawBuffer(std::string_view rawBuffer) const override;

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const override { return {&tupleDelimiter, SIZE_OF_TUPLE_DELIMITER}; }

    [[nodiscard]] std::string_view getFieldDelimitingBytes() const override { return {&fieldDelimiter, SIZE_OF_FIELD_DELIMITER}; }

    [[nodiscard]] QuotationType getQuotationType() const override { return QuotationType::NONE; }

    [[nodiscard]] const std::vector<std::string>& getNullValues() const override { return nullValues; }

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    char tupleDelimiter;
    char fieldDelimiter;
    bool allowCommasInStrings{};
    size_t numberOfFields;
    std::vector<std::string> nullValues;
};

}
