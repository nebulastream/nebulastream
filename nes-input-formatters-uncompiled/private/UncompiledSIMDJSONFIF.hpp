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
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <simdjson.h>
#include <DataTypes/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <UncompiledFieldIndexFunction.hpp>

namespace NES
{

struct ConfigParametersUncompiledSIMDJSON
{
    static inline const DescriptorConfig::ConfigParameter<std::string> TUPLE_DELIMITER{
        "tuple_delimiter",
        "\n",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TUPLE_DELIMITER, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(InputFormatterDescriptor::parameterMap, TUPLE_DELIMITER);
};

struct UncompiledSIMDJSONMetaData
{
    explicit UncompiledSIMDJSONMetaData(const InputFormatterDescriptor& config, const Schema& schema)
        : tupleDelimiter(config.getFromConfig(ConfigParametersUncompiledSIMDJSON::TUPLE_DELIMITER))
    {
        PRECONDITION(
            config.getFromConfig(ConfigParametersUncompiledSIMDJSON::TUPLE_DELIMITER).size() == 1,
            "Delimiters must be of size '1 byte', but the tuple delimiter was {} (size {})",
            config.getFromConfig(ConfigParametersUncompiledSIMDJSON::TUPLE_DELIMITER),
            config.getFromConfig(ConfigParametersUncompiledSIMDJSON::TUPLE_DELIMITER).size());

        /// We expect the names in the json file to not be source qualified
        for (const auto& field : schema.getFields())
        {
            const auto& fieldName = field.name;
            if (const auto qualifierPosition = fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR);
                qualifierPosition != std::string::npos)
            {
                fieldNamesInJson.emplace_back(fieldName.substr(qualifierPosition + 1));
            }
            else
            {
                fieldNamesInJson.emplace_back(fieldName);
            }
        }
    };

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }

    [[nodiscard]] const std::string& getFieldNameInJsonAt(size_t i) const { return fieldNamesInJson.at(i); }

    [[nodiscard]] size_t getNumberOfFields() const { return fieldNamesInJson.size(); }

private:
    std::vector<std::string> fieldNamesInJson;
    std::string tupleDelimiter;
};

simdjson::simdjson_result<simdjson::ondemand::value> accessSIMDJsonFieldOrThrow(
    simdjson::simdjson_result<simdjson::ondemand::document_reference>& simdJsonReference, std::string_view fieldName);

class UncompiledSIMDJSONFIF final : public UncompiledFieldIndexFunction<UncompiledSIMDJSONFIF>
{
    friend UncompiledFieldIndexFunction<UncompiledSIMDJSONFIF>;

    /// UncompiledFieldIndexFunction (CRTP) interface functions
    [[nodiscard]] UncompiledFieldIndex applyGetByteOffsetOfFirstTuple() const { return this->offsetOfFirstTuple; }

    [[nodiscard]] UncompiledFieldIndex applyGetByteOffsetOfLastTuple() const { return this->offsetOfLastTuple; }

    /// SIMDJSON can only determine the correct number of tuples after parsing the entire buffer
    [[nodiscard]] static size_t applyGetTotalNumberOfTuples() { return 0; }

    /// Advances the document stream iterator to the requested tuple position and checks if it exists.
    [[nodiscard]] bool applyHasNext(size_t tupleIdx);

    [[nodiscard]] std::string_view applyReadFieldAt(std::string_view bufferView, size_t tupleIdx, size_t fieldIdx);

public:
    UncompiledSIMDJSONFIF() = default;
    ~UncompiledSIMDJSONFIF() = default;

    void markNoTupleDelimiters();

    void markWithTupleDelimiters(UncompiledFieldIndex offsetToFirstTuple, UncompiledFieldIndex offsetToLastTuple);

    std::pair<bool, UncompiledFieldIndex> indexJSON(std::string_view jsonSV, const UncompiledSIMDJSONMetaData& metaData);

    std::pair<bool, UncompiledFieldIndex> indexJSON(std::string_view jsonSV, const UncompiledSIMDJSONMetaData& metaData, size_t batchSize);

    template <typename T>
    static std::optional<T> parseSIMDJsonValueOrThrow(
        simdjson::simdjson_result<T> simdJsonValue,
        simdjson::simdjson_result<simdjson::ondemand::value>& rawVal,
        const std::string_view expectedType,
        const std::string_view expectedField)
    {
        if (not simdJsonValue.has_value())
        {
            throw CannotFormatMalformedStringValue(
                "SimdJson could not parse field value {} of type '{}' belonging to field '{}' with error: {}",
                rawVal.raw_json().value(),
                expectedType,
                expectedField,
                magic_enum::enum_name(simdJsonValue.error()));
        }
        return simdJsonValue.value();
    }

private:
    bool isAtLastTuple{false};
    size_t lastProcessedTupleIdx{0};
    const UncompiledSIMDJSONMetaData* metaDataPtr{nullptr};
    UncompiledFieldIndex offsetOfFirstTuple{};
    UncompiledFieldIndex offsetOfLastTuple{};
    std::shared_ptr<simdjson::ondemand::parser> parser;
    std::shared_ptr<simdjson::ondemand::document_stream> docStream;
    simdjson::ondemand::document_stream::iterator docStreamIterator;
};

}
