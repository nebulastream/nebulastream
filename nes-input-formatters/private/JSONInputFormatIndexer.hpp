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
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <InputFormatters/InputFormatIndexer.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Util/Logger/Logger.hpp>
#include <FieldOffsets.hpp>

namespace NES::InputFormatters
{
constexpr auto JSON_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::TWO;

struct JSONMetaData
{
    explicit JSONMetaData(const ParserConfig& config, Schema schema) : tupleDelimiter(config.tupleDelimiter), schema(schema) { };

    std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }
    const Schema& getSchema() const { return this->schema; }

private:
    std::string tupleDelimiter;
    Schema schema;
};

class JSONInputFormatter final
    : public InputFormatIndexer<FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>, JSONMetaData, /* IsFormattingRequired */ true>
{
public:
    static constexpr std::string_view NAME = "JSON";
    static constexpr char DELIMITER_SIZE = sizeof(char);
    static constexpr char TUPLE_DELIMITER = '\n';
    static constexpr char KEY_VALUE_DELIMITER = ':';
    static constexpr char FIELD_DELIMITER = ',';

    explicit JSONInputFormatter(const ParserConfig& config, size_t numberOfFieldsInSchema);
    ~JSONInputFormatter() override = default;

    JSONInputFormatter(const JSONInputFormatter&) = delete;
    JSONInputFormatter& operator=(const JSONInputFormatter&) = delete;
    JSONInputFormatter(JSONInputFormatter&&) = delete;
    JSONInputFormatter& operator=(JSONInputFormatter&&) = delete;

    void indexRawBuffer(
        FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const JSONMetaData&) const override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;
    static Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

private:
    size_t numberOfFieldsInSchema;
};

struct ConfigParametersJSON
{
    static inline const std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap();
};
}
