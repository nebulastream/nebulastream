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
#include <ostream>
#include <string_view>

#include <DataTypes/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <UncompiledInputFormatters/UncompiledInputFormatterTaskPipeline.hpp>
#include <UncompiledFieldOffsets.hpp>
#include <UncompiledInputFormatIndexer.hpp>

namespace NES
{

constexpr auto UNCOMPILED_CSV_NUM_OFFSETS_PER_FIELD = UncompiledNumRequiredOffsetsPerField::ONE;

struct ConfigParametersUncompiledCSVInputFormatIndexer
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


struct UncompiledCSVMetaData
{
    explicit UncompiledCSVMetaData(const InputFormatterDescriptor& config, const Schema&)
        : tupleDelimiter(config.getFromConfig(ConfigParametersUncompiledCSVInputFormatIndexer::TUPLE_DELIMITER)) { };

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }

private:
    std::string tupleDelimiter;
};

class UncompiledCSVInputFormatIndexer : public UncompiledInputFormatIndexer<UncompiledCSVInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "UncompiledCSV";
    static constexpr bool IsFormattingRequired = true;
    static constexpr bool HasSpanningTuple = true;
    using UncompiledIndexerMetaData = UncompiledCSVMetaData;
    using UncompiledFieldIndexFunctionType = UncompiledFieldOffsets<UNCOMPILED_CSV_NUM_OFFSETS_PER_FIELD>;

    explicit UncompiledCSVInputFormatIndexer(InputFormatterDescriptor config, size_t numberOfFieldsInSchema);
    ~UncompiledCSVInputFormatIndexer() = default;

    void indexRawBuffer(
        UncompiledFieldOffsets<UNCOMPILED_CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
        const UncompiledRawTupleBuffer& rawBuffer,
        const UncompiledCSVMetaData&) const;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const UncompiledCSVInputFormatIndexer& obj);

private:
    char tupleDelimiter;
    char fieldDelimiter;
    size_t numberOfFieldsInSchema;
};

}
