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
#include <InputFormatters/UncompiledInputFormatterTaskPipeline.hpp>
#include <Util/Logger/Logger.hpp>
#include <UncompiledFieldOffsets.hpp>
#include <UncompiledInputFormatIndexer.hpp>

namespace NES
{
constexpr auto UNCOMPILED_JSON_NUM_OFFSETS_PER_FIELD = UncompiledNumRequiredOffsetsPerField::TWO;

struct UncompiledJSONMetaData
{
    explicit UncompiledJSONMetaData(const ParserConfig& config, const Schema& schema) : tupleDelimiter(config.tupleDelimiter)
    {
        for (const auto& [fieldIdx, field] : schema | NES::views::enumerate)
        {
            if (const auto& qualifierPosition = field.name.find(Schema::ATTRIBUTE_NAME_SEPARATOR); qualifierPosition != std::string::npos)
            {
                fieldNameToIndexOffset.emplace(field.name.substr(qualifierPosition + 1), fieldIdx);
            }
            else
            {
                fieldNameToIndexOffset.emplace(field.name, fieldIdx);
            }
        }
    };

    std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }

    const std::unordered_map<std::string, UncompiledFieldIndex>& getFieldNameToIndexOffset() const { return this->fieldNameToIndexOffset; }

private:
    std::string tupleDelimiter;
    std::unordered_map<std::string, UncompiledFieldIndex> fieldNameToIndexOffset;
};

class UncompiledJSONInputFormatIndexer final : public UncompiledInputFormatIndexer<UncompiledJSONInputFormatIndexer>
{
public:
    static constexpr bool IsFormattingRequired = true;
    static constexpr bool HasSpanningTuple = true;
    using UncompiledIndexerMetaData = UncompiledJSONMetaData;
    using UncompiledFieldIndexFunctionType = UncompiledFieldOffsets<UNCOMPILED_JSON_NUM_OFFSETS_PER_FIELD>;
    static constexpr char DELIMITER_SIZE = sizeof(char);
    static constexpr char TUPLE_DELIMITER = '\n';
    static constexpr char KEY_VALUE_DELIMITER = ':';
    static constexpr char FIELD_DELIMITER = ',';
    static constexpr char KEY_QUOTE = '"';

    explicit UncompiledJSONInputFormatIndexer(const ParserConfig& config, size_t numberOfFieldsInSchema);
    ~UncompiledJSONInputFormatIndexer() = default;

    void indexRawBuffer(UncompiledFieldOffsets<UNCOMPILED_JSON_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const UncompiledRawTupleBuffer& rawBuffer, const UncompiledJSONMetaData&) const;

    friend std::ostream& operator<<(std::ostream& os, const UncompiledJSONInputFormatIndexer& jsonInputFormatIndexer);

private:
    size_t numberOfFieldsInSchema;
};

struct UncompiledConfigParametersJSON
{
    static inline const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap();
};
}
