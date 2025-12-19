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
#include <InputFormatters/InputFormatterTupleBufferRef.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexer.hpp>
#include <RawValueParser.hpp>

namespace NES
{
constexpr auto JSON_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::TWO;

struct JSONMetaData
{
    explicit JSONMetaData(const ParserConfig& config, const MemoryLayout& memoryLayout)
        : schema(memoryLayout.getSchema()), tupleDelimiter(config.tupleDelimiter)
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

    const Schema& getSchema() const { return this->schema; }

    std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }

    static QuotationType getQuotationType() { return QuotationType::DOUBLE_QUOTE; }

    const std::unordered_map<std::string, FieldIndex>& getFieldNameToIndexOffset() const { return this->fieldNameToIndexOffset; }

private:
    Schema schema;
    std::string tupleDelimiter;
    std::unordered_map<std::string, FieldIndex> fieldNameToIndexOffset;
};

class JSONInputFormatIndexer final : public InputFormatIndexer<JSONInputFormatIndexer>
{
public:
    using IndexerMetaData = JSONMetaData;
    using FieldIndexFunctionType = FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>;
    static constexpr char DELIMITER_SIZE = sizeof(char);
    static constexpr char TUPLE_DELIMITER = '\n';
    static constexpr char KEY_VALUE_DELIMITER = ':';
    static constexpr char FIELD_DELIMITER = ',';
    static constexpr char KEY_QUOTE = '"';

    JSONInputFormatIndexer() = default;
    ~JSONInputFormatIndexer() = default;

    static void
    indexRawBuffer(FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const JSONMetaData&);

    friend std::ostream& operator<<(std::ostream& os, const JSONInputFormatIndexer& jsonInputFormatIndexer);
};

struct ConfigParametersJSON
{
    static inline const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap();
};
}
