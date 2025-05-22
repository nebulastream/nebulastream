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
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Util/Logger/Logger.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexer.hpp>

namespace NES::InputFormatters
{
constexpr auto JSON_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::TWO;

struct JSONMetaData
{
    explicit JSONMetaData(const ParserConfig& config, Schema) : tupleDelimiter(config.tupleDelimiter) { };

    std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }

private:
    std::string tupleDelimiter;
};

class JSONInputFormatIndexer final : public InputFormatIndexer<JSONInputFormatIndexer>
{
public:
    static constexpr bool IsFormattingRequired = true;
    static constexpr bool HasSpanningTuple = true;
    using IndexerMetaData = JSONMetaData;
    using FieldIndexFunctionType = FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>;
    static constexpr char DELIMITER_SIZE = sizeof(char);
    static constexpr char TUPLE_DELIMITER = '\n';
    static constexpr char KEY_VALUE_DELIMITER = ':';
    static constexpr char FIELD_DELIMITER = ',';

    explicit JSONInputFormatIndexer(const ParserConfig& config, size_t numberOfFieldsInSchema);
    ~JSONInputFormatIndexer() = default;

    void indexRawBuffer(FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const JSONMetaData&) const;

    friend std::ostream& operator<<(std::ostream& os, const JSONInputFormatIndexer& jsonInputFormatIndexer);

private:
    size_t numberOfFieldsInSchema;
};

struct ConfigParametersJSON
{
    static inline const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap();
};
}
