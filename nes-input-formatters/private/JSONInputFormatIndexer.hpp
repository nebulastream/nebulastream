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
struct JSONMetaData
{
    explicit JSONMetaData(const ParserConfig& config, Schema) : tupleDelimiter(config.tupleDelimiter) { };

    std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }

private:
    std::string tupleDelimiter;
};

class JSONInputFormatter final : public InputFormatIndexer<JSONInputFormatter>
{
public:
    using FieldIndexFunctionType = FieldOffsets<NumRequiredOffsetsPerField::TWO>;
    using IndexerMetaData = JSONMetaData;
    static constexpr bool IsFormattingRequired = true;
    static constexpr bool HasSpanningTuple = true;

    static constexpr std::string_view NAME = "JSON";
    static constexpr char DELIMITER_SIZE = sizeof(char);
    static constexpr char TUPLE_DELIMITER = '\n';
    static constexpr char KEY_VALUE_DELIMITER = ':';
    static constexpr char FIELD_DELIMITER = ',';

    explicit JSONInputFormatter(const ParserConfig& config, size_t numberOfFieldsInSchema);

    void indexRawBuffer(FieldIndexFunctionType& fieldOffsets, const RawTupleBuffer& rawBuffer, const IndexerMetaData&) const;

    friend std::ostream& operator<<(std::ostream& os, const JSONInputFormatter& obj);
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

private:
    size_t numberOfFieldsInSchema;
};

struct ConfigParametersJSON
{
    static inline const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap();
};
}
