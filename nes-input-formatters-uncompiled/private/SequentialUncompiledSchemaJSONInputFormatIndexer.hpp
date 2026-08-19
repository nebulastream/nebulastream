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
#include <string>
#include <string_view>
#include <unordered_map>

#include <DataTypes/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <UncompiledInputFormatIndexer.hpp>
#include <UncompiledSchemaJSONFIF.hpp>

namespace NES
{

struct ConfigParametersUncompiledSchemaJSON
{
    static inline const DescriptorConfig::ConfigParameter<std::string> TUPLE_DELIMITER{
        "tuple_delimiter",
        "\n",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TUPLE_DELIMITER, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(InputFormatterDescriptor::parameterMap, TUPLE_DELIMITER);
};

struct UncompiledSchemaJSONMetaData
{
    explicit UncompiledSchemaJSONMetaData(const InputFormatterDescriptor& config, const Schema&)
        : tupleDelimiter(config.getFromConfig(ConfigParametersUncompiledSchemaJSON::TUPLE_DELIMITER))
    {
        PRECONDITION(
            tupleDelimiter.size() == 1,
            "Delimiters must be of size '1 byte', but the tuple delimiter was {} (size {})",
            tupleDelimiter,
            tupleDelimiter.size());
    }

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }

private:
    std::string tupleDelimiter;
};

/// Uncompiled (interpreted) twin of the SchemaJSON formatter -- the ablation baseline rung for JSON input. The
/// INDEXING phase is identical to the compiled SchemaJSON (simdjson stage 1 over the region between the first
/// and last tuple delimiter, fixed-stride 4F+1 addressing, shape check); only the parse phase differs: the
/// generic interpreted per-field loop (processUncompiledTuple) instead of the compiled strided read. The bulk
/// contract matches SequentialUncompiledCSV: tuple 0 is the first COMPLETE record after the first delimiter;
/// the leading row is completed by the sequential task's carry accumulator, the trailing partial is carried.
class SequentialUncompiledSchemaJSONInputFormatIndexer
    : public UncompiledInputFormatIndexer<SequentialUncompiledSchemaJSONInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SequentialUncompiledSchemaJSON";
    static constexpr bool IsFormattingRequired = true;
    static constexpr bool HasSpanningTuple = true;
    static constexpr bool IsSequential = true;

    using UncompiledIndexerMetaData = UncompiledSchemaJSONMetaData;
    using UncompiledFieldIndexFunctionType = UncompiledSchemaJSONFIF;

    explicit SequentialUncompiledSchemaJSONInputFormatIndexer(size_t numberOfFieldsInSchema);
    ~SequentialUncompiledSchemaJSONInputFormatIndexer() = default;

    void indexRawBuffer(
        UncompiledSchemaJSONFIF& fieldIndexFunction,
        const UncompiledRawTupleBuffer& rawBuffer,
        const UncompiledSchemaJSONMetaData& metaData) const;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SequentialUncompiledSchemaJSONInputFormatIndexer& obj);

private:
    size_t numberOfFieldsInSchema;
};

}
