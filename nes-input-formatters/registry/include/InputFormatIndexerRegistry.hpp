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
#include <memory>
#include <string>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Registry.hpp>
#include <InputFormatterTask.hpp>

namespace NES::InputFormatters
{


using InputFormatIndexerRegistryReturnType = std::unique_ptr<InputFormatterTaskPipeline>;

/// Allows specific InputFormatter to construct templated Formatter using the 'createInputFormatterTaskPipeline()' method.
/// Calls constructor of specific InputFormatter and exposes public members to it.
struct InputFormatIndexerRegistryArguments
{
    InputFormatIndexerRegistryArguments(ParserConfig config, const OriginId originId, Schema schema)
        : inputFormatIndexerConfig(std::move(config)), originId(originId), schema(std::move(schema))
    {
    }

    /// @tparam: FormatterType: the concrete formatter implementation, e.g., CSVInputFormatter
    /// @tparam: FieldAccessType: function used to index fields when parsing/processing the data of the (raw) input buffer
    /// @tparam: HasSpanningTuple: hardcode to 'true' if format cannot guarantee buffers with tuples that never span across buffers
    template <typename FormatterType, typename FieldAccessType, typename IndexerMetaData, bool HasSpanningTuple>
    InputFormatIndexerRegistryReturnType
    createInputFormatterTaskPipeline(std::unique_ptr<FormatterType> inputFormatter, const RawValueParser::QuotationType quotationType)
    {
        auto inputFormatterTask = InputFormatterTask<FormatterType, FieldAccessType, IndexerMetaData, HasSpanningTuple>(
            originId, std::move(inputFormatter), schema, quotationType, inputFormatIndexerConfig);
        return std::make_unique<InputFormatterTaskPipeline>(std::move(inputFormatterTask));
    }

    size_t getNumberOfFieldsInSchema() const { return schema.getNumberOfFields(); }
    ParserConfig inputFormatIndexerConfig;

private:
    OriginId originId{NES::OriginId(NES::OriginId::INVALID)};
    Schema schema;
};

class InputFormatIndexerRegistry : public BaseRegistry<
                                       InputFormatIndexerRegistry,
                                       std::string,
                                       InputFormatIndexerRegistryReturnType,
                                       InputFormatIndexerRegistryArguments>
{
};

}

#define INCLUDED_FROM_INPUT_FORMAT_INDEXER_REGISTRY
#include <InputFormatIndexerGeneratedRegistrar.inc>
#undef INCLUDED_FROM_INPUT_FORMAT_INDEXER_REGISTRY
