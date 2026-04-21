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
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Registry.hpp>
#include <InputFormatIndexer.hpp>
#include <InputFormatter.hpp>

namespace NES
{


using InputFormatIndexerRegistryReturnType = std::unique_ptr<InputFormatter>;

/// Argument bundle handed to `InputFormatIndexerRegistry`-resolved factories. Owns the `ParserConfig` + memory provider and
/// hands a `unique_ptr<InputFormatIndexer>` to `createInputFormatterWithIndexer()` to assemble a concrete `InputFormatter`.
struct InputFormatIndexerRegistryArguments
{
    InputFormatIndexerRegistryArguments(ParserConfig config, std::shared_ptr<TupleBufferRef> memoryProvider)
        : inputFormatIndexerConfig(std::move(config)), memoryProvider(std::move(memoryProvider))
    {
    }

    [[nodiscard]] const ParserConfig& getInputFormatterConfig() const { return inputFormatIndexerConfig; }

    [[nodiscard]] const TupleBufferRef& getInputMemoryProvider() const { return *memoryProvider; }

    /// Instantiates an InputFormatter with a specific input format indexer
    InputFormatIndexerRegistryReturnType createInputFormatterWithIndexer(std::unique_ptr<InputFormatIndexer> inputFormatIndexer)
    {
        return std::make_unique<InputFormatter>(std::move(inputFormatIndexer), memoryProvider, inputFormatIndexerConfig);
    }

private:
    /// We discourage keeping state in an indexer implementation, since the InputFormatter uses its indexer concurrently
    /// As a result, we don't hand the config and memory provider to the indexer in its registry-constructor
    /// Instead, an indexer receives it as a const meta-data object in its main 'indexRawBuffer' function
    /// While this does not prevent an indexer implementation from accessing the state of the meta-data object it helps to discourage it
    ParserConfig inputFormatIndexerConfig;
    std::shared_ptr<TupleBufferRef> memoryProvider;
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
