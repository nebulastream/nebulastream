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
#include <Sources/SourceDescriptor.hpp>
#include <Util/Registry.hpp>
#include <Concepts.hpp>
#include <RawScan.hpp>
#include <RawScanWrapper.hpp>

namespace NES
{


using RawScanIndexerRegistryReturnType = std::unique_ptr<RawScanWrapper>;

/// Allows specific InputFormatter to construct templated Formatter using the 'createRawScanWithIndexer()' method.
/// Calls constructor of specific InputFormatter and exposes public members to it.
struct RawScanIndexerRegistryArguments
{
    RawScanIndexerRegistryArguments(
        ParserConfig config, std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider)
        : RawScanIndexerConfig(std::move(config)), memoryProvider(std::move(memoryProvider))
    {
    }

    /// Instantiates an InputFormatter with a specific input format indexer
    template <RawScanIndexerType IndexerType>
    RawScanIndexerRegistryReturnType createRawScanWithIndexer(IndexerType RawScanIndexer)
    {
        auto rawScan = RawScan<IndexerType>(std::move(RawScanIndexer), std::move(memoryProvider), RawScanIndexerConfig);
        return std::make_unique<RawScanWrapper>(std::move(rawScan));
    }

private:
    /// We discourage keeping state in an indexer implementation, since the RawScan uses its indexer concurrently
    /// As a result, we don't hand the config and memory provider to the indexer in its registry-constructor
    /// Instead, an indexer receives it as a const meta-data object in its main 'indexRawBuffer' function
    /// While this does not prevent an indexer implementation from accessing the state of the meta-data object it helps to discourage it
    ParserConfig RawScanIndexerConfig;
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider;
};

class RawScanIndexerRegistry
    : public BaseRegistry<RawScanIndexerRegistry, std::string, RawScanIndexerRegistryReturnType, RawScanIndexerRegistryArguments>
{
};

}

#define INCLUDED_FROM_INPUT_FORMAT_INDEXER_REGISTRY
#include <RawScanIndexerGeneratedRegistrar.inc>
#undef INCLUDED_FROM_INPUT_FORMAT_INDEXER_REGISTRY
