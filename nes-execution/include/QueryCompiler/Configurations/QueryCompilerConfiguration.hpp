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

#include <memory>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/BooleanValidation.hpp>
#include <Configurations/Validation/FloatRangeValidation.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <Nautilus/NautilusBackend.hpp>
#include <QueryCompiler/Configurations/Enums/CompilationStrategy.hpp>
#include <QueryCompiler/Configurations/Enums/DumpMode.hpp>
#include <QueryCompiler/Configurations/Enums/WindowManagement.hpp>
#include "Configurations/BaseOption.hpp"

namespace NES::QueryCompilation::Configurations
{
static constexpr auto DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES = 100;
static constexpr auto DEFAULT_PAGED_VECTOR_SIZE = 1024;

class QueryCompilerConfiguration final : public NES::Configurations::BaseConfiguration
{
public:
    QueryCompilerConfiguration() = default;
    QueryCompilerConfiguration(std::string name, std::string description) : BaseConfiguration(std::move(name), std::move(description)) {};

    /// Sets the dump mode for the query compiler. This setting is only for the nautilus compiler
    NES::Configurations::EnumOption<DumpMode> dumpMode
        = {"dumpMode", DumpMode::NONE, "If and where to dump query compilation details [NONE|CONSOLE|FILE|FILE_AND_CONSOLE]."};
    NES::Configurations::StringOption dumpPath = {"dumpPath", "", "Path to dump query compilation details."};
    NES::Configurations::EnumOption<Nautilus::Configurations::NautilusBackend> nautilusBackend
        = {"nautilusBackend",
           Nautilus::Configurations::NautilusBackend::COMPILER,
           "Nautilus backend for the nautilus query compiler "
           "[COMPILER|INTERPRETER]."};

    NES::Configurations::UIntOption numberOfPartitions
        = {"numberOfPartitions",
           std::to_string(DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES),
           "Partitions in a hash table",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption pageSize
        = {"pageSize",
           std::to_string(DEFAULT_PAGED_VECTOR_SIZE),
           "Page size of any other paged data structure",
           {std::make_shared<NES::Configurations::NumberValidation>()}};

    NES::Configurations::EnumOption<StreamJoinStrategy> joinStrategy
        = {"joinStrategy",
           StreamJoinStrategy::NESTED_LOOP_JOIN,
           "WindowingStrategy"
           "[HASH_JOIN_LOCAL|HASH_JOIN_GLOBAL_LOCKING|HASH_JOIN_GLOBAL_LOCK_FREE|NESTED_LOOP_JOIN]. "};

    NES::Configurations::StringOption pipelinesTxtFilePath = {"pipelinesTxtFilePath", "pipelines.txt", "Path to dump pipeline details."};
    NES::Configurations::StringOption cacheHitsAndMissesFilePath
        = {"cacheHitsAndMissesFilePath", "cache_hits_and_misses.txt", "Path to dump cache hits and misses."};
    NES::Configurations::EnumOption<SliceStoreType> sliceStoreType
        = {"sliceStoreType",
           SliceStoreType::MAP,
           "Type of slice store"
           "[MAP|LIST]. "};
    NES::Configurations::EnumOption<SliceCacheType> sliceCacheType
        = {"sliceCacheType",
           SliceCacheType::NONE,
           "Type of slice cache"
           "[NONE|FIFO|LRU|SECOND_CHANCE]. "};
    NES::Configurations::EnumOption<ProbeType> probeType
        = {"probeType",
           ProbeType::PROBING,
           "Type of probe. NO_PROBING means that the probe operator will do nothing and will NOT pass any tuples through."
           "[PROBING|NO_PROBING]. "};
    NES::Configurations::UIntOption numberOfEntriesSliceCache
        = {"numberOfEntriesSliceCache", "1", "Size of the slice cache", {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::BoolOption lockSliceCache
        = {"lockSliceCache", "false", "Enable lock in slice cache", {std::make_shared<NES::Configurations::BooleanValidation>()}};
    NES::Configurations::BoolOption sortBufferByField = {"sortBufferByField", "false", "Should the sort buffer operator sort by field.", {std::make_shared<NES::Configurations::BooleanValidation>()}};
    NES::Configurations::BoolOption gatherSlices = {"gatherSlices", "false", "Gather slices after the sort buffer operator and then create them at bulk.", {std::make_shared<NES::Configurations::BooleanValidation>()}};
    NES::Configurations::StringOption sortBufferOrder = {"sortBufferOrder", "Ascending", "Sort order for the sort buffer operator."};

    NES::Configurations::EnumOption<ShuffleStrategy> shuffleStrategy
        = {"shuffleStrategy",
           ShuffleStrategy::NONE,
           "Strategy for introducing delays by shuffling tuples or buffers in the stream. This will be done before any aggregation or join "
           "operator"
           "[NONE|BUFFER|TUPLES|BUFFER_TUPLES]. "};
    NES::Configurations::FloatOption degreeOfDisorder = {
        "degreeOfDisorder", "0.0", "Percentage of degreeOfDisorder", {std::make_shared<NES::Configurations::FloatRangeValidation>(0, 1)}};
    NES::Configurations::UIntOption minDelay
        = {"minDelay", "1", "Minimum delay in milliseconds", {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption maxDelay
        = {"maxDelay", "10", "Maximum delay in milliseconds", {std::make_shared<NES::Configurations::NumberValidation>()}};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &nautilusBackend,
            &pageSize,
            &numberOfPartitions,
            &joinStrategy,
            &sliceStoreType,
            &sliceCacheType,
            &numberOfEntriesSliceCache,
            &lockSliceCache,
            &sortBufferByField,
            &sortBufferOrder,
            &shuffleStrategy,
            &degreeOfDisorder,
            &minDelay,
            &maxDelay,
            &pipelinesTxtFilePath,
            &cacheHitsAndMissesFilePath,
            &probeType,
        &gatherSlices};
    }
};
}
