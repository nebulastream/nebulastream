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

#include <string>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Enums/CompilationStrategy.hpp>
#include <Configurations/Enums/DumpMode.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/Enums/NautilusBackend.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/BooleanValidation.hpp>
#include <Configurations/Validation/FloatValidation.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <Util/Common.hpp>

namespace NES::Configurations
{

static constexpr auto DEFAULT_HASH_NUM_PARTITIONS = 1;
static constexpr auto DEFAULT_HASH_PAGE_SIZE = 131072;
static constexpr auto DEFAULT_HASH_PREALLOC_PAGE_COUNT = 1;
static constexpr auto DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE = 2 * 1024 * 1024;
static constexpr auto DEFAULT_PAGED_VECTOR_SIZE = 1024;

class QueryCompilerConfiguration : public BaseConfiguration
{
public:
    QueryCompilerConfiguration() = default;
    QueryCompilerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description){};

    /// Sets the dump mode for the query compiler. This setting is only for the nautilus compiler
    EnumOption<QueryCompilation::DumpMode> queryCompilerDumpMode
        = {QUERY_COMPILER_DUMP_MODE,
           QueryCompilation::DumpMode::NONE,
           "If and where to dump query compilation details [NONE|CONSOLE|FILE|FILE_AND_CONSOLE]."};

    StringOption queryCompilerDumpPath = {QUERY_COMPILER_DUMP_PATH, "", "Path to dump query compilation details."};

    EnumOption<QueryCompilation::CompilationStrategy> compilationStrategy
        = {QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG,
           QueryCompilation::CompilationStrategy::OPTIMIZE,
           "Optimization strategy for the query compiler [FAST|DEBUG|OPTIMIZE|PROXY_INLINING]."};

    EnumOption<QueryCompilation::NautilusBackend> nautilusBackend
        = {QUERY_COMPILER_NAUTILUS_BACKEND_CONFIG,
           QueryCompilation::NautilusBackend::COMPILER,
           "Nautilus backend for the nautilus query compiler "
           "[COMPILER|INTERPRETER]."};

    BoolOption useCompilationCache
        = {ENABLE_USE_COMPILATION_CACHE_CONFIG, "false", "Enable use compilation caching", {std::make_shared<BooleanValidation>()}};

    /// Hash Join Options
    UIntOption numberOfPartitions
        = {STREAM_JOIN_NUMBER_OF_PARTITIONS_CONFIG,
           std::to_string(NES::Configurations::DEFAULT_HASH_NUM_PARTITIONS),
           "Partitions in the hash table",
           {std::make_shared<NumberValidation>()}};
    UIntOption pageSize
        = {STREAM_JOIN_PAGE_SIZE_CONFIG,
           std::to_string(NES::Configurations::DEFAULT_HASH_PAGE_SIZE),
           "Page size of hash table",
           {std::make_shared<NumberValidation>()}};
    UIntOption preAllocPageCnt
        = {STREAM_JOIN_PREALLOC_PAGE_COUNT_CONFIG,
           std::to_string(NES::Configurations::DEFAULT_HASH_PREALLOC_PAGE_COUNT),
           "Page count of pre allocated pages in each bucket hash table",
           {std::make_shared<NumberValidation>()}};

    UIntOption maxHashTableSize
        = {STREAM_JOIN_MAX_HASH_TABLE_SIZE_CONFIG,
           std::to_string(NES::Configurations::DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE),
           "Maximum size of hash table",
           {std::make_shared<NumberValidation>()}};

    EnumOption<QueryCompilation::StreamJoinStrategy> joinStrategy
        = {JOIN_STRATEGY,
           QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN,
           "WindowingStrategy"
           "[HASH_JOIN_LOCAL|HASH_JOIN_GLOBAL_LOCKING|HASH_JOIN_GLOBAL_LOCK_FREE|NESTED_LOOP_JOIN]. "};

    EnumOption<QueryCompilation::SliceStoreType> sliceStoreType
        = {SLICE_STORE_TYPE,
           QueryCompilation::SliceStoreType::MAP,
           "Type of slice store"
           "[MAP|LIST]. "};

    EnumOption<QueryCompilation::SliceCacheType> sliceCacheType
        = {SLICE_CACHE_TYPE,
           QueryCompilation::SliceCacheType::DEFAULT,
           "Type of slice cache"
           "[DEFAULT|FIFO|LRU]. "};

    UIntOption sliceCacheSize = {SLICE_CACHE_SIZE, "1", "Size of the slice cache", {std::make_shared<NumberValidation>()}};

    BoolOption lockSliceCache = {LOCK_SLICE_CACHE, "false", "Enable lock in slice cache", {std::make_shared<BooleanValidation>()}};

    StringOption sortBufferByField = {SORT_BUFFER_BY_FIELD, "", "Sort field identifier for the sort buffer operator."};

    StringOption sortBufferOrder = {SORT_BUFFER_ORDER, "Ascending", "Sort order for the sort buffer operator."};

    FloatOption unorderedness = {UNORDEREDNESS, "0.0", "Percentage of unorderedness", {std::make_shared<FloatValidation>()}};

    UIntOption minDelay = {MIN_DELAY, "1", "Minimum delay", {std::make_shared<NumberValidation>()}};

    UIntOption maxDelay = {MAX_DELAY, "10", "Maximum delay", {std::make_shared<NumberValidation>()}};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &compilationStrategy,
            &nautilusBackend,
            &useCompilationCache,
            &numberOfPartitions,
            &pageSize,
            &preAllocPageCnt,
            &maxHashTableSize,
            &joinStrategy,
            &sliceStoreType,
            &sliceCacheType,
            &sliceCacheSize,
            &lockSliceCache,
            &sortBufferByField,
            &sortBufferOrder,
            &unorderedness,
            &minDelay,
            &maxDelay};
    }
};

}
