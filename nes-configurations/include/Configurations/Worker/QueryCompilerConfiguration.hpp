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
#include <Configurations/Enums/CompilationStrategy.hpp>
#include <Configurations/Enums/DumpMode.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/Enums/NautilusBackend.hpp>
#include <Configurations/Enums/OutputBufferOptimizationLevel.hpp>
#include <Configurations/Enums/PipeliningStrategy.hpp>
#include <Configurations/Enums/QueryCompilerType.hpp>
#include <Configurations/Enums/WindowingStrategy.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/BooleanValidation.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <Util/Common.hpp>

namespace NES::Configurations
{

static constexpr auto DEFAULT_HASH_NUM_PARTITIONS = 1;
static constexpr auto DEFAULT_HASH_PAGE_SIZE = 131072;
static constexpr auto DEFAULT_HASH_PREALLOC_PAGE_COUNT = 1;
static constexpr auto DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE = 2 * 1024 * 1024;

class QueryCompilerConfiguration : public BaseConfiguration
{
public:
    QueryCompilerConfiguration() = default;
    QueryCompilerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) {};

    EnumOption<QueryCompilation::QueryCompilerType> queryCompilerType
        = {QUERY_COMPILER_TYPE_CONFIG,
           QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER,
           "Type for the query compiler [DEFAULT_QUERY_COMPILER|NAUTILUS_QUERY_COMPILER]."};

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
           QueryCompilation::NautilusBackend::MLIR_COMPILER_BACKEND,
           "Nautilus backend for the nautilus query compiler "
           "[MLIR_COMPILER_BACKEND|INTERPRETER|BC_INTERPRETER_BACKEND|FLOUNDER_COMPILER_BACKEND]."};

    EnumOption<QueryCompilation::PipeliningStrategy> pipeliningStrategy
        = {QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG,
           QueryCompilation::PipeliningStrategy::OPERATOR_FUSION,
           "Pipelining strategy for the query compiler [OPERATOR_FUSION|OPERATOR_AT_A_TIME]."};

    EnumOption<QueryCompilation::OutputBufferOptimizationLevel> outputBufferOptimizationLevel
        = {QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG,
           QueryCompilation::OutputBufferOptimizationLevel::ALL,
           "OutputBufferAllocationStrategy "
           "[ALL|NO|ONLY_INPLACE_OPERATIONS_NO_FALLBACK,"
           "|REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK,|"
           "REUSE_INPUT_BUFFER_NO_FALLBACK|OMIT_OVERFLOW_CHECK_NO_FALLBACK]. "};

    EnumOption<QueryCompilation::WindowingStrategy> windowingStrategy
        = {QUERY_COMPILER_WINDOWING_STRATEGY_CONFIG,
           QueryCompilation::WindowingStrategy::LEGACY,
           "windowingStrategy "
           "[LEGACY|SLICING|BUCKETING]. "};

    BoolOption useCompilationCache
        = {ENABLE_USE_COMPILATION_CACHE_CONFIG, "false", "Enable use compilation caching", {std::make_shared<BooleanValidation>()}};

    /// Hash Join Options
    UIntOption numberOfPartitions
        = {STREAM_HASH_JOIN_NUMBER_OF_PARTITIONS_CONFIG,
           std::to_string(NES::Configurations::DEFAULT_HASH_NUM_PARTITIONS),
           "Partitions in the hash table",
           {std::make_shared<NumberValidation>()}};
    UIntOption pageSize
        = {STREAM_HASH_JOIN_PAGE_SIZE_CONFIG,
           std::to_string(NES::Configurations::DEFAULT_HASH_PAGE_SIZE),
           "Page size of hash table",
           {std::make_shared<NumberValidation>()}};
    UIntOption preAllocPageCnt
        = {STREAM_HASH_JOIN_PREALLOC_PAGE_COUNT_CONFIG,
           std::to_string(NES::Configurations::DEFAULT_HASH_PREALLOC_PAGE_COUNT),
           "Page count of pre allocated pages in each bucket hash table",
           {std::make_shared<NumberValidation>()}};

    UIntOption maxHashTableSize
        = {STREAM_HASH_JOIN_MAX_HASH_TABLE_SIZE_CONFIG,
           std::to_string(NES::Configurations::DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE),
           "Maximum size of hash table",
           {std::make_shared<NumberValidation>()}};

    EnumOption<QueryCompilation::StreamJoinStrategy> joinStrategy
        = {JOIN_STRATEGY,
           QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN,
           "WindowingStrategy"
           "[HASH_JOIN_LOCAL|HASH_JOIN_GLOBAL_LOCKING|HASH_JOIN_GLOBAL_LOCK_FREE|NESTED_LOOP_JOIN]. "};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &queryCompilerType,
            &compilationStrategy,
            &pipeliningStrategy,
            &nautilusBackend,
            &outputBufferOptimizationLevel,
            &windowingStrategy,
            &useCompilationCache,
            &numberOfPartitions,
            &pageSize,
            &preAllocPageCnt,
            &maxHashTableSize,
            &joinStrategy,
        };
    }
};

} /// namespace NES::Configurations
