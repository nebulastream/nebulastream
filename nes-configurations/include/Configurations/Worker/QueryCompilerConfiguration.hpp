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

#ifndef NES_CORE_INCLUDE_CONFIGURATIONS_WORKER_QUERYCOMPILERCONFIGURATION_HPP_
#define NES_CORE_INCLUDE_CONFIGURATIONS_WORKER_QUERYCOMPILERCONFIGURATION_HPP_

#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Enums/QueryCompilerType.hpp>
#include <Configurations/Enums/CompilationStrategy.hpp>
#include <Configurations/Enums/PipeliningStrategy.hpp>
#include <Configurations/Enums/WindowingStrategy.hpp>
#include <Configurations/Enums/NautilusBackend.hpp>
#include <Configurations/Enums/DumpMode.hpp>
#include <Configurations/Enums/OutputBufferOptimizationLevel.hpp>
#include <Configurations/Enums/MemoryLayoutPolicy.hpp>
#include <Configurations/Enums/QueryExecutionMode.hpp>
#include <Util/Common.hpp>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <utility>

namespace NES::Configurations {

static constexpr auto DEFAULT_HASH_NUM_PARTITIONS = 1;
static constexpr auto DEFAULT_HASH_PAGE_SIZE = 131072;
static constexpr auto DEFAULT_HASH_PREALLOC_PAGE_COUNT = 1;
static constexpr auto DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE = 2 * 1024 * 1024;

/**
 * @brief Configuration for the query compiler
 */
class QueryCompilerConfiguration : public BaseConfiguration {
  public:
    QueryCompilerConfiguration() : BaseConfiguration(){};
    QueryCompilerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description){};

    /**
     * @brief Sets the compilation strategy. We differentiate between FAST, DEBUG, and OPTIMIZED compilation.
     */
    EnumOption<QueryCompilation::QueryCompilerType> queryCompilerType = {
        QUERY_COMPILER_TYPE_CONFIG,
        QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER,
        "Indicates the type for the query compiler [DEFAULT_QUERY_COMPILER|NAUTILUS_QUERY_COMPILER]."};

    /**
     * @brief Sets the dump mode for the query compiler. We differentiate between NONE, CONSOLE, FILE, and FILE_AND_CONSOLE.
     * @note This setting is only for the nautilus compiler
     */
    EnumOption<QueryCompilation::DumpMode> queryCompilerDumpMode = {
        QUERY_COMPILER_DUMP_MODE,
        QueryCompilation::DumpMode::NONE,
        "Indicates the type for the query compiler [NONE|CONSOLE|FILE|FILE_AND_CONSOLE]."};

    /**
     * @brief Sets the compilation strategy. We differentiate between FAST, DEBUG, and OPTIMIZED compilation.
     */
    EnumOption<QueryCompilation::CompilationStrategy> compilationStrategy = {
        QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG,
        QueryCompilation::CompilationStrategy::OPTIMIZE,
        "Indicates the optimization strategy for the query compiler [FAST|DEBUG|OPTIMIZE|PROXY_INLINING]."};

    /**
     * @brief Sets the backend for nautilus. We differentiate between MLIR_COMPILER_BACKEND, INTERPRETER, BC_INTERPRETER_BACKEND, and FLOUNDER_COMPILER_BACKEND compilation.
     */
    EnumOption<QueryCompilation::NautilusBackend> nautilusBackend = {
        QUERY_COMPILER_NAUTILUS_BACKEND_CONFIG,
        QueryCompilation::NautilusBackend::MLIR_COMPILER_BACKEND,
        "Indicates the nautilus backend for the nautilus query compiler "
        "[MLIR_COMPILER_BACKEND|INTERPRETER|BC_INTERPRETER_BACKEND|FLOUNDER_COMPILER_BACKEND]."};

    /**
     * @brief Sets the pipelining strategy. We differentiate between an OPERATOR_FUSION and OPERATOR_AT_A_TIME strategy.
     */
    EnumOption<QueryCompilation::PipeliningStrategy> pipeliningStrategy = {
        QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG,
        QueryCompilation::PipeliningStrategy::OPERATOR_FUSION,
        "Indicates the pipelining strategy for the query compiler [OPERATOR_FUSION|OPERATOR_AT_A_TIME]."};

    /**
     * @brief Sets the output buffer optimization level.
     */
    EnumOption<QueryCompilation::OutputBufferOptimizationLevel> outputBufferOptimizationLevel = {
        QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG,
        QueryCompilation::OutputBufferOptimizationLevel::ALL,
        "Indicates the OutputBufferAllocationStrategy "
        "[ALL|NO|ONLY_INPLACE_OPERATIONS_NO_FALLBACK,"
        "|REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK,|"
        "REUSE_INPUT_BUFFER_NO_FALLBACK|OMIT_OVERFLOW_CHECK_NO_FALLBACK]. "};

    /**
     * @brief Sets the strategy for local window computations.
     */
    EnumOption<QueryCompilation::WindowingStrategy> windowingStrategy = {QUERY_COMPILER_WINDOWING_STRATEGY_CONFIG,
                                                                         QueryCompilation::WindowingStrategy::LEGACY,
                                                                         "Indicates the windowingStrategy "
                                                                         "[LEGACY|SLICING|BUCKETING]. "};

    /**
     * @brief Enables compilation cache
     * */
    BoolOption useCompilationCache = {ENABLE_USE_COMPILATION_CACHE_CONFIG, false, "Enable use compilation caching"};

    /**
     * Config options for hash join
     */
    UIntOption numberOfPartitions = {STREAM_HASH_JOIN_NUMBER_OF_PARTITIONS_CONFIG,
                                     NES::Configurations::DEFAULT_HASH_NUM_PARTITIONS,
                                     "Partitions in the hash table"};
    UIntOption pageSize = {STREAM_HASH_JOIN_PAGE_SIZE_CONFIG,
                           NES::Configurations::DEFAULT_HASH_PAGE_SIZE,
                           "Page size of hash table"};
    UIntOption preAllocPageCnt = {STREAM_HASH_JOIN_PREALLOC_PAGE_COUNT_CONFIG,
                                  NES::Configurations::DEFAULT_HASH_PREALLOC_PAGE_COUNT,
                                  "Page count of pre allocated pages in each bucket hash table"};

    UIntOption maxHashTableSize = {STREAM_HASH_JOIN_MAX_HASH_TABLE_SIZE_CONFIG,
                                   NES::Configurations::DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE,
                                   "Maximum size of hash table"};

    EnumOption<QueryCompilation::StreamJoinStrategy> joinStrategy = {
        JOIN_STRATEGY,
        QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN,
        "Indicates the windowingStrategy"
        "[HASH_JOIN_LOCAL|HASH_JOIN_GLOBAL_LOCKING|HASH_JOIN_GLOBAL_LOCK_FREE|NESTED_LOOP_JOIN]. "};

    /**
     * @brief Sets the path to the locally installed CUDA SDK.
     */
    StringOption cudaSdkPath = {CUDA_SDK_PATH, "/usr/local/cuda", "Path to CUDA SDK."};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
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
            &cudaSdkPath,
            &maxHashTableSize,
            &joinStrategy,
        };
    }
};

}// namespace NES::Configurations

#endif// NES_CORE_INCLUDE_CONFIGURATIONS_WORKER_QUERYCOMPILERCONFIGURATION_HPP_
