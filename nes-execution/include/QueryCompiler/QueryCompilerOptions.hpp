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

#include <cstdint>
#include <Configurations/Enums/CompilationStrategy.hpp>
#include <Configurations/Enums/DumpMode.hpp>
#include <Configurations/Enums/NautilusBackend.hpp>
#include <Configurations/Enums/OutputBufferOptimizationLevel.hpp>
#include <Configurations/Enums/PipeliningStrategy.hpp>
#include <Configurations/Enums/QueryCompilerType.hpp>
#include <Configurations/Enums/WindowingStrategy.hpp>
#include <QueryCompiler/Phases/OutputBufferAllocationStrategies.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Util/Common.hpp>

namespace NES::QueryCompilation
{

struct QueryCompilerOptions
{
    enum class FilterProcessingStrategy : uint8_t
    {
        BRANCHED, /// Uses a branches to process filter expressions
        PREDICATION /// Uses predication for filter expressions if possible
    };

    struct StreamHashJoinOptions
    {
        uint64_t numberOfPartitions = Configurations::DEFAULT_HASH_NUM_PARTITIONS;
        uint64_t pageSize = Configurations::DEFAULT_HASH_PAGE_SIZE;
        uint64_t preAllocPageCnt = Configurations::DEFAULT_HASH_PREALLOC_PAGE_COUNT;
        uint64_t totalSizeForDataStructures = Configurations::DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE;
    } __attribute__((aligned(32)));

    uint64_t numSourceLocalBuffers = 64;
    OutputBufferOptimizationLevel outputBufferOptimizationLevel = OutputBufferOptimizationLevel::ALL;
    PipeliningStrategy pipeliningStrategy = PipeliningStrategy::OPERATOR_FUSION;
    CompilationStrategy compilationStrategy = CompilationStrategy::OPTIMIZE;
    FilterProcessingStrategy filterProcessingStrategy = FilterProcessingStrategy::BRANCHED;
    WindowingStrategy windowingStrategy = WindowingStrategy::SLICING;
    QueryCompilerType queryCompiler = QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NautilusBackend nautilusBackend = NautilusBackend::MLIR_COMPILER_BACKEND;
    DumpMode dumpMode = DumpMode::FILE_AND_CONSOLE;
    std::string dumpPath;
    StreamJoinStrategy joinStrategy = StreamJoinStrategy::NESTED_LOOP_JOIN;
    StreamHashJoinOptions hashJoinOptions;
} __attribute__((aligned(64)));


/// TODO(#122): Refactor QueryCompilerConfiguration
[[maybe_unused]]
static std::shared_ptr<QueryCompilerOptions>
queryCompilationOptionsFromConfig(const Configurations::QueryCompilerConfiguration& queryCompilerConfiguration)
{
    auto options = std::make_shared<QueryCompilerOptions>();

    options->compilationStrategy = queryCompilerConfiguration.compilationStrategy;
    options->pipeliningStrategy = queryCompilerConfiguration.pipeliningStrategy;
    options->outputBufferOptimizationLevel = queryCompilerConfiguration.outputBufferOptimizationLevel;

    if (queryCompilerConfiguration.queryCompilerType == QueryCompilerType::NAUTILUS_QUERY_COMPILER
        && queryCompilerConfiguration.windowingStrategy == WindowingStrategy::LEGACY)
    {
        options->windowingStrategy = WindowingStrategy::SLICING;
    }
    else
    {
        options->windowingStrategy = queryCompilerConfiguration.windowingStrategy;
    }

    options->queryCompiler = queryCompilerConfiguration.queryCompilerType;
    options->dumpMode = queryCompilerConfiguration.queryCompilerDumpMode;
    options->nautilusBackend = queryCompilerConfiguration.nautilusBackend;

    options->hashJoinOptions.numberOfPartitions = queryCompilerConfiguration.numberOfPartitions.getValue();
    options->hashJoinOptions.pageSize = queryCompilerConfiguration.pageSize.getValue();
    options->hashJoinOptions.preAllocPageCnt = queryCompilerConfiguration.preAllocPageCnt.getValue();
    /// zero indicate that it has not been set in the yaml config
    if (queryCompilerConfiguration.maxHashTableSize.getValue() != 0)
    {
        options->hashJoinOptions.totalSizeForDataStructures = queryCompilerConfiguration.maxHashTableSize.getValue();
    }

    options->joinStrategy = queryCompilerConfiguration.joinStrategy;

    return options;
}
}
