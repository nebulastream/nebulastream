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
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/Phases/OutputBufferAllocationStrategies.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/Common.hpp>

namespace NES::QueryCompilation
{

struct QueryCompilerOptions
{
    enum class FilterProcessingStrategy : uint8_t
    {
        BRANCHED, /// Uses a branches to process filter functions
        PREDICATION /// Uses predication for filter functions if possible
    };

    struct StreamJoinOptions
    {
        uint64_t numberOfPartitions = Configurations::DEFAULT_HASH_NUM_PARTITIONS;
        uint64_t pageSize = Configurations::DEFAULT_PAGED_VECTOR_SIZE;
        uint64_t preAllocPageCnt = Configurations::DEFAULT_HASH_PREALLOC_PAGE_COUNT;
        uint64_t totalSizeForDataStructures = Configurations::DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE;
    } __attribute__((aligned(32)));

    uint64_t numSourceLocalBuffers = 64;
    CompilationStrategy compilationStrategy = CompilationStrategy::OPTIMIZE;
    FilterProcessingStrategy filterProcessingStrategy = FilterProcessingStrategy::BRANCHED;
    NautilusBackend nautilusBackend = NautilusBackend::COMPILER;
    DumpMode dumpMode = DumpMode::FILE_AND_CONSOLE;
    std::string dumpPath;
    StreamJoinStrategy joinStrategy = StreamJoinStrategy::NESTED_LOOP_JOIN;
    StreamJoinOptions joinOptions;
    SliceCacheType sliceCacheType = SliceCacheType::DEFAULT;
    uint64_t sliceCacheSize = 1;
} __attribute__((aligned(64)));
using QueryCompilerOptionsPtr = std::shared_ptr<QueryCompilerOptions>;

/// TODO #122: Refactor QueryCompilerConfiguration
[[maybe_unused]]
static std::shared_ptr<QueryCompilerOptions>
queryCompilationOptionsFromConfig(const Configurations::QueryCompilerConfiguration& queryCompilerConfiguration)
{
    auto options = std::make_shared<QueryCompilerOptions>();

    options->compilationStrategy = queryCompilerConfiguration.compilationStrategy;
    options->dumpMode = queryCompilerConfiguration.queryCompilerDumpMode;
    options->nautilusBackend = queryCompilerConfiguration.nautilusBackend;
    options->joinOptions.numberOfPartitions = queryCompilerConfiguration.numberOfPartitions.getValue();
    options->joinOptions.pageSize = queryCompilerConfiguration.pageSize.getValue();
    options->joinOptions.preAllocPageCnt = queryCompilerConfiguration.preAllocPageCnt.getValue();
    /// zero indicate that it has not been set in the yaml config
    if (queryCompilerConfiguration.maxHashTableSize.getValue() != 0)
    {
        options->joinOptions.totalSizeForDataStructures = queryCompilerConfiguration.maxHashTableSize.getValue();
    }

    options->joinStrategy = queryCompilerConfiguration.joinStrategy;
    options->sliceCacheType = queryCompilerConfiguration.sliceCacheType;
    options->sliceCacheSize = queryCompilerConfiguration.sliceCacheSize.getValue();

    return options;
}
}
