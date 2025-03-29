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
#include <Util/Common.hpp>

namespace NES::Optimizer
{

struct OptimizerConfiguration
{
    enum class FilterProcessingStrategy : uint8_t
    {
        BRANCHED, /// Uses a branches to process filter functions
        PREDICATION /// Uses predication for filter functions if possible
    };

    struct WindowOperatorOptions
    {
        uint64_t numberOfPartitions = Configurations::DEFAULT_NUMBER_OF_PARTITIONS;
        uint64_t pageSize = Configurations::DEFAULT_PAGED_VECTOR_SIZE;
        uint64_t preAllocPageCnt = Configurations::DEFAULT_HASH_PREALLOC_PAGE_COUNT;
        uint64_t totalSizeForDataStructures = Configurations::DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE;
    } __attribute__((aligned(32)));

    uint64_t numSourceLocalBuffers = 64;
    CompilationStrategy compilationStrategy = CompilationStrategy::OPTIMIZE;
    FilterProcessingStrategy filterProcessingStrategy = FilterProcessingStrategy::BRANCHED;
    QueryCompilation::NautilusBackend nautilusBackend = QueryCompilation::NautilusBackend::COMPILER;
    QueryCompilation::DumpMode dumpMode = QueryCompilation::DumpMode::CONSOLE;
    std::string dumpPath;
    WindowOperatorOptions windowOperatorOptions;
} __attribute__((aligned(64)));

/// TODO #122: Refactor
[[maybe_unused]]
static std::shared_ptr<OptimizerConfiguration>
queryCompilationOptionsFromConfig(const Configurations::QueryCompilerConfiguration& conf)
{
    auto options = std::make_shared<OptimizerConfiguration>();

    options->compilationStrategy = conf.compilationStrategy;
    options->dumpMode = conf.queryCompilerDumpMode;
    options->nautilusBackend = conf.nautilusBackend;
    options->windowOperatorOptions.numberOfPartitions = conf.numberOfPartitions.getValue();
    options->windowOperatorOptions.pageSize = conf.pageSize.getValue();
    options->windowOperatorOptions.preAllocPageCnt = conf.preAllocPageCnt.getValue();
    /// zero indicate that it has not been set in the yaml config
    if (conf.maxHashTableSize.getValue() != 0)
    {
        options->windowOperatorOptions.totalSizeForDataStructures = conf.maxHashTableSize.getValue();
    }
    return options;
}
}
