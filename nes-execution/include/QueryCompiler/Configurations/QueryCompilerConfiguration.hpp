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
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <API/Schema.hpp>
#include <Nautilus/NautilusBackend.hpp>
#include <QueryCompiler/Configurations/Enums/CompilationStrategy.hpp>
#include <QueryCompiler/Configurations/Enums/DumpMode.hpp>
#include "Configurations/BaseOption.hpp"

namespace NES::QueryCompilation::Configurations
{
static constexpr auto DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES = 100;
static constexpr auto DEFAULT_PAGED_VECTOR_SIZE = 1024;

class QueryCompilerConfiguration final : public NES::Configurations::BaseConfiguration
{
public:
    QueryCompilerConfiguration() = default;
    QueryCompilerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) {};

    /// Sets the dump mode for the query compiler. This setting is only for the nautilus compiler
    NES::Configurations::EnumOption<DumpMode> dumpMode
        = {"dumpMode", DumpMode::NONE, "If and where to dump query compilation details [NONE|CONSOLE|FILE|FILE_AND_CONSOLE]."};
    NES::Configurations::StringOption dumpPath = {"dumpPath", "", "Path to dump query compilation details."};
    NES::Configurations::EnumOption<Nautilus::Configurations::NautilusBackend> nautilusBackend
        = {"nautilusBackend",
           Nautilus::Configurations::NautilusBackend::COMPILER,
           "Nautilus backend for the nautilus query compiler "
           "[COMPILER|INTERPRETER]."};
    NES::Configurations::EnumOption<MemorySelectionPhaseType> memorySelectionPhase
        = {"memorySelectionPhase",
           MemorySelectionPhaseType::NONE,
           "Memory layout selection phase for the nautilus query compiler "
           "[FIXED|NONE]."};
    NES::Configurations::EnumOption<Schema::MemoryLayoutType> memoryLayoutType
        = {"memoryLayoutType",
           Schema::MemoryLayoutType::ROW_LAYOUT,
           "Memory layout for the tuple buffer"
           "[ROW_LAYOUT|COLUMNAR_LAYOUT]."};
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

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {&nautilusBackend, &memoryLayoutType, &memorySelectionPhase, &pageSize, &numberOfPartitions, &joinStrategy, &pipelinesTxtFilePath};
    }
};
}
