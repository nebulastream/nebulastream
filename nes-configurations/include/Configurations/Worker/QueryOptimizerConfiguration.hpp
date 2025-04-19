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
#include <Configurations/BaseOption.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <Nautilus/NautilusBackend.hpp>

namespace NES::Configurations
{

static constexpr auto DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES = 100;
static constexpr auto DEFAULT_PAGED_VECTOR_SIZE = 1024;
static constexpr auto DEFAULT_BUFFER_SIZE = 1024;
enum class StreamJoinStrategy : uint8_t
{
    NESTED_LOOP_JOIN
};
enum class DumpMode : uint8_t
{
    /// Disables all dumping
    NONE,
    /// Dumps intermediate representations to console, std:out
    CONSOLE,
    /// Dumps intermediate representations to file
    FILE,
    /// Dumps intermediate representations to console and file
    FILE_AND_CONSOLE
};

class QueryOptimizerConfiguration : public BaseConfiguration
{
public:
    QueryOptimizerConfiguration() = default;
    QueryOptimizerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) {};

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
    NES::Configurations::UIntOption bufferSize
        = {"bufferSize",
           std::to_string(DEFAULT_BUFFER_SIZE),
           "Buffer size e.g. during scan",
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
        return {&nautilusBackend, &pageSize, &numberOfPartitions, &joinStrategy, &pipelinesTxtFilePath};
    }
};

}
