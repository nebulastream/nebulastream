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
#include <Configurations/Validation/BooleanValidation.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <Nautilus/NautilusBackend.hpp>
#include <QueryCompiler/Configurations/Enums/CompilationStrategy.hpp>
#include <QueryCompiler/Configurations/Enums/DumpMode.hpp>
#include <QueryCompiler/Configurations/Enums/WindowManagement.hpp>
#include "Enums/HashMapVarSizedStorageMethod.hpp"

namespace NES::QueryCompilation::Configurations
{
static constexpr auto DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES = 100;
static constexpr auto DEFAULT_NUMBER_OF_RECORDS_PER_KEY = 10;
static constexpr auto DEFAULT_PAGED_VECTOR_SIZE = 8196;

class QueryCompilerConfiguration final : public NES::Configurations::BaseConfiguration
{
public:
    QueryCompilerConfiguration() = default;
    QueryCompilerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

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
    NES::Configurations::UIntOption numberOfRecordsPerKey
        = {"numberOfRecordsPerKey",
           std::to_string(DEFAULT_NUMBER_OF_RECORDS_PER_KEY),
           "Expected number of records per key, for example in a hash join",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption pageSize
        = {"pageSize",
           std::to_string(DEFAULT_PAGED_VECTOR_SIZE),
           "Page size of any other paged data structure",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption varSizedPageSize
        = {"varSizedPageSize",
           std::to_string(DEFAULT_PAGED_VECTOR_SIZE),
           "Size of the pages to the chained hashmap to store data on",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::EnumOption<HashMapVarSizedStorageMethod> varSizedStorageMethod
        = {"varSizedStorageMethod",
           HashMapVarSizedStorageMethod::SINGLE_BUFFER,
           "How to store variable sized data in a chained hashmap"
           "[SINGLE_BUFFER|PAGES|JOINT_PAGES]. "};
    NES::Configurations::EnumOption<StreamJoinStrategy> joinStrategy
        = {"joinStrategy",
           StreamJoinStrategy::NESTED_LOOP_JOIN,
           "JoinStrategy"
           "[NESTED_LOOP_JOIN|HASH_JOIN]. "};
    NES::Configurations::StringOption pipelinesTxtFilePath = {"pipelinesTxtFilePath", "pipelines.txt", "Path to dump pipeline details."};
    NES::Configurations::EnumOption<SliceCacheType> sliceCacheType
        = {"sliceCacheType",
           SliceCacheType::NONE,
           "Type of slice cache"
           "[NONE|FIFO|LRU|SECOND_CHANCE]. "};
    NES::Configurations::UIntOption numberOfEntriesSliceCache
        = {"numberOfEntriesSliceCache", "1", "Size of the slice cache", {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::BoolOption sortBufferByTimestamp
        = {"sortBufferByTimestamp",
           "false",
           "Should the sort buffer operator sort by the timestamp field.",
           {std::make_shared<NES::Configurations::BooleanValidation>()}};


private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &nautilusBackend,
            &pageSize,
            &numberOfPartitions,
            &numberOfRecordsPerKey,
            &varSizedPageSize,
            &varSizedStorageMethod,
            &joinStrategy,
            &pipelinesTxtFilePath,
            &sliceCacheType,
            &numberOfEntriesSliceCache,
            &sortBufferByTimestamp};
    }
};
}
