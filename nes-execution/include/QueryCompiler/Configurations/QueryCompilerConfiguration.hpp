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
#include <Execution/Operators/SliceStore/FileDescriptor/FileDescriptors.hpp>
#include <Execution/Operators/SliceStore/WatermarkPredictor/AbstractWatermarkPredictor.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Nautilus/NautilusBackend.hpp>
#include <QueryCompiler/Configurations/Enums/CompilationStrategy.hpp>
#include <QueryCompiler/Configurations/Enums/DumpMode.hpp>

namespace NES::QueryCompilation::Configurations
{
static constexpr auto DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES = 100;
static constexpr auto DEFAULT_PAGED_VECTOR_SIZE = 1024;

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
    NES::Configurations::UIntOption numWatermarkGapsAllowed
        = {"numWatermarkGapsAllowed",
           "10",
           "Maximum number of gaps in watermark processor sequence numbers for watermark prediction.",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption maxNumSequenceNumbers
        = {"maxNumSequenceNumbers",
           std::to_string(UINT64_MAX),
           "Maximum number of watermark processor sequence numbers for watermark prediction.",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption fileDescriptorBufferSize
        = {"fileDescriptorBufferSize",
           "4096",
           "Buffer size of file writers and readers for file backed data structures.",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption minReadStateSize
        = {"minReadStateSize",
           "0",
           "Minimum state size per slice and thread to be read back to memory.",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption minWriteStateSize
        = {"minWriteStateSize",
           "0",
           "Minimum state size per slice and thread to be written to file.",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption fileOperationTimeDelta
        = {"fileOperationTimeDelta",
           "0",
           "Time delta added to watermark predictions to account for execution time.",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::EnumOption<Runtime::Execution::FileLayout> fileLayout
        = {"fileLayout",
           Runtime::Execution::FileLayout::NO_SEPARATION,
           "File layout for file backed data structures "
           "[NO_SEPARATION_KEEP_KEYS|NO_SEPARATION|SEPARATE_PAYLOAD|SEPARATE_KEYS]."};
    NES::Configurations::EnumOption<Runtime::Execution::WatermarkPredictorType> watermarkPredictorType
        = {"watermarkPredictorType",
           Runtime::Execution::WatermarkPredictorType::KALMAN,
           "Type of watermark predictor "
           "[KALMAN|REGRESSION|RLS]."};
    NES::Configurations::EnumOption<Runtime::Execution::SliceStoreType> sliceStoreType
        = {"sliceStoreType",
           Runtime::Execution::SliceStoreType::DEFAULT,
           "Type of slice store "
           "[DEFAULT|FILE_BACKED]."};
    NES::Configurations::StringOption fileBackedWorkingDir
        = {"fileBackedWorkingDir", "", "Working directory for file backed data structures."};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &nautilusBackend,
            &pageSize,
            &numberOfPartitions,
            &joinStrategy,
            &pipelinesTxtFilePath,
            &numWatermarkGapsAllowed,
            &maxNumSequenceNumbers,
            &fileDescriptorBufferSize,
            &minReadStateSize,
            &minWriteStateSize,
            &fileOperationTimeDelta,
            &fileLayout,
            &watermarkPredictorType,
            &sliceStoreType,
            &fileBackedWorkingDir};
    }
};
}
