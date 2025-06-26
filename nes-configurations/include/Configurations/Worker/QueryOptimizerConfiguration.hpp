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
#include <memory>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <SliceStore/FileDescriptor/FileDescriptors.hpp>
#include <SliceStore/WatermarkPredictor/AbstractWatermarkPredictor.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/ExecutionMode.hpp>

namespace NES::Configurations
{

static constexpr auto DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES = 100;
static constexpr auto DEFAULT_PAGED_VECTOR_SIZE = 1024;
static constexpr auto DEFAULT_OPERATOR_BUFFER_SIZE = 4096;
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
    QueryOptimizerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    NES::Configurations::EnumOption<DumpMode> dumpMode
        = {"dumpMode", DumpMode::NONE, "If and where to dump query compilation details [NONE|CONSOLE|FILE|FILE_AND_CONSOLE]."};
    NES::Configurations::StringOption dumpPath = {"dumpPath", "", "Path to dump query compilation details."};
    NES::Configurations::EnumOption<Nautilus::Configurations::ExecutionMode> executionMode
        = {"executionMode",
           Nautilus::Configurations::ExecutionMode::COMPILER,
           "ExecutionMode for the query compiler "
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
    NES::Configurations::UIntOption operatorBufferSize
        = {"operatorBufferSize",
           std::to_string(DEFAULT_OPERATOR_BUFFER_SIZE),
           "Buffer size of a operator e.g. during scan",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::EnumOption<StreamJoinStrategy> joinStrategy
        = {"joinStrategy",
           StreamJoinStrategy::NESTED_LOOP_JOIN,
           "WindowingStrategy"
           "[HASH_JOIN_LOCAL|HASH_JOIN_GLOBAL_LOCKING|HASH_JOIN_GLOBAL_LOCK_FREE|NESTED_LOOP_JOIN]. "};
    NES::Configurations::StringOption pipelinesTxtFilePath = {"pipelinesTxtFilePath", "pipelines.txt", "Path to dump pipeline details."};
    NES::Configurations::EnumOption<SliceStoreType> sliceStoreType
        = {"sliceStoreType",
           SliceStoreType::DEFAULT,
           "Type of slice store "
           "[DEFAULT|FILE_BACKED]."};
    NES::Configurations::UIntOption lowerMemoryBound
        = {"lowerMemoryBound",
           "0",
           "Lower memory bound in bytes for file backed slice store.",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption upperMemoryBound
        = {"upperMemoryBound",
           "0",
           "Upper memory bound in bytes for file backed slice store.",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption maxNumWatermarkGaps
        = {"maxNumWatermarkGaps",
           "0",
           "Maximum number of gaps in watermark processor sequence numbers for watermark prediction.",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption maxNumSequenceNumbers
        = {"maxNumSequenceNumbers",
           std::to_string(UINT64_MAX),
           "Maximum number of watermark processor sequence numbers for watermark prediction.",
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
    NES::Configurations::EnumOption<FileLayout> fileLayout
        = {"fileLayout",
           FileLayout::NO_SEPARATION,
           "File layout for file backed data structures "
           "[NO_SEPARATION_KEEP_KEYS|NO_SEPARATION|SEPARATE_PAYLOAD|SEPARATE_KEYS]."};
    NES::Configurations::BoolOption withPrediction = {"withPrediction", "false", "Predict watermarks for file backed slice store."};
    NES::Configurations::EnumOption<WatermarkPredictorType> watermarkPredictorType
        = {"watermarkPredictorType",
           WatermarkPredictorType::KALMAN,
           "Type of watermark predictor for file backed slice store "
           "[KALMAN|REGRESSION|RLS]."};
    NES::Configurations::UIntOption fileDescriptorGenerationRate
        = {"fileDescriptorGenerationRate",
           "0",
           "Maximum number of file descriptors that are created per second for file backed slice store (0 equals no limit).",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::UIntOption fileDescriptorBufferSize
        = {"fileDescriptorBufferSize",
           std::to_string(DEFAULT_OPERATOR_BUFFER_SIZE),
           "Buffer size of file writers and readers for file backed data structures.",
           {std::make_shared<NES::Configurations::NumberValidation>()}};
    NES::Configurations::StringOption fileBackedWorkingDir
        = {"fileBackedWorkingDir", "", "Working directory for file backed data structures."};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &executionMode,
            &pageSize,
            &numberOfPartitions,
            &joinStrategy,
            &pipelinesTxtFilePath,
            &operatorBufferSize,
            &sliceStoreType,
            &lowerMemoryBound,
            &upperMemoryBound,
            &maxNumWatermarkGaps,
            &maxNumSequenceNumbers,
            &minReadStateSize,
            &minWriteStateSize,
            &fileOperationTimeDelta,
            &fileLayout,
            &withPrediction,
            &watermarkPredictorType,
            &fileDescriptorGenerationRate,
            &fileDescriptorBufferSize,
            &fileBackedWorkingDir};
    }
};

}
