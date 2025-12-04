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
#include <Configurations/Validation/FloatValidation.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <Util/ExecutionMode.hpp>
#include <InferenceConfiguration.hpp>

namespace NES
{

static constexpr auto DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES = 100;
static constexpr auto DEFAULT_PAGED_VECTOR_SIZE = 1024;
static constexpr auto DEFAULT_OPERATOR_BUFFER_SIZE = 4096;
static constexpr auto DEFAULT_NUMBER_OF_RECORDS_PER_KEY = 10;
static constexpr auto DEFAULT_MAX_NUMBER_OF_BUCKETS = 10'000.0;

enum class StreamJoinStrategy : uint8_t
{
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    OPTIMIZER_CHOOSES
};

class QueryExecutionConfiguration : public BaseConfiguration
{
public:
    QueryExecutionConfiguration() = default;
    QueryExecutionConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    EnumOption<ExecutionMode> executionMode
        = {"execution_mode",
           ExecutionMode::COMPILER,
           "Execution mode for the query compiler"
           "[COMPILER|INTERPRETER]."};
    UIntOption numberOfPartitions
        = {"number_of_partitions",
           std::to_string(DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES),
           "Partitions in a hash table",
           {std::make_shared<NumberValidation>()}};
    UIntOption pageSize
        = {"page_size",
           std::to_string(DEFAULT_PAGED_VECTOR_SIZE),
           "Page size of any other paged data structure",
           {std::make_shared<NumberValidation>()}};
    UIntOption numberOfRecordsPerKey
        = {"number_of_records_per_key",
           std::to_string(DEFAULT_NUMBER_OF_RECORDS_PER_KEY),
           "Expected number of records per key, for example in a hash join. If set too low or high affects the performance.",
           {std::make_shared<NumberValidation>()}};
    UIntOption maxNumberOfBuckets = {
        "max_number_of_buckets",
        std::to_string(DEFAULT_MAX_NUMBER_OF_BUCKETS),
        "Maximal number of buckets for a hash table. If set too low or high degrades either the performance or increases the memory usage.",
        {std::make_shared<FloatValidation>()}};
    UIntOption operatorBufferSize
        = {"operator_buffer_size",
           std::to_string(DEFAULT_OPERATOR_BUFFER_SIZE),
           "Buffer size of a operator e.g. during scan",
           {std::make_shared<NumberValidation>()}};
    EnumOption<StreamJoinStrategy> joinStrategy
        = {"join_strategy",
           StreamJoinStrategy::OPTIMIZER_CHOOSES,
           "Join Strategy"
           "[NESTED_LOOP_JOIN|HASH_JOIN|OPTIMIZER_CHOOSES]."};

    Configurations::InferenceConfiguration inferenceConfiguration = {"inference", "ML Inference Configuration"};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &executionMode,
            &pageSize,
            &numberOfPartitions,
            &joinStrategy,
            &numberOfRecordsPerKey,
            &maxNumberOfBuckets,
            &operatorBufferSize,
            &inferenceConfiguration};
    }
};

}
