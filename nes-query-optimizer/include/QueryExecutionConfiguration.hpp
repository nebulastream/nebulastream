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
#include <Util/ExecutionMode.hpp>

namespace NES
{

static constexpr auto DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES = 100;
static constexpr auto DEFAULT_PAGED_VECTOR_SIZE = 1024;
static constexpr auto DEFAULT_OPERATOR_BUFFER_SIZE = 4096;

enum class StreamJoinStrategy : uint8_t
{
    NESTED_LOOP_JOIN
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
    UIntOption operatorBufferSize
        = {"operator_buffer_size",
           std::to_string(DEFAULT_OPERATOR_BUFFER_SIZE),
           "Buffer size of a operator e.g. during scan",
           {std::make_shared<NumberValidation>()}};
    EnumOption<StreamJoinStrategy> joinStrategy
        = {"join_strategy",
           StreamJoinStrategy::NESTED_LOOP_JOIN,
           "Join Strategy"
           "[NESTED_LOOP_JOIN]. "};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {&executionMode, &pageSize, &numberOfPartitions, &joinStrategy, &operatorBufferSize};
    }
};

}
