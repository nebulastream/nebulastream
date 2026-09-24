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
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/ExecutionMode.hpp>
#include <BloomFilterConfiguration.hpp>
#include <SliceCacheConfiguration.hpp>

namespace NES
{

struct QueryExecutionConfiguration
{
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    QueryExecutionConfiguration() = delete;

    QueryExecutionConfiguration(
        ExecutionMode executionMode,
        uint64_t numberOfPartitions,
        uint64_t pageSize,
        uint64_t numberOfRecordsPerKey,
        uint64_t operatorBufferSize,
        SliceCacheConfiguration sliceCacheConfiguration,
        BloomFilterConfiguration bloomFilterConfiguration)
        : executionMode(executionMode)
        , numberOfPartitions(numberOfPartitions)
        , pageSize(pageSize)
        , numberOfRecordsPerKey(numberOfRecordsPerKey)
        , operatorBufferSize(operatorBufferSize)
        , sliceCacheConfiguration(sliceCacheConfiguration)
        , bloomFilterConfiguration(bloomFilterConfiguration)
    {
    }

    ExecutionMode executionMode;
    uint64_t numberOfPartitions;
    uint64_t pageSize;
    uint64_t numberOfRecordsPerKey;
    uint64_t operatorBufferSize;
    SliceCacheConfiguration sliceCacheConfiguration;
    BloomFilterConfiguration bloomFilterConfiguration;

    static QueryExecutionConfiguration fromConfig(const InstantiatedConfig& config);
};

}
