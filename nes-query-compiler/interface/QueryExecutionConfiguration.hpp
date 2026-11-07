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
        uint64_t numberOfRecordsPerKey,
        uint64_t operatorBufferSize,
        uint64_t unboundedSliceSizeMs,
        uint64_t countDistinctNumberOfBuckets,
        uint64_t unboundedAggregationPartitions,
        SliceCacheConfiguration sliceCacheConfiguration,
        BloomFilterConfiguration bloomFilterConfiguration,
        bool nautilusPerf = false)
        : executionMode(executionMode)
        , numberOfPartitions(numberOfPartitions)
        , numberOfRecordsPerKey(numberOfRecordsPerKey)
        , operatorBufferSize(operatorBufferSize)
        , unboundedSliceSizeMs(unboundedSliceSizeMs)
        , countDistinctNumberOfBuckets(countDistinctNumberOfBuckets)
        , unboundedAggregationPartitions(unboundedAggregationPartitions)
        , sliceCacheConfiguration(sliceCacheConfiguration)
        , bloomFilterConfiguration(bloomFilterConfiguration)
        , nautilusPerf(nautilusPerf)
    {
    }

    ExecutionMode executionMode;
    uint64_t numberOfPartitions;
    uint64_t numberOfRecordsPerKey;
    uint64_t operatorBufferSize;
    uint64_t unboundedSliceSizeMs;
    uint64_t countDistinctNumberOfBuckets;
    /// Number of partitions of the keys of an unbounded aggregation, whose slices are merged and applied in parallel.
    uint64_t unboundedAggregationPartitions;
    SliceCacheConfiguration sliceCacheConfiguration;
    BloomFilterConfiguration bloomFilterConfiguration;
    /// Compiled pipelines keep frame pointers and write perf jitdump records, so that `perf record -g` + `perf inject --jit` can
    /// walk through and name their frames.
    bool nautilusPerf;

    static QueryExecutionConfiguration fromConfig(const InstantiatedConfig& config);
};

}
