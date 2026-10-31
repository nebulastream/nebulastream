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
#include <string>
#include <utility>
#include <vector>
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
        BloomFilterConfiguration bloomFilterConfiguration,
        std::vector<std::string> pythonUdfImportPaths)
        : executionMode(executionMode)
        , numberOfPartitions(numberOfPartitions)
        , pageSize(pageSize)
        , numberOfRecordsPerKey(numberOfRecordsPerKey)
        , operatorBufferSize(operatorBufferSize)
        , sliceCacheConfiguration(sliceCacheConfiguration)
        , bloomFilterConfiguration(bloomFilterConfiguration)
        , pythonUdfImportPaths(std::move(pythonUdfImportPaths))
    {
    }

    ExecutionMode executionMode;
    uint64_t numberOfPartitions;
    uint64_t pageSize;
    uint64_t numberOfRecordsPerKey;
    uint64_t operatorBufferSize;
    SliceCacheConfiguration sliceCacheConfiguration;
    BloomFilterConfiguration bloomFilterConfiguration;
    /// Directories from which inline Python UDFs may import user-provided modules.
    std::vector<std::string> pythonUdfImportPaths;

    [[nodiscard]] const std::vector<std::string>& getPythonUdfImportPaths() const { return pythonUdfImportPaths; }

    static QueryExecutionConfiguration fromConfig(const InstantiatedConfig& config);
};

}
