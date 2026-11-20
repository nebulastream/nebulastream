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
#include <QueryOptimizerNetworkConfiguration.hpp>

namespace NES
{

enum class StreamJoinStrategy : uint8_t
{
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    OPTIMIZER_CHOOSES
};

/// How the planner lays out the buffers between two pipelines, where neither a source nor the sink fixes the layout.
enum class MemoryLayoutPolicy : uint8_t
{
    /// A row per record.
    ROW,
    /// A column per field.
    COLUMNAR
};

struct QueryOptimizerConfiguration
{
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    QueryOptimizerConfiguration() = delete;

    QueryOptimizerConfiguration(
        StreamJoinStrategy joinStrategy, MemoryLayoutPolicy memoryLayout, bool finiteInputs, QueryOptimizerNetworkConfiguration network)
        : joinStrategy(joinStrategy), memoryLayout(memoryLayout), finiteInputs(finiteInputs), network(network)
    {
    }

    StreamJoinStrategy joinStrategy;
    MemoryLayoutPolicy memoryLayout;
    /// All inputs end (e.g. files): joins and aggregations without a window get one window over the whole input, which
    /// fires at the end of the stream. Without it, the planner rejects them, as they would never emit on an endless stream.
    bool finiteInputs;
    QueryOptimizerNetworkConfiguration network;

    static QueryOptimizerConfiguration fromConfig(const InstantiatedConfig& config);
};

}
