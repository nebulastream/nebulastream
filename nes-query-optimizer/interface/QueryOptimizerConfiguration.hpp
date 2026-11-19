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
#include <Util/MemoryLayoutType.hpp>
#include <QueryOptimizerNetworkConfiguration.hpp>

namespace NES
{

enum class StreamJoinStrategy : uint8_t
{
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    OPTIMIZER_CHOOSES
};

struct QueryOptimizerConfiguration
{
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    QueryOptimizerConfiguration() = delete;

    QueryOptimizerConfiguration(StreamJoinStrategy joinStrategy, MemoryLayoutType memoryLayout, QueryOptimizerNetworkConfiguration network)
        : joinStrategy(joinStrategy), memoryLayout(memoryLayout), network(network)
    {
    }

    StreamJoinStrategy joinStrategy;
    /// Memory Layout is determined via the config for now.
    MemoryLayoutType memoryLayout;
    QueryOptimizerNetworkConfiguration network;

    static QueryOptimizerConfiguration fromConfig(const InstantiatedConfig& config);
};

}
