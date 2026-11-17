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

/// How the batches of a UDF process hold the fields that its UDFs do not read or write: in Arrow columns as the others (ARROW), or in
/// the row layout of NES in a row region of the same buffer (HYBRID), see ArrowBufferRef. HYBRID needs the protocol
/// SHARED_MEMORY_ZERO_COPY, as the process leaves these fields where they are.
enum class UdfProcessLayout : uint8_t
{
    ARROW,
    HYBRID
};

struct QueryOptimizerConfiguration
{
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    QueryOptimizerConfiguration() = delete;

    QueryOptimizerConfiguration(
        StreamJoinStrategy joinStrategy,
        QueryOptimizerNetworkConfiguration network,
        UdfProcessLayout udfProcessLayout = UdfProcessLayout::ARROW)
        : joinStrategy(joinStrategy), network(network), udfProcessLayout(udfProcessLayout)
    {
    }

    StreamJoinStrategy joinStrategy;
    QueryOptimizerNetworkConfiguration network;
    UdfProcessLayout udfProcessLayout;

    static QueryOptimizerConfiguration fromConfig(const InstantiatedConfig& config);
};

}
