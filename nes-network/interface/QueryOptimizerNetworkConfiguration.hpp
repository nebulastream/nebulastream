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
#include <optional>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// Network configuration overrides for the query optimizer. These are set by the plan decomposer,
/// which sets the network source / sink configuration per query, overriding the worker-level defaults.
/// Every field is optional: only explicitly passed values override the worker-level defaults.
struct QueryOptimizerNetworkConfiguration
{
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    std::optional<uint64_t> senderQueueSize;
    std::optional<uint64_t> maxPendingAcks;
    std::optional<uint64_t> receiverQueueSize;
    std::optional<uint64_t> backpressureUpperThreshold;
    std::optional<uint64_t> backpressureLowerThreshold;

    static QueryOptimizerNetworkConfiguration fromConfig(const InstantiatedConfig& config);
};

}
