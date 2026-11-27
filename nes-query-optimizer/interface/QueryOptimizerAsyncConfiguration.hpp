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
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// Settings of asynchronously executed stages. The plan decomposer passes them to the Async source and the Handoff sink
/// of every stage it splits. Every field is optional: a value that is not set leaves the default of the source and the
/// sink in place.
struct QueryOptimizerAsyncConfiguration
{
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    QueryOptimizerAsyncConfiguration() = delete;

    QueryOptimizerAsyncConfiguration(
        std::optional<uint64_t> maxConcurrency, std::optional<uint64_t> channelCapacity, std::optional<uint64_t> maxRetries)
        : maxConcurrency(maxConcurrency), channelCapacity(channelCapacity), maxRetries(maxRetries)
    {
    }

    /// Rows of one async stage whose calls are in flight at the same time. A function of the stage may lower it.
    std::optional<uint64_t> maxConcurrency;
    /// Buffers that wait between the producer half of an async stage and its calls before the producer is held back.
    std::optional<uint64_t> channelCapacity;
    /// Further attempts of a failed async call, the n-th after 100 ms * 2^n.
    std::optional<uint64_t> maxRetries;

    static QueryOptimizerAsyncConfiguration fromConfig(const InstantiatedConfig& config);
};

}
