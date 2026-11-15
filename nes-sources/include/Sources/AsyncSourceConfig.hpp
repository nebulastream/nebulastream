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

#include <cstddef>
#include <expected>
#include <memory>
#include <Async/HandoffChannel.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
class InstantiatedConfig;

/// Planner-created configuration. The graph stays typed; descriptor reflection handles wire serialization.
struct AsyncSourceConfig
{
    static constexpr size_t DEFAULT_MAX_CONCURRENCY = 16;
    static constexpr size_t DEFAULT_MAX_RETRIES = 0;

    /// The same channel object is held by the paired Handoff sink configuration.
    std::shared_ptr<HandoffChannel> channel;
    /// Immutable producers, typed constants, scoped inputs, and checked output handles.
    AsyncCallGraph calls;
    /// Rows in flight, further bounded by the stage's functions.
    size_t maxConcurrency = DEFAULT_MAX_CONCURRENCY;
    size_t maxRetries = DEFAULT_MAX_RETRIES;

    /// Async sources need a planner-supplied graph, not frontend literal configuration.
    static std::expected<AsyncSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

}
