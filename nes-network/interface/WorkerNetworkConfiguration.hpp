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

namespace NES
{

/// Default configuration for the network layer (sender and receiver).
/// These values serve as worker-level defaults that apply to all NetworkSources and NetworkSinks on this worker.
/// Individual queries may override these defaults via per-channel configuration in the sink/source descriptors.
/// Nested under WorkerConfiguration as `worker.network.*`.
struct WorkerNetworkConfiguration
{
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    WorkerNetworkConfiguration() = delete;

    WorkerNetworkConfiguration(
        uint64_t senderQueueSize, uint64_t maxPendingAcks, uint64_t receiverQueueSize, uint64_t senderIOThreads, uint64_t receiverIOThreads)
        : senderQueueSize(senderQueueSize)
        , maxPendingAcks(maxPendingAcks)
        , receiverQueueSize(receiverQueueSize)
        , senderIOThreads(senderIOThreads)
        , receiverIOThreads(receiverIOThreads)
    {
    }

    /// Default size of the sender software queue per network channel.
    /// May be overridden per NetworkSink via query-specific configuration.
    uint64_t senderQueueSize;
    /// Default maximum number of buffers that can be in-flight (sent but not yet acknowledged) per network channel.
    /// May be overridden per NetworkSink via query-specific configuration.
    uint64_t maxPendingAcks;
    /// Default size of the receiver data queue per network channel.
    /// May be overridden per NetworkSource via query-specific configuration.
    uint64_t receiverQueueSize;
    /// Number of IO threads for the sender tokio runtime. 0 means use the number of available cores.
    uint64_t senderIOThreads;
    /// Number of IO threads for the receiver tokio runtime. 0 means use the number of available cores.
    uint64_t receiverIOThreads;

    static WorkerNetworkConfiguration fromConfig(const InstantiatedConfig& config);
};
}
