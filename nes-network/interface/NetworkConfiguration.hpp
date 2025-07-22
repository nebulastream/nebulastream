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

#include <memory>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/NumberValidation.hpp>

namespace NES
{

/// Default configuration for the network layer (sender and receiver).
/// These values serve as worker-level defaults that apply to all NetworkSources and NetworkSinks on this worker.
/// Individual queries may override these defaults via per-channel configuration in the sink/source descriptors.
/// Nested under WorkerConfiguration as `worker.network.*`.
class NetworkConfiguration final : public BaseConfiguration
{
public:
    NetworkConfiguration() = default;
    NetworkConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    /// Default size of the sender software queue per network channel.
    /// May be overridden per NetworkSink via query-specific configuration.
    UIntOption senderQueueSize
        = {"sender_queue_size",
           "1024",
           "Default size of the sender software queue per network channel. May be overridden per NetworkSink.",
           {std::make_shared<NumberValidation>()}};

    /// Default maximum number of buffers that can be in-flight (sent but not yet acknowledged) per network channel.
    /// May be overridden per NetworkSink via query-specific configuration.
    UIntOption maxPendingAcks
        = {"max_pending_acks",
           "64",
           "Default maximum number of in-flight buffers awaiting acknowledgment per network channel. May be overridden per NetworkSink.",
           {std::make_shared<NumberValidation>()}};

    /// Default size of the receiver data queue per network channel.
    /// May be overridden per NetworkSource via query-specific configuration.
    UIntOption receiverQueueSize
        = {"receiver_queue_size",
           "10",
           "Default size of the receiver data queue per network channel. May be overridden per NetworkSource.",
           {std::make_shared<NumberValidation>()}};

    /// Number of IO threads for the sender tokio runtime. 0 means use the number of available cores.
    UIntOption senderIOThreads
        = {"sender_io_threads", "1", "Number of IO threads for the sender network runtime. 0 means use the number of available cores."};

    /// Number of IO threads for the receiver tokio runtime. 0 means use the number of available cores.
    UIntOption receiverIOThreads
        = {"receiver_io_threads",
           "1",
           "Number of IO threads for the receiver network runtime. 0 means use the number of available cores."};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &senderQueueSize,
            &maxPendingAcks,
            &receiverQueueSize,
            &senderIOThreads,
            &receiverIOThreads};
    }
};
}
