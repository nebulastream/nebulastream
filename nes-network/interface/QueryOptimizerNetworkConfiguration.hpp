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

/// Network configuration overrides for the query optimizer. These are set by the plan decomposer,
/// which sets the network source / sink configuration per query, overriding the worker-level defaults.
class QueryOptimizerNetworkConfiguration : public BaseConfiguration
{
public:
    QueryOptimizerNetworkConfiguration() = default;
    QueryOptimizerNetworkConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    /// Size of the sender software queue per network channel.
    UIntOption senderQueueSize
        = {"sender_queue_size", "1024", "Size of the sender software queue per network channel.", {std::make_shared<NumberValidation>()}};

    /// Maximum number of buffers that can be in-flight (sent but not yet acknowledged) per network channel.
    UIntOption maxPendingAcks
        = {"max_pending_acks",
           "64",
           "Maximum number of in-flight buffers awaiting acknowledgment per network channel",
           {std::make_shared<NumberValidation>()}};

    /// Size of the receiver data queue per network channel.
    UIntOption receiverQueueSize
        = {"receiver_queue_size", "10", "Size of the receiver data queue per network channel", {std::make_shared<NumberValidation>()}};

    /// Number of buffered tuples at which backpressure is acquired on a network channel.
    UIntOption backpressureUpperThreshold
        = {"backpressure_upper_threshold",
           "1000",
           "Number of buffered tuples at which backpressure is acquired per network channel",
           {std::make_shared<NumberValidation>()}};

    /// Number of buffered tuples at which backpressure is released on a network channel.
    UIntOption backpressureLowerThreshold
        = {"backpressure_lower_threshold",
           "200",
           "Number of buffered tuples at which backpressure is released per network channel",
           {std::make_shared<NumberValidation>()}};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {&senderQueueSize, &maxPendingAcks, &receiverQueueSize, &backpressureUpperThreshold, &backpressureLowerThreshold};
    }
};

}
