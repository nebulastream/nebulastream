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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/UUID.hpp>
#include <HandoffChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Producer half of an asynchronously executed operator: hands buffers to the `AsyncSource` with the
/// same channel on the same worker. When the channel is full, buffers are stashed and the producer's sources are
/// held back until the Async source caught up.
class HandoffSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Handoff";
    static constexpr auto RETRY_INTERVAL = std::chrono::milliseconds(10);
    explicit HandoffSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "HandoffSink(" << channelId << ")"; }

private:
    /// Once the Async source has stopped: drops what is still to be handed off and releases the producer's sources.
    void dropRemaining(const TupleBuffer& buffer);

    std::string channelId;
    size_t channelCapacity;
    std::shared_ptr<HandoffChannel> channel;
    /// Set once the Async source has stopped. Several worker threads may run `execute` at once.
    std::atomic<bool> consumerGone = false;
    /// Applies pressure as soon as the channel rejects a buffer: the channel itself is the buffer that absorbs bursts.
    BackpressureHandler backpressureHandler;
};

/// NOLINTBEGIN(cert-err58-cpp)
struct ConfigParametersHandoff
{
    static inline const DescriptorConfig::ConfigParameter<std::string> CHANNEL{
        "CHANNEL",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            auto value = DescriptorConfig::tryGet(CHANNEL, config);
            if (value && !stringToUUID(*value))
            {
                return std::nullopt;
            }
            return value;
        }};

    /// Only used if the sink starts before the Async source, which then finds the channel this sink created.
    static inline const DescriptorConfig::ConfigParameter<size_t> CHANNEL_CAPACITY{
        "CHANNEL_CAPACITY",
        HandoffChannel::DEFAULT_CAPACITY,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHANNEL_CAPACITY, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, CHANNEL, CHANNEL_CAPACITY);
};

/// NOLINTEND(cert-err58-cpp)

}

FMT_OSTREAM(NES::HandoffSink);
