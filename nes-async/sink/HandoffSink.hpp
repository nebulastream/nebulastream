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

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Async/HandoffChannel.hpp>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// The producer half of an asynchronously executed operator: it ends the upstream
/// pipeline and hands its buffers to the consumer half through an in-process channel.
///
/// Both halves live in the same process and find each other through `HandoffChannel`'s
/// registry, keyed by the channel id in their descriptors. Buffers are passed on without
/// copying — unlike the network sink, nothing is serialized here.
///
/// When the channel is full this sink behaves exactly like `NetworkSink` does with a full
/// send queue: it stashes the buffer in a `BackpressureHandler` and asks the engine to
/// repeat the task shortly. That is also what throttles the upstream sources, since
/// backpressure does not span the two query plans.
class HandoffSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Handoff";

    explicit HandoffSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    static constexpr std::chrono::milliseconds RETRY_INTERVAL{10};

    std::string channelId;
    size_t channelCapacity;
    std::shared_ptr<HandoffChannel> channel;
    BackpressureHandler backpressureHandler;
};

struct ConfigParametersHandoffSink
{
    /// Buffers are handed over as they are, so there is nothing to format. Forced to NATIVE
    /// regardless of what the caller passes: any other value would make the pipelining phase
    /// insert a formatting emit operator and write, say, CSV into the channel — which the
    /// consumer source, reading raw records, could not interpret.
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> OUTPUT_FORMAT{
        "OUTPUT_FORMAT", "NATIVE", [](const std::unordered_map<std::string, std::string>&) { return std::optional("NATIVE"); }};

    /// Identifies the channel; the consumer source carries the same value.
    static inline const DescriptorConfig::ConfigParameter<std::string> CHANNEL{
        "CHANNEL",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHANNEL, config); }};

    /// How many buffers may wait in the channel before the producer is throttled.
    static inline const DescriptorConfig::ConfigParameter<size_t> CHANNEL_CAPACITY{
        "CHANNEL_CAPACITY",
        size_t{64},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHANNEL_CAPACITY, config); }};

    /// Order matters: the map is filled with `emplace`, so the first entry for a key wins. Our
    /// OUTPUT_FORMAT therefore has to come before SinkDescriptor's, whose version has no default
    /// and would demand an explicit value.
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(OUTPUT_FORMAT, CHANNEL, CHANNEL_CAPACITY, SinkDescriptor::parameterMap);
};

}

FMT_OSTREAM(NES::HandoffSink);
