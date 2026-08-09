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
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <folly/Synchronized.h>
#include <network/lib.h>
#include <rust/cxx.h>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
class NetworkSink final : public Sink
{
public:
    constexpr static auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);

    static const std::string& name()
    {
        static const std::string Instance = "Network";
        return Instance;
    }

    NetworkSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~NetworkSink() override = default;

    NetworkSink(const NetworkSink&) = delete;
    NetworkSink& operator=(const NetworkSink&) = delete;
    NetworkSink(NetworkSink&&) = delete;
    NetworkSink& operator=(NetworkSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec) override;
    void stop(PipelineExecutionContext& pec) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    size_t tupleSize;
    folly::Synchronized<std::vector<TupleBuffer>> bufferBacklog;
    BackpressureHandler backpressureHandler;
    std::optional<rust::Box<SenderNetworkService>> server;
    std::optional<rust::Box<SenderDataChannel>> channel;
    std::string channelId;
    std::unordered_map<OriginId, OriginId> channelOriginIds;
    std::string connectionAddr;
    std::string thisConnection;
    size_t senderQueueSize;
    size_t maxPendingAcks;
    std::atomic_bool closed;
};

/// NOLINTBEGIN(cert-err58-cpp)
struct ConfigParametersNetworkSink
{
    static inline const DescriptorConfig::ConfigParameter<std::string> DATA_ENDPOINT{
        "DATA_ENDPOINT",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(DATA_ENDPOINT, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> BIND{
        "BIND",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BIND, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> CHANNEL{
        "CHANNEL",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHANNEL, config); }};
    /// Origins the receiving side sees this channel as, as "upstream:downstream" pairs separated by commas. A subplan shared by
    /// several sinks crosses the node boundary once per sink, and every one of those channels relays the same upstream origins, so
    /// without remapping the receiving node would end up with several sources claiming one origin — which it keys its running
    /// sources on, and can therefore only hold one of. The mapping is one-to-one rather than collapsing onto a single id: a sink
    /// may be fed by several origins at once (a union placed upstream of the boundary), and sequence numbers are unique only
    /// within an origin, so merging them would produce duplicate sequence numbers on the channel.
    static inline const DescriptorConfig::ConfigParameter<std::string> ORIGIN_ID_MAP{
        "ORIGIN_ID_MAP",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(ORIGIN_ID_MAP, config); }};

    /// Per-channel sender queue size override. 0 means use the worker-level default.
    static inline const DescriptorConfig::ConfigParameter<size_t> SENDER_QUEUE_SIZE{
        "SENDER_QUEUE_SIZE",
        size_t{0},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SENDER_QUEUE_SIZE, config); }};

    /// Per-channel max pending acks override. 0 means use the worker-level default.
    static inline const DescriptorConfig::ConfigParameter<size_t> MAX_PENDING_ACKS{
        "MAX_PENDING_ACKS",
        size_t{0},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_PENDING_ACKS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SinkDescriptor::parameterMap, DATA_ENDPOINT, CHANNEL, ORIGIN_ID_MAP, BIND, SENDER_QUEUE_SIZE, MAX_PENDING_ACKS);
};

/// NOLINTEND(cert-err58-cpp)

}
