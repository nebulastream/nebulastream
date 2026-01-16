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
#include <deque>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <folly/Synchronized.h>
#include <network/lib.h>
#include <rust/cxx.h>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
class BackpressureHandler
{
    struct State
    {
        bool hasBackpressure = false;
        std::deque<TupleBuffer> buffered;
        SequenceNumber pendingSequenceNumber = INVALID<SequenceNumber>;
        ChunkNumber pendingChunkNumber = INVALID<ChunkNumber>;
    };

    folly::Synchronized<State> stateLock;

public:
    std::optional<TupleBuffer> onFull(TupleBuffer buffer, BackpressureController& backpressureController);
    std::optional<TupleBuffer> onSuccess(BackpressureController& backpressureController);
    bool empty() const;
};

class NetworkSink final : public Sink
{
public:
    constexpr static auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(25);

    static const std::string& name()
    {
        static const std::string Instance = "Network";
        return Instance;
    }

    explicit NetworkSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
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
    std::string connectionAddr;
    std::string thisConnection;
    std::atomic_bool closed;
};

struct ConfigParametersNetworkSink
{
    static inline const DescriptorConfig::ConfigParameter<std::string> CONNECTION{
        "connection",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CONNECTION, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> BIND{
        "bind",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BIND, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> CHANNEL{
        "channel",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHANNEL, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, CONNECTION, CHANNEL, BIND);
};

}
