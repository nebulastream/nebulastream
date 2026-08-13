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
#include <optional>
#include <ostream>
#include <string>
#include <vector>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <folly/Synchronized.h>
#include <network/lib.h>
#include <rust/cxx.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Sink-defined config struct: instantiated from the generic config by the SinkConfig registry
/// entry, carried through the SinkDescriptor as std::any, and serialized via reflection of
/// exactly this struct (all members are reflectable).
struct NetworkSinkConfig
{
    std::string dataEndpoint;
    std::string bind;
    std::string channel;
    /// Per-channel sender queue size override. 0 means use the worker-level default.
    size_t senderQueueSize;
    /// Per-channel max pending acks override. 0 means use the worker-level default.
    size_t maxPendingAcks;

    static std::expected<NetworkSinkConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

class NetworkSink final : public Sink
{
public:
    constexpr static auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);

    static const std::string& name()
    {
        static const std::string Instance = "Network";
        return Instance;
    }

    NetworkSink(BackpressureController backpressureController, const NetworkSinkConfig& config, const SinkDescriptor& sinkDescriptor);
    ~NetworkSink() override = default;

    NetworkSink(const NetworkSink&) = delete;
    NetworkSink& operator=(const NetworkSink&) = delete;
    NetworkSink(NetworkSink&&) = delete;
    NetworkSink& operator=(NetworkSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec) override;
    void stop(PipelineExecutionContext& pec) override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

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
    size_t senderQueueSize;
    size_t maxPendingAcks;
    std::atomic_bool closed;
};

}
