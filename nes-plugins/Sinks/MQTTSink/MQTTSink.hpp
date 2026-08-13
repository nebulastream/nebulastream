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
#include <cstdint>
#include <expected>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <folly/Synchronized.h>
#include <mqtt/async_client.h>
#include <nes-network-bindings/lib.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

constexpr std::string_view GENERATE_CLIENT_ID_TOKEN = "HACK_GENERATED_TOKEN_SENTINEL_VALUE";

struct MQTTSinkConfig
{
    std::string serverURI;
    std::string clientId;
    std::string topic;
    int64_t qos{};
    bool retained{};
    int64_t maxOutstandingMessages{};

    static std::expected<MQTTSinkConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

class MQTTSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "MQTT";
    static constexpr auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);

    explicit MQTTSink(BackpressureController backpressureController, const MQTTSinkConfig& config, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    /// Reports connection loss reported by paho outside of a publish() call. paho only calls
    /// connection_lost() for an unexpected drop, not for a clean disconnect() in stop().
    class ConnectionCallback final : public mqtt::callback
    {
    public:
        void connection_lost(const std::string& cause) override;
        folly::Synchronized<std::string> lostCause;
    };

    /// Outcome of a single publish attempt:
    ///   Ok     - paho accepted the message into its outbound queue.
    ///   Closed - non-recoverable paho error (disconnected, protocol, etc.); caller should fail the query.
    ///   Full   - paho's outbound queue is at capacity; caller should buffer & retry.
    SendResult tryPublish(const TupleBuffer& buffer);

    std::string serverURI;
    std::string clientId;
    std::string topic;
    int32_t qos;
    bool retained;
    int32_t maxOutstandingMessages;

    ConnectionCallback connectionCallback;
    std::unique_ptr<mqtt::async_client> client;
    BackpressureHandler backpressureHandler;
};

}

FMT_OSTREAM(NES::MQTTSink);
