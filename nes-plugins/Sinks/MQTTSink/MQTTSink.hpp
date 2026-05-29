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
#include <cstdint>
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
#include <Util/Logger/Logger.hpp>
#include <mqtt/async_client.h>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

constexpr std::string_view GENERATE_CLIENT_ID_TOKEN = "HACK_GENERATED_TOKEN_SENTINEL_VALUE";

/// A sink that publishes the bytes of every incoming TupleBuffer as an MQTT
/// message to the configured broker/topic.
class MQTTSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "MQTT";
    static constexpr auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);

    explicit MQTTSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    /// Outcome of a single publish attempt. Mirrors NetworkSink's `SendResult`:
    ///   Ok     - paho accepted the message into its outbound queue.
    ///   Full   - paho's outbound queue is at capacity; caller should buffer & retry.
    ///   Closed - non-recoverable paho error (disconnected, protocol, etc.); caller should fail the query.
    enum class PublishResult : uint8_t
    {
        Ok,
        Full,
        Closed,
    };

    PublishResult tryPublish(const TupleBuffer& buffer);

    std::string serverURI;
    std::string clientId;
    std::string topic;
    int32_t qos;
    bool retained;
    int32_t maxOutstandingMessages;
    bool sendWhileDisconnected;
    bool automaticReconnect;

    std::unique_ptr<mqtt::async_client> client;
    BackpressureHandler backpressureHandler;
};

/// Defines the names, (optional) default values, (optional) validation & config functions for all MQTT sink config parameters.
/// NOLINTBEGIN(cert-err58-cpp): static-storage ConfigParameter initialization is the project-wide pattern for sink plugins
/// (FileSink, etc.). The constructors can theoretically throw `std::bad_alloc`; in practice they are evaluated once at
/// static-init time on a path the runtime cannot meaningfully recover from. Refactoring would require redesigning the
/// ConfigParameter registry across every plugin — out of scope for any single PR.
struct ConfigParametersMQTTSink
{
    static inline const DescriptorConfig::ConfigParameter<std::string> SERVER_URI{
        "server_uri",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SERVER_URI, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> CLIENT_ID{
        "client_id",
        std::string(GENERATE_CLIENT_ID_TOKEN),
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        { return DescriptorConfig::tryGet(CLIENT_ID, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> TOPIC{
        "topic",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TOPIC, config); }};

    static inline const DescriptorConfig::ConfigParameter<int32_t> QOS{
        "qos",
        1,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<int32_t>
        {
            const int32_t qos = std::stoi(config.at(QOS));
            if (qos != 0 && qos != 1 && qos != 2)
            {
                NES_ERROR("MQTTSink: QualityOfService is: {}, but must be 0, 1, or 2.", qos);
                return std::nullopt;
            }
            return qos;
        }};

    static inline const DescriptorConfig::ConfigParameter<bool> RETAINED{
        "retained",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(RETAINED, config); }};

    /// Maximum number of QoS>=1 messages that may be unacknowledged at any time. When this many
    /// delivery tokens are still pending, the sink treats the broker as backed up and retries via
    /// the BackpressureHandler. The same value is also passed to paho's create_options as a defensive
    /// hard cap on the internal outbound queue.
    /// QoS 0 has no acknowledgements, so this knob has no effect under QoS 0 (fire-and-forget by spec).
    static inline const DescriptorConfig::ConfigParameter<int32_t> MAX_OUTSTANDING_MESSAGES{
        "max_outstanding_messages",
        128,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(MAX_OUTSTANDING_MESSAGES, config); }};

    /// Survive a transient connection loss instead of failing the query: while the link to the broker
    /// is down, execute() keeps buffering publishes (capped at `max_outstanding_messages`) and they
    /// are flushed once the connection is re-established. Default true preserves the historical
    /// reconnect-and-deliver behavior. Set false to make a connection loss fatal to in-flight writes:
    /// a publish issued while disconnected is rejected (CannotOpenSink) rather than buffered.
    static inline const DescriptorConfig::ConfigParameter<bool> SEND_WHILE_DISCONNECTED{
        "send_while_disconnected",
        true,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(SEND_WHILE_DISCONNECTED, config); }};

    /// Whether paho re-establishes the connection on its own after a drop. Default true pairs with
    /// `send_while_disconnected` to ride out a transient outage; set false to leave a dropped
    /// connection dead (no automatic recovery).
    static inline const DescriptorConfig::ConfigParameter<bool> AUTOMATIC_RECONNECT{
        "automatic_reconnect",
        true,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(AUTOMATIC_RECONNECT, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SinkDescriptor::parameterMap,
            SERVER_URI,
            CLIENT_ID,
            TOPIC,
            QOS,
            RETAINED,
            MAX_OUTSTANDING_MESSAGES,
            SEND_WHILE_DISCONNECTED,
            AUTOMATIC_RECONNECT);
};

/// NOLINTEND(cert-err58-cpp)

}

FMT_OSTREAM(NES::MQTTSink);
