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
#include <Util/Strings.hpp>
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

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override;
    BufferResult executeBuffer(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;

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

    std::unique_ptr<mqtt::async_client> client;
    BackpressureHandler backpressureHandler;
};

/// Defines the names, (optional) default values, (optional) validation & config functions for all MQTT sink config parameters.
struct ConfigParametersMQTTSink
{
    ///NOLINTBEGIN(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> SERVER_URI{
        "SERVER_URI",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SERVER_URI, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> CLIENT_ID{
        "CLIENT_ID",
        std::string(GENERATE_CLIENT_ID_TOKEN),
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        { return DescriptorConfig::tryGet(CLIENT_ID, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> TOPIC{
        "TOPIC",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TOPIC, config); }};

    static inline const DescriptorConfig::ConfigParameter<int32_t> QOS{
        "QOS",
        1,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<int32_t>
        {
            auto qos = from_chars<uint8_t>(config.at(QOS));
            if (!qos || (qos.value() != 0 && qos.value() != 1 && qos.value() != 2))
            {
                NES_ERROR("MQTTSink: QualityOfService is: {}, but must be 0, 1, or 2.", config.at(QOS));
                return std::nullopt;
            }
            return qos;
        }};

    static inline const DescriptorConfig::ConfigParameter<bool> RETAINED{
        "RETAINED",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(RETAINED, config); }};

    /// Maximum number of QoS>=1 messages that may be unacknowledged at any time. When this many
    /// delivery tokens are still pending, the sink treats the broker as backed up and retries via
    /// the BackpressureHandler. The same value is also passed to paho's create_options as a defensive
    /// hard cap on the internal outbound queue.
    /// QoS 0 has no acknowledgements, so this knob has no effect under QoS 0 (fire-and-forget by spec).
    static inline const DescriptorConfig::ConfigParameter<int32_t> MAX_OUTSTANDING_MESSAGES{
        "MAX_OUTSTANDING_MESSAGES",
        128,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(MAX_OUTSTANDING_MESSAGES, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SinkDescriptor::parameterMap, SERVER_URI, CLIENT_ID, TOPIC, QOS, RETAINED, MAX_OUTSTANDING_MESSAGES);
    ///NOLINTEND(cert-err58-cpp)
};

}

FMT_OSTREAM(NES::MQTTSink);
