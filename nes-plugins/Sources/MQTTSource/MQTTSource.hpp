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
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <mqtt/async_client.h>
#include <PayloadStash.hpp>

namespace NES
{

class MQTTSource : public Source
{
public:
    /// NOLINTNEXTLINE(cert-err58-cpp): static-storage std::string init is the project-wide plugin-name idiom; matches sibling Source plugins.
    static inline const std::string NAME = "MQTT";

    explicit MQTTSource(const SourceDescriptor& sourceDescriptor);
    ~MQTTSource() override = default;

    MQTTSource(const MQTTSource&) = delete;
    MQTTSource& operator=(const MQTTSource&) = delete;
    MQTTSource(MQTTSource&&) = delete;
    MQTTSource& operator=(MQTTSource&&) = delete;

    /// Open connection to MQTT broker.
    void open(std::shared_ptr<AbstractBufferProvider>) override;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Close connection to MQTT broker.
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string serverURI;
    std::string clientId;
    std::string topic;
    int32_t qos;
    std::chrono::milliseconds flushingInterval;
    size_t maxFlushRetries;
    /// When true, the source pairs with an in-process publisher (set up by the
    /// MQTT InlineData/FileData registrars when a `.test` uses ATTACH). On
    /// open() the source subscribes to a derived control topic and publishes
    /// a "ready" marker; the publisher waits for that marker before sending
    /// data rows on `topic`; the source EOS-es when an "eos" marker arrives
    /// on the control topic. With this enabled, retained messages + the
    /// topic-per-row hack go away — pure pub/sub at line speed.
    bool controlChannelEnabled;
    /// Derived from `topic` at construction. The actual data topic the source
    /// subscribes to / receives rows on. Today identical to `topic`; kept as
    /// a separate field so future schemes (e.g. wildcard-strip semantics) have
    /// somewhere to live.
    std::string dataTopic;
    /// Derived from `topic` + "/_nes_control". Used only when controlChannelEnabled.
    std::string controlTopic;
    /// Set when an "eos" marker is consumed on the control topic. The next
    /// fillTupleBuffer call returns EoS immediately instead of waiting for
    /// the flush-retry timeout.
    bool eosReceived = false;

    std::unique_ptr<mqtt::async_client> client;

    PayloadStash payloadStash;

    void writePayloadToBuffer(std::string_view payload, TupleBuffer& tb, size_t& tbOffset);
};

/// Defines the names, (optional) default values, (optional) validation & config functions for all MQTT config parameters.
/// NOLINTBEGIN(cert-err58-cpp): static-storage ConfigParameter initialization is the project-wide pattern for source plugins
/// (TCPSource, FileSource, etc.). The constructors can theoretically throw `std::bad_alloc`; in practice they are evaluated
/// once at static-init time on a path the runtime cannot meaningfully recover from. Refactoring would require redesigning the
/// ConfigParameter registry across every plugin — out of scope for any single PR.
struct ConfigParametersMQTTSource
{
    static inline const DescriptorConfig::ConfigParameter<std::string> SERVER_URI{
        "serveruri",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SERVER_URI, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> CLIENT_ID{
        "clientId",
        "generated",
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            if (auto it = config.find(CLIENT_ID); it != config.end())
            {
                return it->second;
            }
            return UUIDToString(generateUUID());
        }};

    static inline const DescriptorConfig::ConfigParameter<std::string> TOPIC{
        "topic",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TOPIC, config); }};

    static inline const DescriptorConfig::ConfigParameter<float> FLUSH_INTERVAL_MS{
        "flushintervalms",
        0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};

    static inline const DescriptorConfig::ConfigParameter<size_t> MAX_FLUSH_RETRIES{
        "maxflushretries",
        5,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_FLUSH_RETRIES, config); }};

    static inline const DescriptorConfig::ConfigParameter<int32_t> QOS{
        "qos",
        1,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint8_t>
        {
            int32_t qos = std::stoi(config.at(QOS));
            if (qos != 0 && qos != 1 && qos != 2)
            {
                NES_ERROR("MQTTSource: QualityOfService is: {}, but must be 0, 1, or 2.", qos);
                return std::nullopt;
            }
            return qos;
        }};

    /// Internal flag set by the MQTT InlineData/FileData registrars to wire the
    /// source up for a publish-after-subscribe handshake. Defaults to false, so
    /// non-test MQTT sources behave like ordinary streaming subscribers. Users
    /// can override in a .test file's SET clause to opt in/out explicitly.
    static inline const DescriptorConfig::ConfigParameter<bool> CONTROL_CHANNEL_ENABLED{
        "controlchannel",
        false,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(CONTROL_CHANNEL_ENABLED, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap,
            SERVER_URI,
            CLIENT_ID,
            QOS,
            TOPIC,
            FLUSH_INTERVAL_MS,
            MAX_FLUSH_RETRIES,
            CONTROL_CHANNEL_ENABLED);
};

/// NOLINTEND(cert-err58-cpp)

}
