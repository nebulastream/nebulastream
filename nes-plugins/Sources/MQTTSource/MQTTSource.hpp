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
#include <mqtt/async_client.h>
#include <PayloadStash.hpp>

namespace NES
{

/// Same sentinel (and pattern) as MQTTSink: `client_id` defaults to this token
/// and the constructor swaps it for a fresh UUID. The swap cannot live in the
/// CLIENT_ID lambda — DescriptorConfig::validateAndFormat only invokes a
/// parameter's lambda when the key is present, so a defaulted key bypasses it;
/// and a plain literal default would give every id-less source the SAME client
/// id, which MQTT punishes with session takeover (the broker disconnects the
/// previous holder of the id).
constexpr std::string_view GENERATE_CLIENT_ID_TOKEN = "HACK_GENERATED_TOKEN_SENTINEL_VALUE";

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
    /// POSTCONDITION (conn-tests-design-v2.md §3.4 / §5): when this returns,
    /// the broker has acknowledged the SUBSCRIBE (paho's subscribe token has
    /// been `wait()`-ed on, which blocks until SUBACK). The conn-test
    /// harness's READY/GO barrier relies on this — a seed published after
    /// READY is guaranteed to land on a wired-up subscription.
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
    bool cleanSession;

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
        "server_uri",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SERVER_URI, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> CLIENT_ID{
        "client_id",
        std::string(GENERATE_CLIENT_ID_TOKEN),
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CLIENT_ID, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> TOPIC{
        "topic",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TOPIC, config); }};

    static inline const DescriptorConfig::ConfigParameter<float> FLUSH_INTERVAL_MS{
        "flush_interval_ms",
        0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};

    static inline const DescriptorConfig::ConfigParameter<size_t> MAX_FLUSH_RETRIES{
        "max_flush_retries",
        5,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_FLUSH_RETRIES, config); }};

    /// When false, the connector opens a *persistent* MQTT session
    /// (clean_session=false). Paired with a fixed clientId, the broker keeps
    /// the subscription and queues QoS>=1 messages across a disconnect, so a
    /// reconnect resumes without re-subscribing. Default true preserves the
    /// historical clean-session behavior for every existing user.
    static inline const DescriptorConfig::ConfigParameter<bool> CLEAN_SESSION{
        "clean_session",
        true,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CLEAN_SESSION, config); }};

    static inline const DescriptorConfig::ConfigParameter<int32_t> QOS{
        "qos",
        1,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<int32_t>
        {
            int32_t qos = std::stoi(config.at(QOS));
            if (qos != 0 && qos != 1 && qos != 2)
            {
                NES_ERROR("MQTTSource: QualityOfService is: {}, but must be 0, 1, or 2.", qos);
                return std::nullopt;
            }
            return qos;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, SERVER_URI, CLIENT_ID, QOS, TOPIC, FLUSH_INTERVAL_MS, MAX_FLUSH_RETRIES, CLEAN_SESSION);
};

/// NOLINTEND(cert-err58-cpp)

}
