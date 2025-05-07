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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <random>
#include <string>
#include <string_view>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <mqtt/async_client.h>
#include <PayloadStash.hpp>

namespace NES::Sources
{

class MQTTSource : public Source
{
public:
    static inline const std::string NAME = "MQTT";

    explicit MQTTSource(const SourceDescriptor& sourceDescriptor);
    ~MQTTSource() override = default;

    MQTTSource(const MQTTSource&) = delete;
    MQTTSource& operator=(const MQTTSource&) = delete;
    MQTTSource(MQTTSource&&) = delete;
    MQTTSource& operator=(MQTTSource&&) = delete;

    /// Open connection to MQTT broker.
    void open() override;

    size_t fillTupleBuffer(Memory::TupleBuffer& tupleBuffer, Memory::AbstractBufferProvider&, const std::stop_token& stopToken) override;

    /// Close connection to MQTT broker.
    void close() override;

    static Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string serverURI;
    std::string clientId;
    std::string topic;
    int32_t qos;
    std::chrono::milliseconds flushingInterval;

    std::unique_ptr<mqtt::async_client> client;

    PayloadStash payloadStash;

    void writePayloadToBuffer(std::string_view payload, Memory::TupleBuffer& tb, size_t& tbOffset);
};

namespace detail::uuid
{
static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_int_distribution<> dis(0, 15);
static std::uniform_int_distribution<> dis2(8, 11);

std::string generateUUID()
{
    std::stringstream ss;
    ss << std::hex;
    for (int i = 0; i < 8; i++)
    {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 4; i++)
    {
        ss << dis(gen);
    }
    ss << "-4";
    for (int i = 0; i < 3; i++)
    {
        ss << dis(gen);
    }
    ss << "-";
    ss << dis2(gen);
    for (int i = 0; i < 3; i++)
    {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 12; i++)
    {
        ss << dis(gen);
    }
    return ss.str();
}
}

/// Defines the names, (optional) default values, (optional) validation & config functions for all MQTT config parameters.
struct ConfigParametersMQTT
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> SERVER_URI{
        "serverURI",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(SERVER_URI, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> CLIENT_ID{
        "clientId",
        "generated",
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            if (auto it = config.find(CLIENT_ID); it != config.end())
            {
                return it->second;
            }
            return detail::uuid::generateUUID();
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> TOPIC{
        "topic",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return Configurations::DescriptorConfig::tryGet(TOPIC, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<float> FLUSH_INTERVAL_MS{
        "flushIntervalMS",
        0,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<int32_t> QOS{
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

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(SERVER_URI, CLIENT_ID, QOS, TOPIC, FLUSH_INTERVAL_MS);
};

}
