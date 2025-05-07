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

#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <random>
#include <string>
#include <unordered_map>

#include <mqtt/async_client.h>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Sinks
{

class MQTTSink : public Sink
{
public:
    static inline std::string NAME = "MQTT";
    explicit MQTTSink(const SinkDescriptor& sinkDescriptor);
    ~MQTTSink() override = default;

    MQTTSink(const MQTTSink&) = delete;
    MQTTSink& operator=(const MQTTSink&) = delete;
    MQTTSink(MQTTSink&&) = delete;
    MQTTSink& operator=(MQTTSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const Memory::TupleBuffer& inputBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    std::string serverUri;
    std::string clientId;
    std::string topic;
    int32_t qos;

    std::unique_ptr<mqtt::async_client> client;

    std::unique_ptr<Format> formatter;
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

    static inline const Configurations::DescriptorConfig::ConfigParameter<int32_t> QOS{
        "qos",
        1,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint8_t>
        {
            int32_t qos = std::stoi(config.at(QOS));
            if (qos != 0 && qos != 1 && qos != 2)
            {
                NES_ERROR("MQTTSink: QualityOfService is: {}, but must be 0, 1, or 2.", qos);
                return std::nullopt;
            }
            return qos;
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<Configurations::EnumWrapper, Configurations::InputFormat>
        INPUT_FORMAT{
            "inputFormat",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return Configurations::DescriptorConfig::tryGet(INPUT_FORMAT, config); }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(SERVER_URI, CLIENT_ID, QOS, TOPIC, INPUT_FORMAT);
};

}
