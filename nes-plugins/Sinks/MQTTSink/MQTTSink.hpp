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
#include <future>
#include <memory>
#include <optional>
#include <ostream>
#include <random>
#include <string>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>
#include <mqtt/async_client.h>
#include <PipelineExecutionContext.hpp>

namespace NES
{


struct OOPolicy;
struct Writer
{
    Writer(std::unique_ptr<OOPolicy> policy, std::string server_uri, std::string client_id, std::string topic, int32_t qos);
    ~Writer();
    folly::MPMCQueue<std::pair<SequenceData, std::string>> dataQueue{10};
    std::unique_ptr<OOPolicy> policy;

    std::string serverUri;
    std::string clientId;
    std::string topic;
    int32_t qos;

    std::promise<void> terminationPromise;
};

class MQTTSink : public Sink
{
public:
    static inline std::string NAME = "MQTT";
    explicit MQTTSink(const SinkDescriptor& sinkDescriptor);
    ~MQTTSink() override;

    MQTTSink(const MQTTSink&) = delete;
    MQTTSink(MQTTSink&&) = delete;
    MQTTSink& operator=(const MQTTSink&) = delete;
    MQTTSink& operator=(MQTTSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    /// Order Matters: writer needs to outlive the dispatcher
    Writer writer;
    std::jthread dispatcher;

    std::unique_ptr<Format> formatter;
    std::shared_future<void> terminationFuture;
};

namespace
{
std::random_device rd;
std::mt19937 gen(rd());
std::uniform_int_distribution<> dis(0, 15);
std::uniform_int_distribution<> dis2(8, 11);

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

struct ConfigParametersMQTTSink
{
    static inline const DescriptorConfig::ConfigParameter<std::string> SERVER_URI{
        "server_uri",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SERVER_URI, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> CLIENT_ID{
        "client_id",
        "generated",
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            if (auto it = config.find(CLIENT_ID); it != config.end())
            {
                return it->second;
            }
            return generateUUID();
        }};

    static inline const DescriptorConfig::ConfigParameter<std::string> TOPIC{
        "topic",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TOPIC, config); }};

    static inline const DescriptorConfig::ConfigParameter<int32_t> QOS{
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

    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, InputFormat> INPUT_FORMAT{
        "input_format",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INPUT_FORMAT, config); }};

    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, OutOfOrderPolicy>
        OUT_OF_ORDER_POLICY{
            "outOfOrderPolicy",
            EnumWrapper(OutOfOrderPolicy::ALLOW),
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(OUT_OF_ORDER_POLICY, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SERVER_URI, CLIENT_ID, QOS, TOPIC, INPUT_FORMAT, OUT_OF_ORDER_POLICY);
};

}
