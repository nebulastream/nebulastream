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

#include "Sources/Source.hpp"

#include <optional>
#include <string>

#include <mqtt/async_client.h>

#include <Configurations/Descriptor.hpp>

namespace NES::Sources
{


/// Defines the names, (optional) default values, (optional) validation & config functions for all MQTT config parameters.
struct ConfigParametersMQTT
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> SERVER_URI{
        "serverURI", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(SERVER_URI, config);
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> CLIENT_ID{
        "clientId", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(CLIENT_ID, config);
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> USER_NAME{
        "userName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(USER_NAME, config);
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> TOPIC{
        "topic", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(TOPIC, config);
        }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap();
};


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

    size_t fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer) override;

    /// Open connection to MQTT broker.
    void open() override;
    /// Close connection to MQTT broker.
    void close() override;

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string>&& config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string serverURI;
    std::string clientId;
    std::string userName;
    std::string topic;

    std::unique_ptr<mqtt::async_client> mqttClient;
};
}
