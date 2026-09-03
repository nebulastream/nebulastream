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
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>

#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <mqtt/client.h>
#include <ErrorHandling.hpp>
#include <PayloadStash.hpp>

namespace NES
{

constexpr std::string_view GENERATE_CLIENT_ID_TOKEN = "HACK_GENERATED_TOKEN_SENTINEL_VALUE";

struct MQTTSourceConfig
{
    std::string serverURI;
    std::string clientId;
    std::string topic;
    int64_t qos{};
    int64_t flushIntervalMs{};
    std::string implicitMessageDelimiter;
    bool logMessages{};

    static std::expected<MQTTSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

class MQTTSource : public Source
{
public:
    static constexpr std::string_view NAME = "MQTT";

    explicit MQTTSource(const MQTTSourceConfig& config);
    ~MQTTSource() override = default;

    MQTTSource(const MQTTSource&) = delete;
    MQTTSource& operator=(const MQTTSource&) = delete;
    MQTTSource(MQTTSource&&) = delete;
    MQTTSource& operator=(MQTTSource&&) = delete;

    void open(std::shared_ptr<AbstractBufferProvider>) override;
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void close() override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string serverURI;
    std::string clientId;
    std::string topic;
    int32_t qos;
    std::optional<std::chrono::milliseconds> flushingInterval;
    std::string implicitMessageDelimiter;
    bool logMessages;

    std::unique_ptr<mqtt::client> client;

    PayloadStash payloadStash;

    void writePayloadToBuffer(std::string_view payload, TupleBuffer& tb, size_t& tbOffset);
};

}
