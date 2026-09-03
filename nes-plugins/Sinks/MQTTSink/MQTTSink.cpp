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

#include <MQTTSink.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <MQTTAsync.h>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <Util/Variant.hpp>
#include <mqtt/async_client.h>
#include <mqtt/buffer_ref.h>
#include <mqtt/connect_options.h>
#include <mqtt/exception.h>
#include <mqtt/message.h>
#include <mqtt/types.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{
std::string generateClientId(std::string clientId)
{
    if (clientId == GENERATE_CLIENT_ID_TOKEN)
    {
        return UUIDToString(generateUUID());
    }
    return clientId;
}

/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<std::string> SERVER_URI{Identifier::parse("SERVER_URI"), "The MQTT broker URI"};

const ConfigField<std::string> CLIENT_ID{
    Identifier::parse("CLIENT_ID"), "The MQTT client ID (leave default to auto-generate)", std::string{GENERATE_CLIENT_ID_TOKEN}};

const ConfigField<std::string> TOPIC{Identifier::parse("TOPIC"), "The MQTT topic to publish to"};

const ConfigField<int64_t> QOS{
    Identifier::parse("QOS"),
    "MQTT QoS level (0, 1, or 2)",
    [](const ConfigLiteral& literal) -> std::expected<int64_t, Exception>
    {
        auto value = tryGetOr<int64_t>(literal, expectedType<int64_t>());
        if (!value)
        {
            return std::unexpected{value.error()};
        }
        if (*value != 0 && *value != 1 && *value != 2)
        {
            return std::unexpected{InvalidConfigParameter("MQTTSink: QoS must be 0, 1, or 2")};
        }
        return value;
    },
    int64_t{1}};

const ConfigField<bool> RETAINED{Identifier::parse("RETAINED"), "Whether published messages should be retained by the broker", false};

const ConfigField<int64_t> MAX_OUTSTANDING_MESSAGES{
    Identifier::parse("MAX_OUTSTANDING_MESSAGES"),
    "Maximum number of unacknowledged QoS>=1 messages; no effect under QoS 0",
    [](const ConfigLiteral& literal) { return tryGetOr<int64_t>(literal, expectedType<int64_t>()); },
    int64_t{128}};
/// NOLINTEND(cert-err58-cpp)
}

Schema<QualifiedErasedConfigField, Ordered> MQTTSink::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("MQTT_SINK"), SERVER_URI, CLIENT_ID, TOPIC, QOS, RETAINED, MAX_OUTSTANDING_MESSAGES);
}

std::expected<MQTTSinkConfig, Exception> MQTTSinkConfig::fromConfig(const InstantiatedConfig& config)
{
    return MQTTSinkConfig{
        .serverURI = config.get(SERVER_URI),
        .clientId = config.get(CLIENT_ID),
        .topic = config.get(TOPIC),
        .qos = config.get(QOS),
        .retained = config.get(RETAINED),
        .maxOutstandingMessages = config.get(MAX_OUTSTANDING_MESSAGES),
    };
}

MQTTSink::MQTTSink(BackpressureController backpressureController, const MQTTSinkConfig& config, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , serverURI(config.serverURI)
    , clientId(generateClientId(config.clientId))
    , topic(config.topic)
    , qos(static_cast<int32_t>(config.qos))
    , retained(config.retained)
    , maxOutstandingMessages(static_cast<int32_t>(config.maxOutstandingMessages))
    , backpressureHandler(sinkDescriptor.getBackpressureUpperThreshold(), sinkDescriptor.getBackpressureLowerThreshold())
{
}

std::ostream& MQTTSink::toString(std::ostream& os) const
{
    os << "\nMQTTSink(";
    os << "\n  serverURI: " << serverURI;
    os << "\n  clientId: " << clientId;
    os << "\n  topic: " << topic;
    os << "\n  qos: " << qos;
    os << "\n  retained: " << retained;
    os << "\n  maxOutstandingMessages: " << maxOutstandingMessages;
    os << ")\n";
    return os;
}

void MQTTSink::start(PipelineExecutionContext&)
{
    NES_INFO("Opening MQTTSink at {} using clientId: {}.", serverURI, clientId);
    client = std::make_unique<mqtt::async_client>(serverURI, clientId, maxOutstandingMessages);
    try
    {
        mqtt::connect_options connectOptions;
        connectOptions.set_max_inflight(maxOutstandingMessages);
        auto connectToken = [&]()
        {
            /// apparently, MQTT connect requires synchronization, with concurrent connects (multiple mqtt sinks).
            static std::mutex mqttConnectLock;
            const std::scoped_lock lock(mqttConnectLock);
            return client->connect(std::move(connectOptions));
        }();
        connectToken->wait();
        const auto connectResponse = connectToken->get_connect_response();
        NES_INFO("Connected to MQTT broker: {}. Version: {}", connectResponse.get_server_uri(), connectResponse.get_mqtt_version());
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSink("Failed to connect to MQTT broker {}: {}", serverURI, e.what());
    }
}

MQTTSink::PublishResult MQTTSink::tryPublish(const TupleBuffer& buffer)
{
    size_t messageSize = buffer.getNumberOfTuples();
    for (size_t index = 0; index < buffer.getNumberOfChildBuffers(); index++)
    {
        messageSize += buffer.loadChildBuffer(ChildBufferIndex(index)).getNumberOfTuples();
    }
    mqtt::binary payload;
    payload.reserve(messageSize);
    auto data = buffer.getAvailableMemoryArea<char>().first(buffer.getNumberOfTuples());
    payload.append(data.begin(), data.end());
    for (size_t index = 0; index < buffer.getNumberOfChildBuffers(); index++)
    {
        auto child = buffer.loadChildBuffer(ChildBufferIndex(index));
        auto childData = child.getAvailableMemoryArea<char>().first(child.getNumberOfTuples());
        payload.append(childData.begin(), childData.end());
    }

    try
    {
        client->publish(mqtt::make_message(topic, mqtt::binary_ref(std::move(payload)), qos, retained));
    }
    catch (const mqtt::exception& e)
    {
        if (e.get_return_code() == MQTTASYNC_MAX_BUFFERED_MESSAGES)
        {
            return PublishResult::Full;
        }
        NES_ERROR("MQTTSink publish to topic {} failed: {}", topic, e.what());
        return PublishResult::Closed;
    }
    return PublishResult::Ok;
}

void MQTTSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pec)
{
    PRECONDITION(client, "MQTTSink client is not initialized");
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in MQTTSink.");

    auto currentBuffer = std::optional(inputTupleBuffer);
    while (currentBuffer)
    {
        switch (tryPublish(*currentBuffer))
        {
            case PublishResult::Ok: {
                currentBuffer = backpressureHandler.onSuccess(backpressureController);
                continue;
            }
            case PublishResult::Full: {
                if (const auto emit = backpressureHandler.onFull(*currentBuffer, backpressureController))
                {
                    pec.repeatTask(*emit, BACKPRESSURE_RETRY_INTERVAL);
                }
                return;
            }
            case PublishResult::Closed: {
                [[maybe_unused]] auto droppedBuffer = backpressureHandler.onFull(*currentBuffer, backpressureController);
                throw CannotOpenSink("MQTTSink connection to broker {} was closed", serverURI);
            }
        }
    }
}

void MQTTSink::stop(PipelineExecutionContext& pec)
{
    if (!client)
    {
        return;
    }
    INVARIANT(backpressureHandler.empty(), "BackpressureHandler is not empty");
    try
    {
        /// Wait for all in-flight QoS>=1 messages to be acknowledged before disconnecting.
        /// QoS 0 messages have no pending token; nothing to wait for.
        for (const auto& token : client->get_pending_delivery_tokens())
        {
            if (!token->is_complete())
            {
                pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
                return;
            }
        }
        if (client->is_connected())
        {
            client->disconnect()->wait();
        }
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSink("When closing MQTT sink: {}", e.what());
    }
    NES_INFO("MQTT Sink completed.");
    client.reset();
}

}
