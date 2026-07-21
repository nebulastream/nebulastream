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
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <MQTTAsync.h>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <mqtt/async_client.h>
#include <mqtt/buffer_ref.h>
#include <mqtt/connect_options.h>
#include <mqtt/exception.h>
#include <mqtt/message.h>
#include <mqtt/types.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

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
}

MQTTSink::MQTTSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController), sinkDescriptor)
    , serverURI(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::SERVER_URI))
    , clientId(generateClientId(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::CLIENT_ID)))
    , topic(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::TOPIC))
    , qos(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::QOS))
    , retained(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::RETAINED))
    , maxOutstandingMessages(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::MAX_OUTSTANDING_MESSAGES))
    , backpressureHandler(
          sinkDescriptor.getFromConfig(SinkDescriptor::BACKPRESSURE_UPPER_THRESHOLD),
          sinkDescriptor.getFromConfig(SinkDescriptor::BACKPRESSURE_LOWER_THRESHOLD))
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

/// apparently, MQTT connect requires synchronization, with concurrent connects (multiple mqtt sinks).
static std::mutex MQTTConnectLock;

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
            std::scoped_lock lock(MQTTConnectLock);
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
        messageSize += buffer.loadChildBuffer(VariableSizedAccess::Index(index)).getNumberOfTuples();
    }
    mqtt::binary payload;
    payload.reserve(messageSize);
    auto data = buffer.getAvailableMemoryArea<char>().first(buffer.getNumberOfTuples());
    payload.append(data.begin(), data.end());
    for (size_t index = 0; index < buffer.getNumberOfChildBuffers(); index++)
    {
        auto child = buffer.loadChildBuffer(VariableSizedAccess::Index(index));
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

Sink::BufferResult MQTTSink::executeBuffer(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pec)
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
                return BufferResult::RETRY;
            }
            case PublishResult::Closed: {
                [[maybe_unused]] auto droppedBuffer = backpressureHandler.onFull(*currentBuffer, backpressureController);
                throw CannotOpenSink("MQTTSink connection to broker {} was closed", serverURI);
            }
        }
    }
    return BufferResult::COMPLETED;
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

DescriptorConfig::Config MQTTSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersMQTTSink>(std::move(config), NAME);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): registry signature fixed by framework.
SinkValidationRegistryReturnType RegisterMQTTSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return MQTTSink::validateAndFormat(std::move(sinkConfig.config));
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): registry signature fixed by framework.
SinkRegistryReturnType RegisterMQTTSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<MQTTSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
