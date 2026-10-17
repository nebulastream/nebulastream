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

#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <MQTTAsync.h>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
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

namespace NES
{

void MQTTSink::ConnectionCallback::connection_lost(const std::string& cause)
{
    *lostCause.wlock() = cause;
}

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
    : Sink(std::move(backpressureController))
    , serverURI(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::SERVER_URI))
    , clientId(generateClientId(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::CLIENT_ID)))
    , topic(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::TOPIC))
    , qos(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::QOS))
    , retained(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::RETAINED))
    , maxOutstandingMessages(sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::MAX_OUTSTANDING_MESSAGES))
    , backpressureHandler(
          sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::BACKPRESSURE_UPPER_THRESHOLD),
          sinkDescriptor.getFromConfig(ConfigParametersMQTTSink::BACKPRESSURE_LOWER_THRESHOLD))
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
    client->set_callback(connectionCallback);
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

void MQTTSink::throwIfConnectionLost() const
{
    if (const auto cause = *connectionCallback.lostCause.rlock(); !cause.empty())
    {
        throw CannotWriteToSink("MQTTSink lost connection to broker {}: {}", serverURI, cause);
    }
}

bool MQTTSink::tryPublish(const TupleBuffer& buffer)
{
    throwIfConnectionLost();

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
            return false;
        }
        throw CannotWriteToSink("MQTTSink publish to topic {} failed: {}", topic, e.what());
    }
    return true;
}

void MQTTSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pec)
{
    PRECONDITION(client, "MQTTSink client is not initialized");
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in MQTTSink.");

    if (const auto retry = backpressureHandler.send(
            inputTupleBuffer, backpressureController, [this](const TupleBuffer& buffer) { return tryPublish(buffer); }))
    {
        pec.repeatTask(*retry, BackpressureHandler::RETRY_INTERVAL);
    }
}

void MQTTSink::stop(PipelineExecutionContext& pec)
{
    if (!client)
    {
        return;
    }
    try
    {
        if (!backpressureHandler.stop(
                backpressureController,
                [this](const TupleBuffer& buffer) { return tryPublish(buffer); },
                [this]
                {
                    throwIfConnectionLost();
                    /// Wait for all in-flight QoS>=1 messages to be acknowledged before disconnecting.
                    /// QoS 0 messages have no pending token; nothing to wait for.
                    return std::ranges::all_of(
                        client->get_pending_delivery_tokens(), [](const auto& token) { return token->is_complete(); });
                }))
        {
            if (stopRetries++ < MAX_STOP_RETRIES)
            {
                pec.repeatTask({}, BackpressureHandler::RETRY_INTERVAL);
                return;
            }
            NES_WARNING("MQTTSink for topic {} could not flush to {} within {} retries", topic, serverURI, MAX_STOP_RETRIES);
            backpressureHandler.close(backpressureController);
        }
        if (client->is_connected())
        {
            client->disconnect()->wait();
        }
    }
    catch (const mqtt::exception& e)
    {
        NES_WARNING("{}", e.what());
    }
    NES_INFO("MQTT Sink completed.");
    client.reset();
}

DescriptorConfig::Config MQTTSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersMQTTSink>(std::move(config), NAME);
}

}
