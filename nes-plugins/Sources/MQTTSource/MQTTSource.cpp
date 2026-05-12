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

#include <MQTTSource.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <mqtt/client.h>
#include <mqtt/exception.h>
#include <mqtt/message.h>
#include <mqtt/reason_code.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

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

std::optional<std::chrono::milliseconds> parseFlushingInterval(size_t flushingIntervalMs)
{
    if (flushingIntervalMs == 0)
    {
        return std::nullopt;
    }
    return std::chrono::milliseconds(flushingIntervalMs);
}
}

MQTTSource::MQTTSource(const SourceDescriptor& sourceDescriptor)
    : serverURI(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::SERVER_URI))
    , clientId(generateClientId(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::CLIENT_ID)))
    , topic(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::TOPIC))
    , qos(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::QOS))
    , flushingInterval(parseFlushingInterval(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::FLUSH_INTERVAL_MS)))
{
}

std::ostream& MQTTSource::toString(std::ostream& str) const
{
    str << "\nMQTTSource(";
    str << "\n  serverURI: " << serverURI;
    str << "\n  clientId: " << clientId;
    str << "\n  topic: " << topic;
    str << "\n  qos: " << qos;
    str << ")\n";
    return str;
}

void MQTTSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    NES_INFO("Opening MQTTSource at {} using clientId: {}.", serverURI, clientId);
    client = std::make_unique<mqtt::client>(serverURI, clientId);

    try
    {
        client->start_consuming();

        const auto connectResponse = client->connect();
        NES_INFO("Connected to MQTT broker: {}. Version: {}", connectResponse.get_server_uri(), connectResponse.get_mqtt_version());

        const auto subscribeResponse = client->subscribe(topic, qos);
        auto codes = subscribeResponse.get_reason_codes()
            | std::views::transform([](const mqtt::ReasonCode& code) { return magic_enum::enum_name(code); });
        NES_INFO("Subscribed to topic response codes: [\"{}\"]", fmt::join(codes, ","));
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSource(e.what());
    }
}

Source::FillTupleBufferResult MQTTSource::fillTupleBuffer(NES::TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    size_t tbOffset = 0;
    const auto tbSize = tupleBuffer.getBufferSize();
    std::string_view payload;
    auto nextFlush = flushingInterval.transform([](auto flush) { return std::chrono::system_clock::now() + flush; });

    /// If there is a stashed payload, consume it first
    if (!payloadStash.empty())
    {
        payload = payloadStash.consume(tbSize);
        writePayloadToBuffer(payload, tupleBuffer, tbOffset);
    }

    /// When the stashed payload is larger than a single TB, we skip the loop
    while (tbOffset < tbSize && !stopToken.stop_requested())
    {
        if (nextFlush.transform([](auto flush) { return flush < std::chrono::system_clock::now(); }).value_or(true))
        {
            if (tbOffset == 0)
            {
                nextFlush = flushingInterval.transform([](auto flush) { return std::chrono::system_clock::now() + flush; });
            }
            else
            {
                break;
            }
        }

        if (!client->is_connected())
        {
            throw CannotOpenSource("Connection lost");
        }

        mqtt::const_message_ptr message;
        if (nextFlush.has_value())
        {
            /// Interrupt the read
            const std::stop_callback stopCallback(stopToken, [&]() { client->stop_consuming(); });
            if (!client->try_consume_message_until(&message, *nextFlush))
            {
                continue;
            }
        }
        else
        {
            /// Interrupt the read
            const std::stop_callback stopCallback(stopToken, [&]() { client->stop_consuming(); });
            message = client->consume_message();
        }

        if (!message)
        {
            NES_DEBUG("Wakeup Signal received");
            continue;
        }

        payload = message->get_payload();
        writePayloadToBuffer(payload, tupleBuffer, tbOffset);
    }
    return FillTupleBufferResult::withBytes(std::min(tbOffset, tbSize));
}

/// Write the message payload to the tuple buffer
/// @return the offset in the TB after writing the payload
void MQTTSource::writePayloadToBuffer(const std::string_view payload, TupleBuffer& tb, size_t& tbOffset)
{
    const auto tbSize = tb.getBufferSize();
    /// If the payload fits into the TB, write it
    if (tbOffset + payload.size() <= tbSize)
    {
        std::memcpy(tb.getAvailableMemoryArea().data() + tbOffset, payload.data(), payload.size());
        tbOffset += payload.size();
        return;
    }

    /// Otherwise, split. Write the prefix to the TB and stash the remainder
    const auto prefixSize = tbSize - tbOffset;
    std::memcpy(tb.getAvailableMemoryArea().data() + tbOffset, payload.data(), prefixSize);

    payloadStash.stash(std::string_view{payload.data() + prefixSize, payload.size() - prefixSize});
    tbOffset = tbSize;
}

void MQTTSource::close()
{
    try
    {
        client->unsubscribe(topic);
        client->disconnect();
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSource("When closing mqtt source: {}", e.what());
    }
}

DescriptorConfig::Config MQTTSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersMQTTSource>(std::move(config), NAME);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): registry signature fixed by framework.
SourceValidationRegistryReturnType RegisterMQTTSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return MQTTSource::validateAndFormat(std::move(sourceConfig.config));
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): registry signature fixed by framework.
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterMQTTSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<MQTTSource>(sourceRegistryArguments.sourceDescriptor);
}

}
