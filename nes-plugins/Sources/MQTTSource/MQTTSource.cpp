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
#include <cstddef>
#include <cstring>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>

#include <mqtt/connect_options.h>
#include <mqtt/exception.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES::Sources
{

MQTTSource::MQTTSource(const SourceDescriptor& sourceDescriptor)
    : serverURI(sourceDescriptor.getFromConfig(ConfigParametersMQTT::SERVER_URI))
    , clientId(sourceDescriptor.getFromConfig(ConfigParametersMQTT::CLIENT_ID))
    , topic(sourceDescriptor.getFromConfig(ConfigParametersMQTT::TOPIC))
    , qos(sourceDescriptor.getFromConfig(ConfigParametersMQTT::QOS))
    , flushingInterval(std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::duration<float, std::milli>(sourceDescriptor.getFromConfig(ConfigParametersMQTT::FLUSH_INTERVAL_MS))))
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

void MQTTSource::open()
{
    client = std::make_unique<mqtt::async_client>(serverURI, clientId);

    try
    {
        const auto connectOptions = mqtt::connect_options_builder().automatic_reconnect(true).clean_session(false).finalize();

        client->start_consuming();

        const auto token = client->connect(connectOptions);

        if (const auto response = token->get_connect_response(); !response.is_session_present())
        {
            client->subscribe(topic, qos)->wait();
        }
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSource(e.what());
    }
}

size_t MQTTSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, Memory::AbstractBufferProvider&, const std::stop_token& stopToken)
{
    size_t tbOffset = 0;
    const auto tbSize = tupleBuffer.getBufferSize();
    std::string_view payload;
    auto nextFlush = std::chrono::system_clock::now() + std::chrono::milliseconds(flushingInterval);

    /// If there is a stashed payload, consume it first
    if (!payloadStash.empty())
    {
        payload = payloadStash.consume(tbSize);
        writePayloadToBuffer(payload, tupleBuffer, tbOffset);
    }

    /// When the stashed payload is larger than a single TB, we skip the loop
    while (tbOffset < tbSize && !stopToken.stop_requested())
    {
        if (nextFlush < std::chrono::system_clock::now())
        {
            if (tbOffset == 0)
            {
                nextFlush = std::chrono::system_clock::now() + std::chrono::milliseconds(flushingInterval);
            }
            else
            {
                break;
            }
        }

        const auto message = client->try_consume_message_until(nextFlush);
        if (!message)
        {
            continue;
        }

        payload = message->get_payload();
        writePayloadToBuffer(payload, tupleBuffer, tbOffset);
    }
    return std::min(tbOffset, tbSize);
}

/// Write the message payload to the tuple buffer
/// @return the offset in the TB after writing the payload
void MQTTSource::writePayloadToBuffer(const std::string_view payload, Memory::TupleBuffer& tb, size_t& tbOffset)
{
    const auto tbSize = tb.getBufferSize();
    /// If the payload fits into the TB, write it
    if (tbOffset + payload.size() <= tbSize)
    {
        std::memcpy(tb.getBuffer() + tbOffset, payload.data(), payload.size());
        tbOffset += payload.size();
        return;
    }

    /// Otherwise, split. Write the prefix to the TB and stash the remainder
    const auto prefixSize = tbSize - tbOffset;
    std::memcpy(tb.getBuffer() + tbOffset, payload.data(), prefixSize);

    payloadStash.stash(std::string_view{payload.data() + prefixSize, payload.size() - prefixSize});
    tbOffset = tbSize;
}

void MQTTSource::close()
{
    try
    {
        client->unsubscribe(topic)->wait();
        client->disconnect()->wait();
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSource("When closing mqtt source: {}", e.what());
    }
}

Configurations::DescriptorConfig::Config MQTTSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersMQTT>(std::move(config), NAME);
}

SourceValidationRegistryReturnType
SourceValidationGeneratedRegistrar::RegisterMQTTSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return MQTTSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterMQTTSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<MQTTSource>(sourceRegistryArguments.sourceDescriptor);
}

}
