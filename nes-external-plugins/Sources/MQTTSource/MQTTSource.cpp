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

#include "MQTTSource.hpp"

#include <memory>
#include <ostream>
#include <string_view>

#include <Configurations/Descriptor.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::Sources
{

MQTTSource::MQTTSource(const SourceDescriptor& sourceDescriptor)
    : serverURI(sourceDescriptor.getFromConfig(ConfigParametersMQTT::SERVER_URI))
    , topic(sourceDescriptor.getFromConfig(ConfigParametersMQTT::TOPIC))
    , qos(sourceDescriptor.getFromConfig(ConfigParametersMQTT::QOS))
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
        auto connOpts = mqtt::connect_options_builder().automatic_reconnect(true).clean_session(true).finalize();

        client->start_consuming();

        auto token = client->connect(connOpts);
        auto response = token->get_connect_response();

        if (response.is_session_present())
        {
            client->subscribe(topic, 1)->wait();
        }
    }
    catch (const mqtt::exception& e)
    {
    }
}

size_t MQTTSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer)
{
    size_t tbOffset = 0;
    const size_t tbSize = tupleBuffer.getBufferSize();

    std::string_view payload;
    /// If there is a stashed payload, consume it first
    if (!payloadStash.empty())
    {
        payload = payloadStash.consume(tbSize);
        tbOffset = writePayloadToBuffer(payload, tupleBuffer, tbOffset);
    }

    /// When the stashed payload is larger than a single TB, we skip the loop
    while (tbOffset < tbSize)
    {
        auto message = client->consume_message();
        payload = message->get_payload();
        tbOffset = writePayloadToBuffer(payload, tupleBuffer, tbOffset);
    }

    return tbSize;
}

/// Write the message payload to the tuple buffer
/// @return the offset in the TB after writing the payload
size_t MQTTSource::writePayloadToBuffer(std::string_view payload, NES::Memory::TupleBuffer& tb, size_t tbOffset)
{
    /// If the payload fits into the TB, write it
    if (tbOffset + payload.size() <= tb.getBufferSize())
    {
        std::memcpy(tb.getBuffer() + tbOffset, payload.data(), payload.size());
        return tbOffset += payload.size();
    }

    /// Otherwise, split. Write the prefix to the TB and stash the remainder
    auto prefixSize = tb.getBufferSize() - tbOffset;
    std::memcpy(tb.getBuffer() + tbOffset, payload.data(), prefixSize);

    payloadStash.stash(std::string_view{payload.data() + prefixSize, payload.size() - prefixSize});
}

void MQTTSource::close()
{
    client->unsubscribe(topic)->wait();
    client->disconnect()->wait();
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
MQTTSource::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersMQTT>(std::move(config), NAME);
}

namespace SourceValidationGeneratedRegistrar
{
std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
RegisterMQTTSourceValidation(std::unordered_map<std::string, std::string>&& sourceConfig)
{
    return MQTTSource::validateAndFormat(std::move(sourceConfig));
}
}

namespace SourceGeneratedRegistrar
{
std::unique_ptr<Source> RegisterMQTTSource(const SourceDescriptor& sourceDescriptor)
{
    return std::make_unique<MQTTSource>(sourceDescriptor);
}
}

}
