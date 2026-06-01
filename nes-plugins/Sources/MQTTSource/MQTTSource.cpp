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
#include <ostream>
#include <ratio>
#include <stop_token>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <mqtt/async_client.h>
#include <mqtt/connect_options.h>
#include <mqtt/exception.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <mqtt/token.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

MQTTSource::MQTTSource(const SourceDescriptor& sourceDescriptor)
    : serverURI(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::SERVER_URI))
    , clientId(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::CLIENT_ID))
    , topic(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::TOPIC))
    , qos(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::QOS))
    , flushingInterval(std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::duration<float, std::milli>(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::FLUSH_INTERVAL_MS))))
    , maxFlushRetries(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::MAX_FLUSH_RETRIES))
    , cleanSession(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::CLEAN_SESSION))
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
    client = std::make_unique<mqtt::async_client>(serverURI, clientId);

    try
    {
        /// clean_session(false) opens a persistent session: the broker keeps
        /// the subscription and queues QoS>=1 messages across a disconnect, so
        /// paho's automatic_reconnect — reusing this same client (and its
        /// clientId) — resumes delivery without re-subscribing. Default (true)
        /// keeps the historical clean-session behavior.
        const auto connectOptions = mqtt::connect_options_builder().automatic_reconnect(true).clean_session(cleanSession).finalize();

        client->start_consuming();

        const auto token = client->connect(connectOptions);
        const auto response = token->get_connect_response();
        NES_DEBUG("MQTT open response: (Is session present: {})", response.is_session_present());

        /// `wait()` on the SUBSCRIBE token blocks until the broker delivers
        /// SUBACK. Honors the conn-tests-design-v2.md §3.4 / §5 postcondition
        /// for `MQTTSource::open()` — once we return, the subscription is
        /// fully wired up on the broker side.
        client->subscribe(topic, qos)->wait();
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSource(e.what());
    }
}

Source::FillTupleBufferResult MQTTSource::fillTupleBuffer(NES::TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    size_t tbOffset = 0;
    size_t flushRetries = 0;
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
                if (flushRetries > maxFlushRetries)
                {
                    return FillTupleBufferResult::eos();
                }
                ++flushRetries;
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
    const auto prefix = payload.substr(0, tbSize - tbOffset);
    std::memcpy(tb.getAvailableMemoryArea().data() + tbOffset, prefix.data(), prefix.size());

    payloadStash.stash(payload.substr(prefix.size()));
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

DescriptorConfig::Config MQTTSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersMQTTSource>(std::move(config), NAME);
}

SourceValidationRegistryReturnType RegisterMQTTSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return MQTTSource::validateAndFormat(std::move(sourceConfig.config));
}

/// The matching declaration in the auto-generated SourceGeneratedRegistrar.inc takes SourceRegistryArguments by value; a
/// const-ref definition here would not match (compile error). The generator owns the calling convention. The function is
/// also reached only by name from the registrar; `static` / anonymous-namespace would hide it from the registrar resolver.
/// NOLINTBEGIN(performance-unnecessary-value-param, misc-use-internal-linkage)
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterMQTTSource(SourceRegistryArguments sourceRegistryArguments)
/// NOLINTEND(performance-unnecessary-value-param, misc-use-internal-linkage)
{
    return std::make_unique<MQTTSource>(sourceRegistryArguments.sourceDescriptor);
}

}
