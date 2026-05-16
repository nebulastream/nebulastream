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
#include <filesystem>
#include <fstream>
#include <memory>
#include <ostream>
#include <ratio>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <mqtt/async_client.h>
#include <mqtt/connect_options.h>
#include <mqtt/exception.h>
#include <mqtt/message.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
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
    , client(nullptr)
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
        const auto connectOptions = mqtt::connect_options_builder().automatic_reconnect(true).clean_session(true).finalize();

        client->start_consuming();

        const auto token = client->connect(connectOptions);

        if (const auto response = token->get_connect_response(); !response.is_session_present())
        {
            client->subscribe(topic, qos)->wait();
        }
        else
        {
            client->subscribe(topic, qos)->wait();
            NES_DEBUG("MQTT open response: (Is session present: {})", response.is_session_present())
        }
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

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterMQTTSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<MQTTSource>(sourceRegistryArguments.sourceDescriptor);
}

namespace
{

/// `ATTACH FILE` / `ATTACH INLINE` for the MQTT source: publish each row to a
/// unique retained topic so the source can subscribe and read it back. The
/// alternative — pre-seeding the broker via a `mqtt_producer` compose service
/// — kept the data file far from the .test file and forced a third yaml/jsonl
/// hop. With these registrars in place, the data lives in the .test file (or a
/// referenced testdata path) and the broker stays a pure dependency.
///
/// Topic semantics: the user's `topic` config is treated as a publish prefix
/// after stripping a trailing `/+` if present (so `test/data/+` and
/// `test/data` both publish to `test/data/0`, `test/data/1`, ...). The source
/// subscribes to whatever topic the user wrote, including the wildcard.
std::string publishPrefix(const PhysicalSourceConfig& cfg)
{
    auto it = cfg.sourceConfig.find(ConfigParametersMQTTSource::TOPIC);
    if (it == cfg.sourceConfig.end())
    {
        throw InvalidConfigParameter("MQTT InlineData/FileData: `topic` is required");
    }
    std::string topic = it->second;
    if (topic.ends_with("/+"))
    {
        topic.resize(topic.size() - 2);
    }
    else if (topic == "+")
    {
        throw InvalidConfigParameter("MQTT InlineData/FileData: `topic` cannot be a bare wildcard");
    }
    return topic;
}

void publishRowAsRetained(mqtt::async_client& client, const std::string& topic, std::string_view row, int32_t qos)
{
    /// The JSON / CSV parsers downstream read the source buffer as a stream
    /// and split records on newline. Append one per published row so two
    /// rows in the same tuple buffer don't fuse into an un-parseable blob.
    /// Any pre-existing trailing CR/LF is stripped first so a CRLF .test
    /// file or a producer-side \n doesn't yield empty rows.
    std::string payload(row);
    while (!payload.empty() && (payload.back() == '\n' || payload.back() == '\r'))
    {
        payload.pop_back();
    }
    payload.push_back('\n');
    auto message = mqtt::message::create(topic, payload.data(), payload.size(), qos, /*retained=*/true);
    client.publish(message)->wait();
}

class Publisher
{
public:
    explicit Publisher(const PhysicalSourceConfig& cfg)
        : serverURI(cfg.sourceConfig.at(ConfigParametersMQTTSource::SERVER_URI))
        , qos(cfg.sourceConfig.contains(ConfigParametersMQTTSource::QOS) ? std::stoi(cfg.sourceConfig.at(ConfigParametersMQTTSource::QOS))
                                                                         : 1)
        , topicPrefix(publishPrefix(cfg))
        , client(serverURI, "nes-systest-mqtt-loader-" + UUIDToString(generateUUID()))
    {
        try
        {
            const auto connectOptions = mqtt::connect_options_builder().clean_session(true).finalize();
            client.connect(connectOptions)->wait();
        }
        catch (const mqtt::exception& e)
        {
            throw CannotOpenSource("MQTT loader could not connect to {}: {}", serverURI, e.what());
        }
    }

    ~Publisher()
    {
        try
        {
            client.disconnect()->wait();
        }
        catch (const mqtt::exception& e)
        {
            NES_WARNING("MQTT loader disconnect failed: {}", e.what());
        }
    }

    Publisher(const Publisher&) = delete;
    Publisher& operator=(const Publisher&) = delete;
    Publisher(Publisher&&) = delete;
    Publisher& operator=(Publisher&&) = delete;

    void publishRow(std::string_view row)
    {
        const auto topic = topicPrefix + "/" + std::to_string(nextIndex++);
        try
        {
            publishRowAsRetained(client, topic, row, qos);
        }
        catch (const mqtt::exception& e)
        {
            throw CannotOpenSource("MQTT loader failed to publish to {}: {}", topic, e.what());
        }
    }

private:
    std::string serverURI;
    int32_t qos;
    std::string topicPrefix;
    mqtt::async_client client;
    size_t nextIndex = 0;
};

}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterMQTTInlineData(InlineDataRegistryArguments args)
{
    Publisher publisher(args.physicalSourceConfig);
    for (const auto& row : args.tuples)
    {
        publisher.publishRow(row);
    }
    return args.physicalSourceConfig;
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterMQTTFileData(FileDataRegistryArguments args)
{
    std::ifstream input(args.testFilePath);
    if (!input.is_open())
    {
        throw CannotOpenSource("MQTT FileData: cannot open {}", args.testFilePath.string());
    }
    /// Stream line-by-line so multi-gigabyte fixtures don't blow up memory —
    /// each publish handles one row before the next read.
    Publisher publisher(args.physicalSourceConfig);
    std::string line;
    while (std::getline(input, line))
    {
        if (line.empty())
        {
            continue;
        }
        publisher.publishRow(line);
    }
    return args.physicalSourceConfig;
}

}
