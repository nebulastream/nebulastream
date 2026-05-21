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

/// Test publisher for the MQTT source plugin. Owns the InlineData / FileData
/// registrars that the `.test` runner invokes for `ATTACH INLINE` /
/// `ATTACH FILE`, and the publisher thread that pushes those rows into the
/// broker.
///
/// The publisher only uses one generic source feature, the source's
/// `presencetopic` config: when set, the source publishes its clientId on
/// that topic once its subscription on the data topic is fully wired up.
/// The publisher subscribes to the same topic, waits for the message, then
/// publishes data rows. That avoids the publish-before-subscribe race
/// without a bespoke MQTT control protocol — the source isn't doing
/// anything test-specific, it's just advertising its presence.
///
/// EOS is delivered by the source's existing heartbeat-absence path
/// (`flushIntervalMs` × `maxFlushRetries` of no data ⇒ EOS). The publisher
/// just disconnects after its last row; the source winds down naturally a
/// few hundred milliseconds later.
///
/// Scenarios beyond "publish a fixed row list" — malformed payloads,
/// disconnects mid-stream, idle gaps, switching between good and bad data —
/// are expressible by extending the lambda passed to spawnPublisherThread.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <mqtt/async_client.h>
#include <mqtt/connect_options.h>
#include <mqtt/exception.h>
#include <mqtt/message.h>

#include <Sources/SourceDataProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <MQTTSource.hpp>

namespace NES
{

namespace
{

/// Long enough for sanitizer / coverage / TSAN builds where source startup
/// is measurably slower than the ~100ms observed on a debug build.
constexpr std::chrono::seconds kPresenceTimeout{90};

struct Endpoint
{
    std::string serverURI;
    std::string dataTopic;
    std::string presenceTopic;
    int32_t qos;
};

Endpoint readEndpoint(const PhysicalSourceConfig& cfg)
{
    const auto& src = cfg.sourceConfig;
    const auto require = [&](const std::string& key)
    {
        const auto it = src.find(key);
        if (it == src.end())
        {
            throw InvalidConfigParameter("MQTT publisher: `{}` is required", key);
        }
        return it->second;
    };
    const auto dataTopic = require(ConfigParametersMQTTSource::TOPIC);
    const auto qos = src.contains(ConfigParametersMQTTSource::QOS) ? std::stoi(src.at(ConfigParametersMQTTSource::QOS)) : 1;
    return {require(ConfigParametersMQTTSource::SERVER_URI), dataTopic, dataTopic + "/_nes_presence", qos};
}

/// The in-binary dispatcher (ExternalSystestDispatch.cpp) exports
/// NES_EXTERNAL_ENDPOINT_BROKER = `<host>:<port>` when it brings the broker
/// up on a host port. Rewrite the source's serveruri to that so the source
/// and the publisher both reach the dispatcher-managed endpoint rather than
/// the symbolic `mqtt_broker:1883` the .test file declares.
void applyDispatchOverrides(PhysicalSourceConfig& cfg)
{
    /// NOLINTNEXTLINE(concurrency-mt-unsafe): called from registry-time descriptor construction, before any worker thread runs.
    if (const char* endpoint = std::getenv("NES_EXTERNAL_ENDPOINT_BROKER"))
    {
        cfg.sourceConfig[ConfigParametersMQTTSource::SERVER_URI] = endpoint;
    }
}

/// Turn the source's `presencetopic` knob on so the source advertises its
/// subscription, and the publisher can wait for it. The publisher and the
/// source agree on the derived topic name (`<dataTopic>/_nes_presence`)
/// via readEndpoint above.
void wireSourcePresence(PhysicalSourceConfig& cfg, const std::string& presenceTopic)
{
    cfg.sourceConfig[ConfigParametersMQTTSource::PRESENCE_TOPIC] = presenceTopic;
}

/// Test-publisher-only knob: when set, the publisher rewrites the source's
/// serveruri to this value *after* readEndpoint has captured the dispatcher
/// endpoint for itself. The publisher still reaches the real broker; the
/// source tries to open the bogus URI and fails fast with CannotOpenSource
/// (4002). Lets MQTTRuntime_Unreachable.test cover the "broker unreachable"
/// error path without relying on host networking quirks (a reserved port, an
/// invalid hostname, etc. behave differently across sanitizer builds and CI
/// runners). The knob is consumed and erased here so
/// MQTTSource::validateAndFormat never sees the unknown key.
constexpr std::string_view kTestSourceUriOverrideKey = "_test_source_uri_override";

/// Test-publisher-only knob: when set to a non-negative integer N, the
/// publisher publishes only the first N rows of the ATTACH INLINE content,
/// then disconnects without sending the rest. The source consumes those N
/// rows and EOSes via heartbeat-absence — verifying that a mid-stream
/// publisher disappearance is handled gracefully and the source delivers
/// the partial result rather than hanging or surfacing an error. Like the
/// uri-override knob this is consumed and erased before MQTT validation.
constexpr std::string_view kTestDisconnectAfterKey = "_test_disconnect_after_n_tuples";

std::optional<std::string> consumeTestKnob(PhysicalSourceConfig& cfg, const std::string_view key)
{
    const auto it = cfg.sourceConfig.find(std::string{key});
    if (it == cfg.sourceConfig.end())
    {
        return std::nullopt;
    }
    auto value = it->second;
    cfg.sourceConfig.erase(it);
    return value;
}

/// Strip any trailing CR/LF and append exactly one '\n'. Downstream JSON/CSV
/// parsers split records on newline; without normalisation two adjacent rows
/// in the same TupleBuffer can fuse into a single un-parseable blob.
std::string normalisePayload(std::string_view row)
{
    std::string payload(row);
    while (!payload.empty() && (payload.back() == '\n' || payload.back() == '\r'))
    {
        payload.pop_back();
    }
    payload.push_back('\n');
    return payload;
}

/// Block until the source publishes its clientId on the presence topic. The
/// publisher subscribes to that topic before the source is even constructed
/// (registrar runs synchronously at descriptor-construction time), so any
/// message on it is the source's presence announcement. We don't inspect
/// the payload — there is only one possible sender. Throws on timeout or
/// cancellation.
void waitForPresence(mqtt::async_client& client, const std::string& presenceTopic, const std::stop_token& stopToken)
{
    client.subscribe(presenceTopic, 0)->wait();
    const auto deadline = std::chrono::system_clock::now() + kPresenceTimeout;
    while (std::chrono::system_clock::now() < deadline)
    {
        if (stopToken.stop_requested())
        {
            throw CannotOpenSource("MQTT publisher cancelled while waiting for source presence");
        }
        const auto pollUntil = std::min(deadline, std::chrono::system_clock::now() + std::chrono::milliseconds{100});
        if (client.try_consume_message_until(pollUntil))
        {
            return;
        }
    }
    throw CannotOpenSource("MQTT publisher timed out after {}s waiting for source presence on {}", kPresenceTimeout.count(), presenceTopic);
}

template <typename Iterate>
std::jthread spawnPublisherThread(Endpoint endpoint, Iterate iterate)
{
    return std::jthread(
        [endpoint = std::move(endpoint), iterate = std::move(iterate)](const std::stop_token& stopToken)
        {
            try
            {
                mqtt::async_client client(endpoint.serverURI, "nes-systest-mqtt-loader-" + UUIDToString(generateUUID()));
                client.start_consuming();
                client.connect(mqtt::connect_options_builder().clean_session(true).finalize())->wait();
                waitForPresence(client, endpoint.presenceTopic, stopToken);
                iterate(client, endpoint.dataTopic, endpoint.qos, stopToken);
                client.disconnect()->wait();
            }
            catch (const std::exception& e)
            {
                NES_ERROR("MQTT publisher thread failed: {}", e.what());
            }
        });
}

void publishRow(mqtt::async_client& client, const std::string& dataTopic, int32_t qos, std::string_view row)
{
    const auto payload = normalisePayload(row);
    auto message = mqtt::message::create(dataTopic, payload.data(), payload.size(), qos, /*retained=*/false);
    client.publish(message)->wait();
}

}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterMQTTInlineData(InlineDataRegistryArguments args)
{
    const auto sourceUriOverride = consumeTestKnob(args.physicalSourceConfig, kTestSourceUriOverrideKey);
    const auto disconnectAfter = consumeTestKnob(args.physicalSourceConfig, kTestDisconnectAfterKey);
    applyDispatchOverrides(args.physicalSourceConfig);
    auto endpoint = readEndpoint(args.physicalSourceConfig);
    wireSourcePresence(args.physicalSourceConfig, endpoint.presenceTopic);
    if (sourceUriOverride.has_value())
    {
        args.physicalSourceConfig.sourceConfig[ConfigParametersMQTTSource::SERVER_URI] = sourceUriOverride.value();
    }
    auto rows = std::move(args.tuples);
    if (disconnectAfter.has_value())
    {
        const auto limit = std::stoul(disconnectAfter.value());
        if (limit < rows.size())
        {
            rows.resize(limit);
        }
    }
    args.serverThreads->push_back(spawnPublisherThread(
        std::move(endpoint),
        [rows = std::move(rows)](mqtt::async_client& client, const std::string& dataTopic, int32_t qos, const std::stop_token& stopToken)
        {
            for (const auto& row : rows)
            {
                if (stopToken.stop_requested())
                {
                    return;
                }
                publishRow(client, dataTopic, qos, row);
            }
        }));
    return args.physicalSourceConfig;
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterMQTTFileData(FileDataRegistryArguments args)
{
    if (!std::filesystem::is_regular_file(args.testFilePath))
    {
        throw CannotOpenSource("MQTT FileData: not a regular file: {}", args.testFilePath.string());
    }
    applyDispatchOverrides(args.physicalSourceConfig);
    auto endpoint = readEndpoint(args.physicalSourceConfig);
    wireSourcePresence(args.physicalSourceConfig, endpoint.presenceTopic);
    args.serverThreads->push_back(spawnPublisherThread(
        std::move(endpoint),
        [path = args.testFilePath](mqtt::async_client& client, const std::string& dataTopic, int32_t qos, const std::stop_token& stopToken)
        {
            std::ifstream input(path);
            if (!input.is_open())
            {
                NES_ERROR("MQTT FileData publisher: cannot open {}", path.string());
                return;
            }
            std::string line;
            /// Line-by-line so multi-gigabyte fixtures don't blow up memory.
            while (std::getline(input, line))
            {
                if (stopToken.stop_requested())
                {
                    return;
                }
                if (!line.empty())
                {
                    publishRow(client, dataTopic, qos, line);
                }
            }
        }));
    return args.physicalSourceConfig;
}

}
