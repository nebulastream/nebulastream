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

/// Test subscriber for the MQTT sink plugin -- the sink-side mirror of
/// MQTTTestPublisher. Owns the SinkCapture registrar the systest binder
/// invokes for a `SELECT ... INTO MQTT(...)` query.
///
/// At bind time the registrar rewrites the sink's broker endpoint to the
/// dispatcher-allocated host port, installs a paho message callback, then
/// connects and subscribes -- synchronously, before the query runs, so there
/// is no publish-before-subscribe race. From then on paho's own dispatch
/// thread appends each drained message to the systest result file as it
/// arrives.
///
/// The subscriber is handed to the same per-test-file thread vector the source
/// publisher threads use, and joined the same way once the test file is done.
/// It needs no dedicated join before the result check: by the time a query
/// reaches Stopped the broker already holds every message (MQTTSink::stop
/// waits on QoS-1 PUBACKs -- which is also why the capture forces QoS >= 1),
/// and the callback has appended them well before checkResult runs. The schema
/// header line a File sink would write is emitted by the systest binder once
/// schema inference has run. The MQTTSink stays completely test-unaware -- all
/// scaffolding lives here, exactly as MQTTTestPublisher keeps MQTTSource
/// test-unaware.

#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

#include <mqtt/async_client.h>
#include <mqtt/connect_options.h>
#include <mqtt/exception.h>
#include <mqtt/message.h>

#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <ErrorHandling.hpp>
#include <MQTTSink.hpp>
#include <SinkCaptureRegistry.hpp>

namespace NES
{

namespace
{

struct Endpoint
{
    std::string serverURI;
    std::string topic;
    int32_t qos;
};

Endpoint readEndpoint(const std::unordered_map<std::string, std::string>& cfg)
{
    const auto require = [&](const std::string& key) -> std::string
    {
        const auto it = cfg.find(key);
        if (it == cfg.end())
        {
            throw InvalidConfigParameter("MQTT capture: `{}` is required", key);
        }
        return it->second;
    };
    const auto qos = cfg.contains(ConfigParametersMQTTSink::QOS) ? std::stoi(cfg.at(ConfigParametersMQTTSink::QOS)) : 1;
    return {require(ConfigParametersMQTTSink::SERVER_URI), require(ConfigParametersMQTTSink::TOPIC), qos};
}

/// The in-binary dispatcher (ExternalSystestDispatch.cpp) exports
/// NES_EXTERNAL_ENDPOINT_BROKER = `<host>:<port>` when it brings the broker
/// up on a host port. Rewrite the sink's server_uri to it so the MQTTSink
/// under test and this capture subscriber both reach the dispatcher-managed
/// endpoint rather than the symbolic `mqtt_broker:1883` the .test declares.
void applyDispatchOverrides(std::unordered_map<std::string, std::string>& cfg)
{
    /// NOLINTNEXTLINE(concurrency-mt-unsafe): called from registry-time descriptor construction, before any worker thread runs.
    if (const char* endpoint = std::getenv("NES_EXTERNAL_ENDPOINT_BROKER"))
    {
        cfg[ConfigParametersMQTTSink::SERVER_URI] = endpoint;
    }
}

/// The capture relies on `query stopped => broker holds every message`, which
/// holds only for QoS >= 1: MQTTSink::stop waits on PUBACKs before the query
/// can reach Stopped. A QoS-0 sink cannot be captured deterministically by any
/// means, so raise the publish QoS to 1 if the .test left it lower.
void forceAtLeastQos1(std::unordered_map<std::string, std::string>& cfg)
{
    const auto it = cfg.find(ConfigParametersMQTTSink::QOS);
    if (it == cfg.end() or std::stoi(it->second) < 1)
    {
        cfg[ConfigParametersMQTTSink::QOS] = "1";
    }
}

/// Append one MQTT message payload (the MQTTSink publishes the formatted bytes
/// of a TupleBuffer per message) and guarantee a trailing newline so two
/// adjacent messages cannot fuse into one un-parseable record.
void writePayload(std::ofstream& out, std::string_view payload)
{
    out << payload;
    if (!payload.empty() && payload.back() != '\n')
    {
        out << '\n';
    }
}

/// Test-capture-only knob: when set, the registrar rewrites the sink's
/// server_uri to this value *after* readEndpoint has captured the dispatcher
/// endpoint for the capture subscriber. The subscriber still reaches the real
/// broker; the MQTTSink under test opens the bogus URI and fails fast in
/// start(). Lets MQTTSinkRuntime_Unreachable.test cover the "broker
/// unreachable" error path without depending on host networking quirks (a
/// reserved port, an invalid hostname, etc. behave differently across
/// sanitizer builds and CI runners). The knob is consumed and erased here so
/// MQTTSink::validateAndFormat never sees the unknown key -- the sink-side
/// mirror of MQTTTestPublisher's _test_source_uri_override.
constexpr std::string_view kTestSinkUriOverrideKey = "_test_sink_uri_override";

/// Read a test-only config key and erase it from the config so the sink's
/// validateAndFormat never sees the unknown key. Mirrors consumeTestKnob in
/// MQTTTestPublisher.cpp.
std::optional<std::string> consumeTestKnob(std::unordered_map<std::string, std::string>& cfg, const std::string_view key)
{
    const auto it = cfg.find(std::string{key});
    if (it == cfg.end())
    {
        return std::nullopt;
    }
    auto value = it->second;
    cfg.erase(it);
    return value;
}

}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): registry signature fixed by framework.
SinkCaptureRegistryReturnType SinkCaptureGeneratedRegistrar::RegisterMQTTSinkCapture(SinkCaptureRegistryArguments args)
{
    const auto sinkUriOverride = consumeTestKnob(args.sinkConfig, kTestSinkUriOverrideKey);
    applyDispatchOverrides(args.sinkConfig);
    forceAtLeastQos1(args.sinkConfig);
    const auto endpoint = readEndpoint(args.sinkConfig);

    auto client = std::make_unique<mqtt::async_client>(endpoint.serverURI, "nes-systest-mqtt-capture-" + UUIDToString(generateUUID()));

    /// Append each drained payload to the result file as it arrives, on paho's
    /// own dispatch thread. The systest binder writes the schema header before
    /// the query runs, so the callback only ever appends body lines. Results
    /// under test are small; an open/append/close per message is negligible.
    client->set_message_callback(
        [resultFile = args.resultFile](const mqtt::const_message_ptr& message)
        {
            if (not message)
            {
                return;
            }
            try
            {
                std::ofstream out(resultFile, std::ios::app);
                writePayload(out, message->get_payload_str());
            }
            catch (const std::exception& e)
            {
                NES_ERROR("MQTT capture: failed to append to {}: {}", resultFile.string(), e.what());
            }
        });

    try
    {
        client->connect(mqtt::connect_options_builder().clean_session(true).finalize())->wait();
        client->subscribe(endpoint.topic, endpoint.qos)->wait();
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSink("MQTT capture: failed to connect/subscribe to {}: {}", endpoint.serverURI, e.what());
    }

    /// Hand the live subscriber to the shared per-test-file thread vector --
    /// the same one the source publisher threads use. The thread does no
    /// draining itself (the callback does, eagerly, on paho's thread); it just
    /// keeps the subscriber alive for the run and disconnects when the test
    /// file is done and ~jthread requests stop.
    args.serverThreads->push_back(std::jthread(
        [client = std::move(client)](const std::stop_token& stopToken)
        {
            std::mutex mutex;
            std::condition_variable_any stopSignal;
            std::unique_lock lock(mutex);
            stopSignal.wait(lock, stopToken, [] { return false; });
            try
            {
                if (client->is_connected())
                {
                    client->disconnect()->wait();
                }
            }
            catch (const std::exception& e)
            {
                NES_ERROR("MQTT capture: disconnect failed: {}", e.what());
            }
        }));

    /// The capture subscriber above is wired to the real broker; now point the
    /// MQTTSink under test at the test-only override endpoint so its start()
    /// exercises the broker-unreachable path. No-op unless the .test set the knob.
    if (sinkUriOverride.has_value())
    {
        args.sinkConfig[ConfigParametersMQTTSink::SERVER_URI] = sinkUriOverride.value();
    }

    return args.sinkConfig;
}

}
