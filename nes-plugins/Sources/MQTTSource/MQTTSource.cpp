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
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <ostream>
#include <ratio>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
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
#include <Sources/SourceDataProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <mqtt/token.h>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

namespace
{
/// Convention: control messages flow on `<data-topic>/_nes_control`. Putting
/// the suffix under the user-chosen topic prefix keeps it inside the
/// test-scoped namespace, avoids `$`-prefixed broker-reserved topics, and
/// makes it obvious in mosquitto logs what each message is for.
std::string deriveControlTopic(std::string_view dataTopic)
{
    return std::string(dataTopic) + "/_nes_control";
}
}

MQTTSource::MQTTSource(const SourceDescriptor& sourceDescriptor)
    : serverURI(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::SERVER_URI))
    , clientId(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::CLIENT_ID))
    , topic(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::TOPIC))
    , qos(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::QOS))
    , flushingInterval(std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::duration<float, std::milli>(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::FLUSH_INTERVAL_MS))))
    , maxFlushRetries(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::MAX_FLUSH_RETRIES))
    , controlChannelEnabled(sourceDescriptor.getFromConfig(ConfigParametersMQTTSource::CONTROL_CHANNEL_ENABLED))
    , dataTopic(topic)
    , controlTopic(deriveControlTopic(topic))
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
        const auto response = token->get_connect_response();
        NES_DEBUG("MQTT open response: (Is session present: {})", response.is_session_present());

        client->subscribe(dataTopic, qos)->wait();
        if (controlChannelEnabled)
        {
            /// Signal one-way to the in-process publisher (set up by the
            /// InlineData/FileData registrar) that the source is live and
            /// safe to send to. We don't subscribe to the control topic at
            /// the source side — EOS is delivered as a zero-byte sentinel
            /// on the *data* topic instead, so both data and end-of-stream
            /// share a single ordered MQTT subscription (the broker does
            /// not guarantee delivery order across separate subscriptions
            /// on the same client, which used to race the EOS marker ahead
            /// of trailing data rows).
            client->publish(controlTopic, "ready", qos, /*retained=*/false)->wait();
        }
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSource(e.what());
    }
}

Source::FillTupleBufferResult MQTTSource::fillTupleBuffer(NES::TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    /// If an EOS sentinel (a zero-byte message on the data topic) was consumed
    /// during a previous fillTupleBuffer call — or mid-loop below, after we'd
    /// committed bytes to the buffer — return EoS immediately so the query
    /// engine flushes the last window / aggregator and the source thread exits.
    /// This short-circuit replaces the heartbeat-absence flush-retry timeout
    /// when the control-channel handshake is in play.
    if (eosReceived)
    {
        return FillTupleBufferResult::eos();
    }

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
                /// When the control-channel handshake is in play, the *only*
                /// legitimate EoS signal is the empty-payload sentinel on the
                /// data topic. Heartbeat-absence is meaningless during a batch
                /// load — the publisher can spend tens of seconds publishing
                /// before paho's incoming queue refills, and EoS-ing here
                /// would drop the trailing rows it hasn't sent yet. Keep
                /// polling; stopToken at the loop top still allows cancellation.
                if (!controlChannelEnabled && flushRetries > maxFlushRetries)
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

        /// EOS sentinel: a zero-byte message on the data topic, written by
        /// the in-process publisher after its last data row. Reusing the data
        /// topic for EOS keeps everything on one ordered MQTT subscription,
        /// so the broker can't deliver EOS ahead of trailing data the way it
        /// could when EOS lived on a sibling control topic. Empty real rows
        /// never reach the broker because the publisher skips empty input
        /// lines, so the empty payload is unambiguous.
        if (controlChannelEnabled && payload.empty())
        {
            eosReceived = true;
            if (tbOffset == 0)
            {
                return FillTupleBufferResult::eos();
            }
            return FillTupleBufferResult::withBytes(tbOffset);
        }

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
        client->unsubscribe(dataTopic)->wait();
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

namespace
{

/// `ATTACH FILE` / `ATTACH INLINE` for the MQTT source. The earlier scheme
/// pre-published every row as a *retained* message on its own topic
/// (`test/data/0`, `/1`, …) so that whenever the source eventually subscribed,
/// mosquitto would replay them all. That worked but leaked broker-side knobs
/// (`max_queued_messages`, retained-tree size) into test-time tuning, and
/// dropped messages silently above the default queue cap.
///
/// New shape: a publish-after-subscribe handshake on a small control topic.
/// The registrar spawns a publisher thread that connects to the broker,
/// subscribes to `<topic>/_nes_control`, and waits for the source to send
/// "ready" on that topic. Once it sees "ready", it publishes every row on
/// `topic` (non-retained QoS=1), then publishes "eos" on the control topic
/// and disconnects. The source consumes data on `topic` and EOS-es when it
/// sees "eos" on the control topic. No retained messages, one data topic,
/// no broker config bumps needed, and EOS becomes a protocol event rather
/// than a heartbeat-absence timeout.
///
/// The thread lives in `args.serverThreads` — same lifetime contract TCP's
/// in-process mock server uses, so cancellation on shutdown is uniform.
/// 90s — long enough for sanitizer / TSAN / coverage builds where binder
/// startup is measurably slower than the ~1s observed on a debug build.
/// Below ~60s a TSAN run on CI can routinely time out before the source's
/// open() reaches the handshake point, leaving the publisher dead and the
/// test failing with an all-empty result. The handshake itself is fast;
/// this is purely the headroom for the source to spin up.
constexpr std::chrono::milliseconds kDefaultReadyTimeout{90000};
constexpr std::string_view kControlReady = "ready";

/// Accepts the ConfigParameter<T> static instances directly via their implicit
/// `operator const std::string&()`, so callers can write
/// `getRequired(cfg, ConfigParametersMQTTSource::TOPIC)` rather than spelling
/// the parameter name as a literal twice (and risking drift on rename).
std::string getRequired(const std::unordered_map<std::string, std::string>& cfg, const std::string& key)
{
    auto it = cfg.find(key);
    if (it == cfg.end())
    {
        throw InvalidConfigParameter("MQTT publisher: `{}` is required", key);
    }
    return it->second;
}

std::string normalisePayload(std::string_view row)
{
    /// JSON / CSV parsers downstream split records on newline. Strip any
    /// pre-existing trailing CR/LF (CRLF .test files, producer-side \n) and
    /// append exactly one \n, so two consecutive rows in the same tuple
    /// buffer don't fuse into an un-parseable blob.
    std::string payload(row);
    while (!payload.empty() && (payload.back() == '\n' || payload.back() == '\r'))
    {
        payload.pop_back();
    }
    payload.push_back('\n');
    return payload;
}

/// The publisher half of the control-channel handshake. Owns its own paho
/// client (separate from the source's). Constructed inside a publisher
/// thread; lifetime ends when the thread exits.
class Publisher
{
public:
    /// Trailing underscore on the parameters is required — same-name parameters
    /// would shadow the members in the initializer list, making `std::move(name)`
    /// move the parameter and the next initializer reading `name` a use-after-move.
    Publisher(std::string serverURI_, std::string dataTopic_, int32_t qos_) /// NOLINT(readability-identifier-naming)
        : serverURI(std::move(serverURI_))
        , dataTopic(std::move(dataTopic_))
        , controlTopic(deriveControlTopic(dataTopic))
        , qos(qos_)
        /// kMaxBufferedMessages caps paho's outgoing-message queue at the
        /// client side. The 2-arg constructor defaults to ~10 — too low for
        /// the batched-wait publisher loop, which keeps up to
        /// kPublishBatchSize tokens unwaited. Set it high enough that the
        /// publisher never blocks on the queue cap; broker-side
        /// max_inflight_messages is the actual rate-limiting knob.
        , client(serverURI, "nes-systest-mqtt-loader-" + UUIDToString(generateUUID()), kMaxBufferedMessages)
    {
        try
        {
            /// max_inflight raises the client-side QoS=1 inflight window
            /// from paho's default (10) up to something that lets the
            /// batched publisher fill the broker-side queue. Effective
            /// inflight is min(client max_inflight, mosquitto's
            /// max_inflight_messages); the broker's is set to 200 in
            /// mosquitto.conf.
            const auto connectOptions = mqtt::connect_options_builder().clean_session(true).max_inflight(kClientMaxInflight).finalize();
            client.start_consuming();
            client.connect(connectOptions)->wait();
            client.subscribe(controlTopic, qos)->wait();
        }
        catch (const mqtt::exception& e)
        {
            throw CannotOpenSource("MQTT loader could not connect/subscribe at {}: {}", serverURI, e.what());
        }
    }

    ~Publisher()
    {
        try
        {
            client.unsubscribe(controlTopic)->wait();
            client.disconnect()->wait();
        }
        catch (const mqtt::exception& e)
        {
            NES_WARNING("MQTT loader cleanup failed: {}", e.what());
        }
    }

    Publisher(const Publisher&) = delete;
    Publisher& operator=(const Publisher&) = delete;
    Publisher(Publisher&&) = delete;
    Publisher& operator=(Publisher&&) = delete;

    /// Block until the source publishes "ready" on the control topic, or
    /// until `timeout` elapses, or until `stopToken` is requested. Throws on
    /// timeout — the source never coming up is a test setup error worth
    /// surfacing loudly.
    void waitForReady(std::chrono::milliseconds timeout, const std::stop_token& stopToken)
    {
        const auto deadline = std::chrono::system_clock::now() + timeout;
        constexpr auto pollInterval = std::chrono::milliseconds{100};
        while (std::chrono::system_clock::now() < deadline)
        {
            if (stopToken.stop_requested())
            {
                throw CannotOpenSource("MQTT loader cancelled while waiting for subscriber ready");
            }
            const auto pollUntil = std::min(deadline, std::chrono::system_clock::now() + pollInterval);
            const auto message = client.try_consume_message_until(pollUntil);
            if (!message)
            {
                continue;
            }
            if (message->get_topic() == controlTopic && message->get_payload() == kControlReady)
            {
                return;
            }
            /// Anything else on the control topic — including an "eos" left
            /// from a previous run — is benign; keep polling.
        }
        throw CannotOpenSource("MQTT loader timed out after {}ms waiting for subscriber ready on {}", timeout.count(), controlTopic);
    }

    /// Publish one row on the data topic, *without* waiting for its PUBACK.
    /// The returned token goes into `pendingTokens` and is drained either
    /// when the batch hits `kPublishBatchSize` or when `publishEos()` runs.
    /// At QoS=1 the broker still durably receives every message — we just
    /// don't pay one RTT per row, which is the dominant cost when publishing
    /// to a local broker over millions of rows. PUBACKs are still required
    /// for delivery; mosquitto's max_inflight_messages governs the actual
    /// window of concurrent in-flight publishes (bumped to 200 in
    /// mosquitto.conf so a batch isn't held up serially on the broker side).
    void publishRow(std::string_view row)
    {
        try
        {
            const auto payload = normalisePayload(row);
            auto message = mqtt::message::create(dataTopic, payload.data(), payload.size(), qos, /*retained=*/false);
            pendingTokens.push_back(client.publish(message));
            if (pendingTokens.size() >= kPublishBatchSize)
            {
                drainPending();
            }
        }
        catch (const mqtt::exception& e)
        {
            throw CannotOpenSource("MQTT loader failed to publish to {}: {}", dataTopic, e.what());
        }
    }

    /// End-of-stream sentinel: a zero-byte message published on the *data*
    /// topic, *after* the last data row. Keeps EOS on the same MQTT
    /// subscription as the data, so the broker's per-subscription ordering
    /// guarantee applies — EOS cannot be delivered ahead of trailing rows.
    /// Empty rows are filtered upstream in the file-reading loop, so a
    /// zero-byte message is unambiguously the sentinel.
    ///
    /// Drains any pending publish tokens *before* sending the EOS marker, so
    /// the broker is guaranteed to have all data rows durably stored before
    /// the EOS event the source EOS-es on. Otherwise the EOS PUBACK could
    /// race ahead of trailing data PUBACKs and the source would EOS with the
    /// tail in flight (per-subscription ordering only guarantees the *send*
    /// order, not the broker's storage order).
    void publishEos()
    {
        try
        {
            drainPending();
            auto message = mqtt::message::create(dataTopic, "", 0, qos, /*retained=*/false);
            client.publish(message)->wait();
        }
        catch (const mqtt::exception& e)
        {
            NES_WARNING("MQTT loader failed to publish eos on {}: {}", dataTopic, e.what());
        }
    }

private:
    /// 1000 buffers a row of headroom while keeping memory bounded. With
    /// mosquitto's `max_inflight_messages=200` the broker concurrently
    /// processes ~200 of these at a time, so batch sizes above ~1000 stop
    /// helping. Tuned for the 1M / 5M YSB cases; smaller tests drain on
    /// EOS regardless.
    static constexpr std::size_t kPublishBatchSize = 1000;

    /// paho's client-side buffer; must be larger than kPublishBatchSize so
    /// the publisher never trips MQTTASYNC_MAX_BUFFERED (-12) while batches
    /// drain. Effectively unlimited for our scale (1M rows × a few hundred
    /// bytes = ~250MB at the absolute worst, much less in practice because
    /// the broker drains continuously).
    static constexpr int kMaxBufferedMessages = 100000;

    /// Client-side QoS=1 inflight window. paho's default of 10 caps
    /// effective throughput at ~10 messages per RTT regardless of how
    /// quickly the broker acks; bumping this lets the broker's
    /// max_inflight_messages window (200) be the actual bottleneck.
    static constexpr int kClientMaxInflight = 1000;

    void drainPending()
    {
        for (auto& token : pendingTokens)
        {
            token->wait();
        }
        pendingTokens.clear();
    }

    std::string serverURI;
    std::string dataTopic;
    std::string controlTopic;
    int32_t qos;
    mqtt::async_client client;
    std::vector<mqtt::token_ptr> pendingTokens;
};

/// Pull what the publisher needs out of the descriptor's stringly-typed map.
/// Done once at thread spawn so we never touch PhysicalSourceConfig from the
/// background thread.
struct PublisherConfig
{
    std::string serverURI;
    std::string dataTopic;
    int32_t qos{};
    std::chrono::milliseconds readyTimeout{};
};

PublisherConfig extractPublisherConfig(const PhysicalSourceConfig& cfg)
{
    PublisherConfig out;
    out.serverURI = getRequired(cfg.sourceConfig, ConfigParametersMQTTSource::SERVER_URI);
    out.dataTopic = getRequired(cfg.sourceConfig, ConfigParametersMQTTSource::TOPIC);
    out.qos
        = cfg.sourceConfig.contains(ConfigParametersMQTTSource::QOS) ? std::stoi(cfg.sourceConfig.at(ConfigParametersMQTTSource::QOS)) : 1;
    /// Deliberately *not* tied to the source's flush-retry budget. The
    /// publisher is waiting for the source to come up (query planning +
    /// executor setup + source open()), which is much slower than the
    /// "next-message timeout" the source's flush settings measure. 30s
    /// is generous enough that a slow test environment doesn't false-trip,
    /// and the std::jthread stop_token unwinds it early on shutdown.
    out.readyTimeout = kDefaultReadyTimeout;
    return out;
}

/// Tag the source descriptor as control-channel-enabled. The systest binder
/// passes this back to the source's constructor, which then knows to do the
/// "ready" handshake in open(). Decoupled in the sense that the source plugin
/// never references the registrar — it just reads its config.
void enableControlChannel(PhysicalSourceConfig& cfg)
{
    cfg.sourceConfig[ConfigParametersMQTTSource::CONTROL_CHANNEL_ENABLED] = "true";
}

/// When the in-binary dispatcher in ExternalSystestDispatch.cpp brings up the
/// broker locally on a host port, it exports NES_EXTERNAL_ENDPOINT_BROKER =
/// `<host>:<port>` for the `broker` endpoint declared in the profile's
/// profile.yaml. The MQTT source's serveruri gets rewritten to this so the
/// in-process source and publisher reach the broker at the dispatcher-managed
/// endpoint instead of the symbolic `mqtt_broker:1883` the .test file
/// declares. Applied here (in the registrar) rather than in the source's
/// constructor so the rewritten URI lives in the descriptor and both the
/// source and the publisher pick it up consistently.
void applyDispatchOverrides(PhysicalSourceConfig& cfg)
{
    /// NOLINTNEXTLINE(concurrency-mt-unsafe): called from registry-time descriptor construction, before any worker thread runs.
    if (const char* endpoint = std::getenv("NES_EXTERNAL_ENDPOINT_BROKER"))
    {
        cfg.sourceConfig[ConfigParametersMQTTSource::SERVER_URI] = endpoint;
    }
}

}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterMQTTInlineData(InlineDataRegistryArguments args)
{
    applyDispatchOverrides(args.physicalSourceConfig);
    auto config = extractPublisherConfig(args.physicalSourceConfig);
    auto rows = std::move(args.tuples);

    auto publisherThread = std::jthread(
        [config = std::move(config), rows = std::move(rows)](const std::stop_token& stopToken)
        {
            try
            {
                Publisher publisher(config.serverURI, config.dataTopic, config.qos);
                publisher.waitForReady(config.readyTimeout, stopToken);
                for (const auto& row : rows)
                {
                    if (stopToken.stop_requested())
                    {
                        return;
                    }
                    publisher.publishRow(row);
                }
                publisher.publishEos();
            }
            catch (const std::exception& e)
            {
                NES_ERROR("MQTT InlineData publisher thread failed: {}", e.what());
            }
        });
    args.serverThreads->push_back(std::move(publisherThread));
    enableControlChannel(args.physicalSourceConfig);
    return args.physicalSourceConfig;
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterMQTTFileData(FileDataRegistryArguments args)
{
    if (!std::filesystem::is_regular_file(args.testFilePath))
    {
        throw CannotOpenSource("MQTT FileData: not a regular file: {}", args.testFilePath.string());
    }
    applyDispatchOverrides(args.physicalSourceConfig);
    auto config = extractPublisherConfig(args.physicalSourceConfig);
    auto path = args.testFilePath;

    auto publisherThread = std::jthread(
        [config = std::move(config), path = std::move(path)](const std::stop_token& stopToken)
        {
            try
            {
                std::ifstream input(path);
                if (!input.is_open())
                {
                    NES_ERROR("MQTT FileData publisher: cannot open {}", path.string());
                    return;
                }
                Publisher publisher(config.serverURI, config.dataTopic, config.qos);
                publisher.waitForReady(config.readyTimeout, stopToken);
                std::string line;
                /// Stream line-by-line so multi-gigabyte fixtures don't blow
                /// up memory — each publish handles one row before the next read.
                while (std::getline(input, line))
                {
                    if (stopToken.stop_requested())
                    {
                        return;
                    }
                    if (line.empty())
                    {
                        continue;
                    }
                    publisher.publishRow(line);
                }
                publisher.publishEos();
            }
            catch (const std::exception& e)
            {
                NES_ERROR("MQTT FileData publisher thread failed: {}", e.what());
            }
        });
    args.serverThreads->push_back(std::move(publisherThread));
    enableControlChannel(args.physicalSourceConfig);
    return args.physicalSourceConfig;
}

}
