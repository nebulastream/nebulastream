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

#include <KafkaSource.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
constexpr std::chrono::seconds WARNING_INTERVAL{10};
/// Warn once a lot of detached close() teardowns (see closeConsumer()) are in flight at once: each one holds a
/// librdkafka client context alive until its own internal timeout elapses, so a source that repeatedly opens and
/// closes against a persistently unreachable broker could otherwise pile these up unbounded.
constexpr uint64_t DETACHED_TEARDOWN_WARNING_THRESHOLD = 8;
constexpr size_t MAX_TOPIC_NAME_LENGTH = 249;
std::atomic<uint64_t> inFlightDetachedTeardowns{0};

/// True at most once per WARNING_INTERVAL per call site; keeps an outage from flooding the log.
bool shouldWarn(std::chrono::steady_clock::time_point& last)
{
    const auto now = std::chrono::steady_clock::now();
    if (now - last < WARNING_INTERVAL)
    {
        return false;
    }
    last = now;
    return true;
}

/// Text that librdkafka or a broker supplied, made safe for one log line: control characters (line breaks would forge log lines) become '?'
/// and the text is cut at 512 bytes.
std::string printableForLog(std::string_view text)
{
    constexpr size_t MAX_LENGTH = 512;
    std::string result(text.substr(0, MAX_LENGTH));
    std::ranges::replace_if(
        result,
        [](const char character) { return static_cast<unsigned char>(character) < 0x20 || static_cast<unsigned char>(character) == 0x7f; },
        '?');
    return result;
}

void throwIfNotOk(const RdKafka::Conf::ConfResult result, const std::string& key, const std::string& error)
{
    if (result != RdKafka::Conf::CONF_OK)
    {
        throw CannotOpenSource("Kafka source: cannot set consumer option {}: {}", key, error);
    }
}

/// Sets a string option. Taking `const std::string&` (not a forwarding template) keeps string literals from decaying inside the call.
void setOrThrow(RdKafka::Conf& conf, const std::string& key, const std::string& value)
{
    std::string error;
    throwIfNotOk(conf.set(key, value, error), key, error);
}

/// Registers a callback (any non-string pointer type RdKafka::Conf::set overloads accept).
template <typename Callback>
requires(std::is_pointer_v<Callback> && !std::is_convertible_v<Callback, std::string>)
void setOrThrow(RdKafka::Conf& conf, const std::string& key, Callback callback)
{
    std::string error;
    throwIfNotOk(conf.set(key, callback, error), key, error);
}

std::string toLowerCase(std::string text)
{
    std::ranges::transform(
        text, text.begin(), [](const char character) { return static_cast<char>(std::tolower(static_cast<unsigned char>(character))); });
    return text;
}

std::string toUpperCase(std::string text)
{
    std::ranges::transform(
        text, text.begin(), [](const char character) { return static_cast<char>(std::toupper(static_cast<unsigned char>(character))); });
    return text;
}

bool isValidTopicName(const std::string_view name)
{
    return !name.empty() && name.size() <= MAX_TOPIC_NAME_LENGTH && name != "." && name != ".."
        && std::ranges::all_of(
            name,
            [](const char character)
            {
                return std::isalnum(static_cast<unsigned char>(character)) != 0 || character == '.' || character == '_' || character == '-';
            });
}

/// NOLINTBEGIN(bugprone-throwing-static-initialization)
const ConfigField<std::string> BROKERS{
    Identifier::parse("BROKERS"),
    "Comma-separated Kafka bootstrap servers (librdkafka bootstrap.servers)",
    [](const ConfigLiteral& literal) -> std::expected<std::string, Exception>
    {
        auto value = tryGetOr<std::string>(literal, expectedType<std::string>());
        if (value && value->empty())
        {
            return std::unexpected{InvalidConfigParameter("KafkaSource: BROKERS must not be empty")};
        }
        return value;
    }};

const ConfigField<std::string> TOPIC{
    Identifier::parse("TOPIC"),
    "The single Kafka topic to consume; all of its partitions are read",
    [](const ConfigLiteral& literal) -> std::expected<std::string, Exception>
    {
        auto value = tryGetOr<std::string>(literal, expectedType<std::string>());
        if (value && !isValidTopicName(*value))
        {
            return std::unexpected{InvalidConfigParameter(
                "KafkaSource: TOPIC must be one topic name of letters, digits, '.', '_' and '-' (at most {} characters), got '{}'",
                MAX_TOPIC_NAME_LENGTH,
                *value)};
        }
        return value;
    }};

const ConfigField<std::string> GROUP_ID{
    Identifier::parse("GROUP_ID"),
    "The consumer group the offsets are committed under",
    [](const ConfigLiteral& literal) -> std::expected<std::string, Exception>
    {
        auto value = tryGetOr<std::string>(literal, expectedType<std::string>());
        if (value && value->empty())
        {
            return std::unexpected{InvalidConfigParameter("KafkaSource: GROUP_ID must not be empty")};
        }
        return value;
    }};

const ConfigField<std::string> AUTO_OFFSET_RESET{
    Identifier::parse("AUTO_OFFSET_RESET"),
    "Where to start without a committed offset, or when it is out of range: latest or earliest",
    [](const ConfigLiteral& literal) -> std::expected<std::string, Exception>
    {
        return tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](std::string value) -> std::expected<std::string, Exception>
                {
                    value = toLowerCase(std::move(value));
                    if (value != "latest" && value != "earliest")
                    {
                        return std::unexpected{
                            InvalidConfigParameter("KafkaSource: AUTO_OFFSET_RESET must be latest or earliest, got '{}'", value)};
                    }
                    return value;
                });
    },
    std::string{"latest"}};

const ConfigField<std::string> COMMIT_MODE{
    Identifier::parse("COMMIT_MODE"),
    "SYNC waits for the broker to acknowledge each offset commit, ASYNC does not wait",
    [](const ConfigLiteral& literal) -> std::expected<std::string, Exception>
    {
        return tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](std::string value) -> std::expected<std::string, Exception>
                {
                    value = toUpperCase(std::move(value));
                    if (value != "SYNC" && value != "ASYNC")
                    {
                        return std::unexpected{InvalidConfigParameter("KafkaSource: COMMIT_MODE must be SYNC or ASYNC, got '{}'", value)};
                    }
                    return value;
                });
    },
    std::string{"SYNC"}};

const ConfigField<int64_t> FLUSH_INTERVAL_MS{
    Identifier::parse("FLUSH_INTERVAL_MS"),
    "Longest time in ms a partly filled tuple buffer is held; 0 waits until the buffer is full",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<int64_t>(literal, expectedType<int64_t>())
            .and_then(narrowConfigValue<int64_t, int64_t, std::numeric_limits<int64_t>::max(), 0>);
    },
    int64_t{0}};

const ConfigField<std::string> IMPLICIT_MESSAGE_DELIMITER{
    Identifier::parse("IMPLICIT_MESSAGE_DELIMITER"), "Delimiter appended to each received message", std::string{"\n"}};
/// NOLINTEND(bugprone-throwing-static-initialization)
}

Schema<QualifiedErasedConfigField, Ordered> KafkaSource::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("KAFKA_SOURCE"),
        BROKERS,
        TOPIC,
        GROUP_ID,
        AUTO_OFFSET_RESET,
        COMMIT_MODE,
        FLUSH_INTERVAL_MS,
        IMPLICIT_MESSAGE_DELIMITER);
}

std::expected<KafkaSourceConfig, Exception> KafkaSourceConfig::fromConfig(const InstantiatedConfig& config)
{
    return KafkaSourceConfig{
        .brokers = config.get(BROKERS),
        .topic = config.get(TOPIC),
        .groupId = config.get(GROUP_ID),
        .autoOffsetReset = config.get(AUTO_OFFSET_RESET),
        .synchronousCommit = config.get(COMMIT_MODE) == "SYNC",
        .flushIntervalMs = config.get(FLUSH_INTERVAL_MS),
        .implicitMessageDelimiter = config.get(IMPLICIT_MESSAGE_DELIMITER),
    };
}

/// Connection-level errors that retrying cannot fix: rejected credentials, a rejected certificate or trust store (a broker restart shows
/// as a transport error instead), a fatal client error. Same set as KafkaSink's event handler.
bool KafkaSource::isFatalConnectionError(const RdKafka::ErrorCode code)
{
    return code == RdKafka::ERR__AUTHENTICATION || code == RdKafka::ERR__SSL || code == RdKafka::ERR__FATAL;
}

/// Errors after which the source cannot make progress; everything else (transport, broker down, ...)
/// is transient and librdkafka reconnects on its own.
bool KafkaSource::isFatalError(const RdKafka::ErrorCode code)
{
    switch (code)
    {
        case RdKafka::ERR__UNKNOWN_TOPIC:
        case RdKafka::ERR__UNKNOWN_PARTITION:
        case RdKafka::ERR_UNKNOWN_TOPIC_OR_PART:
        case RdKafka::ERR_TOPIC_AUTHORIZATION_FAILED:
        case RdKafka::ERR_GROUP_AUTHORIZATION_FAILED:
        case RdKafka::ERR__INVALID_ARG:
            return true;
        default:
            return isFatalConnectionError(code);
    }
}

void KafkaSource::EventCallback::event_cb(RdKafka::Event& event)
{
    switch (event.type())
    {
        case RdKafka::Event::EVENT_ERROR:
            if (isFatalConnectionError(event.err()))
            {
                /// Served inside consume(): the next fill turns the latch into a query failure instead of waiting for brokers that
                /// will keep refusing.
                auto expected = RdKafka::ERR_NO_ERROR;
                if (fatalError.compare_exchange_strong(expected, event.err()))
                {
                    NES_ERROR("Kafka source: connection error that retrying cannot fix: {}", printableForLog(event.str()));
                }
                break;
            }
            /// Transient errors (broker down, transport) are reported here while librdkafka reconnects.
            if (shouldWarn(lastWarning))
            {
                NES_WARNING(
                    "Kafka source: {} ({}), librdkafka keeps reconnecting", printableForLog(event.str()), RdKafka::err2str(event.err()));
            }
            break;
        case RdKafka::Event::EVENT_LOG:
            /// With auto.offset.reset=earliest|latest librdkafka resets an out-of-range committed offset itself and only
            /// logs it; surface it, since the skipped range is data loss the operator must see.
            if (event.fac() == "OFFSET" && event.str().find("offset reset") != std::string::npos)
            {
                NES_WARNING("Kafka source: committed offset is out of range, applying AUTO_OFFSET_RESET: {}", event.str());
            }
            else
            {
                NES_DEBUG("librdkafka [{}] {}", event.fac(), event.str());
            }
            break;
        default:
            break;
    }
}

KafkaSource::KafkaSource(const KafkaSourceConfig& config)
    : brokers(config.brokers)
    , topic(config.topic)
    , groupId(config.groupId)
    , autoOffsetReset(config.autoOffsetReset)
    , synchronousCommit(config.synchronousCommit)
    , implicitDelimiter(config.implicitMessageDelimiter)
    , flushInterval(config.flushIntervalMs)
{
}

KafkaSource::~KafkaSource()
{
    closeConsumer();
}

void KafkaSource::closeConsumer()
{
    if (!consumer)
    {
        return;
    }
    /// KafkaConsumer::close() waits for the group coordinator without a timeout, which never answers while the brokers
    /// are down. Close through a queue with a deadline instead.
    auto* handle = consumer->c_ptr();
    std::unique_ptr<rd_kafka_queue_t, decltype(&rd_kafka_queue_destroy)> events{rd_kafka_queue_new(handle), rd_kafka_queue_destroy};
    if (auto* error = rd_kafka_consumer_close_queue(handle, events.get()))
    {
        NES_WARNING("Kafka source: closing the consumer failed: {}", rd_kafka_error_string(error));
        rd_kafka_error_destroy(error);
    }
    const auto deadline = std::chrono::steady_clock::now() + COMMIT_TIMEOUT;
    while (rd_kafka_consumer_closed(handle) == 0 && std::chrono::steady_clock::now() < deadline)
    {
        if (auto* event = rd_kafka_queue_poll(events.get(), static_cast<int>(POLL_TIMEOUT.count())))
        {
            rd_kafka_event_destroy(event);
        }
    }
    /// events references handle; it must be destroyed before consumer (which destroys handle via
    /// rd_kafka_destroy()), otherwise that destroy call waits on the queue we never released.
    events.reset();
    if (rd_kafka_consumer_closed(handle) != 0)
    {
        /// Already fully closed: consumer.reset() (rd_kafka_destroy_flags) is fast here, nothing left to wait on.
        consumer.reset();
        return;
    }
    /// Not yet closed (broker unreachable): consumer.reset() would still block past our own deadline above.
    /// rd_kafka_destroy_app() re-issues consumer_close(), which is a no-op since we already started termination,
    /// then blocks joining librdkafka's internal thread until its own group-termination timeout elapses
    /// (session.timeout.ms, 45s by default) - defeating the bound this function exists to guarantee. Finish the
    /// teardown on a detached thread instead: close() returns now, and the consumer is destroyed once librdkafka
    /// is actually ready, whenever that is.
    /// eventCallback/commitCallback are moved in too, not just consumer: librdkafka still holds their addresses
    /// and may dispatch through them (e.g. a final EVENT_LOG) while this thread's rd_kafka_destroy_flags() is still
    /// joining internal threads, which can easily outlive this KafkaSource. Both callbacks are self-contained (no
    /// reference back to this KafkaSource) precisely so they're safe to keep alive independently of it.
    NES_WARNING("Kafka source: consumer for '{}' did not close within {}, finishing teardown in the background", topic, COMMIT_TIMEOUT);
    const auto inFlight = inFlightDetachedTeardowns.fetch_add(1, std::memory_order_relaxed) + 1;
    if (inFlight > DETACHED_TEARDOWN_WARNING_THRESHOLD)
    {
        NES_WARNING(
            "Kafka source: {} detached close() teardowns are still in flight for group '{}' - the broker may be "
            "persistently unreachable",
            inFlight,
            groupId);
    }
    std::thread(
        [c = std::move(consumer), e = std::move(eventCallback), cb = std::move(commitCallback)]() mutable
        {
            (void)e;
            (void)cb;
            c.reset();
            inFlightDetachedTeardowns.fetch_sub(1, std::memory_order_relaxed);
        })
        .detach();
}

std::ostream& KafkaSource::toString(std::ostream& str) const
{
    return str << fmt::format(
               "\nKafkaSource(brokers: {}, topic: {}, groupId: {}, autoOffsetReset: {}, commit: {})\n",
               brokers,
               topic,
               groupId,
               autoOffsetReset,
               synchronousCommit ? "SYNC" : "ASYNC");
}

std::unique_ptr<RdKafka::Conf> KafkaSource::buildConsumerConf() const
{
    std::unique_ptr<RdKafka::Conf> conf{RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)};
    setOrThrow(*conf, "bootstrap.servers", brokers);
    setOrThrow(*conf, "group.id", groupId);
    setOrThrow(*conf, "enable.auto.commit", "false");
    setOrThrow(*conf, "auto.offset.reset", autoOffsetReset);
    setOrThrow(*conf, "enable.partition.eof", "false");
    /// Bounds librdkafka's prefetch queue so memory stays flat while the source waits on backpressure.
    setOrThrow(*conf, "queued.max.messages.kbytes", "16384");
    return conf;
}

void KafkaSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    const auto conf = buildConsumerConf();
    eventCallback = std::make_unique<EventCallback>();
    commitCallback = std::make_unique<CommitCallback>(failedCommits, groupId);
    setOrThrow(*conf, "event_cb", eventCallback.get());
    setOrThrow(*conf, "offset_commit_cb", commitCallback.get());

    std::string error;
    consumer.reset(RdKafka::KafkaConsumer::create(conf.get(), error));
    if (!consumer)
    {
        throw CannotOpenSource("Kafka source: cannot create consumer: {}", error);
    }

    /// Fail fast when no broker is reachable, and learn the partitions of the topic to consume.
    std::string createError;
    const std::unique_ptr<RdKafka::Topic> handle{RdKafka::Topic::create(consumer.get(), topic, nullptr, createError)};
    if (!handle)
    {
        throw CannotOpenSource("Kafka source: cannot create a handle for topic '{}': {}", topic, createError);
    }
    RdKafka::Metadata* rawMetadata = nullptr;
    const auto metadataError = consumer->metadata(false, handle.get(), &rawMetadata, static_cast<int>(OPEN_TIMEOUT.count()));
    const std::unique_ptr<RdKafka::Metadata> metadata{rawMetadata};
    if (metadataError != RdKafka::ERR_NO_ERROR)
    {
        throw CannotOpenSource("Kafka source: brokers {} unreachable: {}", brokers, RdKafka::err2str(metadataError));
    }
    const auto* topicMetadata = metadata->topics()->empty() ? nullptr : metadata->topics()->front();
    if (topicMetadata == nullptr || topicMetadata->err() != RdKafka::ERR_NO_ERROR || topicMetadata->partitions()->empty())
    {
        throw CannotOpenSource(
            "Kafka source: topic '{}' not found on {}: {}",
            topic,
            brokers,
            RdKafka::err2str(topicMetadata ? topicMetadata->err() : RdKafka::ERR_UNKNOWN_TOPIC_OR_PART));
    }
    std::vector<int32_t> partitionIds;
    for (const auto* partition : *topicMetadata->partitions())
    {
        partitionIds.push_back(partition->id());
    }

    /// OFFSET_STORED: resume from GROUP_ID's committed offset, else AUTO_OFFSET_RESET. librdkafka also applies
    /// AUTO_OFFSET_RESET when a committed offset is out of range; it only logs that (see EventCallback).
    std::vector<RdKafka::TopicPartition*> partitions;
    partitions.reserve(partitionIds.size());
    for (const auto partition : partitionIds)
    {
        partitions.push_back(RdKafka::TopicPartition::create(topic, partition, RdKafka::Topic::OFFSET_STORED));
    }
    const auto assignError = consumer->assign(partitions);
    RdKafka::TopicPartition::destroy(partitions);
    if (assignError != RdKafka::ERR_NO_ERROR)
    {
        throw CannotOpenSource("Kafka source: cannot assign partitions of '{}': {}", topic, RdKafka::err2str(assignError));
    }
    NES_INFO("Kafka source consuming '{}' partitions [{}] from {} as group '{}'", topic, fmt::join(partitionIds, ","), brokers, groupId);
}

void KafkaSource::commitCompleted(const bool wait, const std::stop_token& stopToken)
{
    if (completedInEmittedBuffer.empty())
    {
        return;
    }
    /// Kafka commits the next offset to read. The C API is used for its result queue: the C++ commitSync()
    /// blocks without a timeout, so an unreachable broker would block stop() forever.
    auto* handle = consumer->c_ptr();
    const std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(&rd_kafka_topic_partition_list_destroy)> offsets{
        rd_kafka_topic_partition_list_new(static_cast<int>(completedInEmittedBuffer.size())), rd_kafka_topic_partition_list_destroy};
    for (const auto& [partition, offset] : completedInEmittedBuffer)
    {
        rd_kafka_topic_partition_list_add(offsets.get(), topic.c_str(), partition)->offset = offset + 1;
    }
    if (!wait)
    {
        /// ASYNC: truly fire-and-forget (rd_kafka_commit_queue without a queue would wait for the broker). The result
        /// is reported to CommitCallback from a later consume(); the next commit covers these offsets.
        if (const auto error = rd_kafka_commit(handle, offsets.get(), 1); error != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            countFailedCommit(rd_kafka_err2str(error));
            return;
        }
        completedInEmittedBuffer.clear();
        return;
    }
    const std::unique_ptr<rd_kafka_queue_t, decltype(&rd_kafka_queue_destroy)> results{rd_kafka_queue_new(handle), rd_kafka_queue_destroy};
    auto error = rd_kafka_commit_queue(handle, offsets.get(), results.get(), nullptr, nullptr);
    const auto deadline = std::chrono::steady_clock::now() + COMMIT_TIMEOUT;
    bool answered = false;
    while (error == RD_KAFKA_RESP_ERR_NO_ERROR && !answered && !stopToken.stop_requested() && std::chrono::steady_clock::now() < deadline)
    {
        if (auto* event = rd_kafka_queue_poll(results.get(), static_cast<int>(POLL_TIMEOUT.count())))
        {
            if (rd_kafka_event_type(event) == RD_KAFKA_EVENT_OFFSET_COMMIT)
            {
                error = rd_kafka_event_error(event);
                answered = true;
            }
            rd_kafka_event_destroy(event);
        }
    }
    if (answered && error == RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        completedInEmittedBuffer.clear();
        return;
    }
    countFailedCommit(
        answered || error != RD_KAFKA_RESP_ERR_NO_ERROR ? fmt::format("failed: {}", rd_kafka_err2str(error)) : "timed out, retrying later");
}

void KafkaSource::countFailedCommit(std::string_view reason)
{
    /// Never fatal: the data already moved on; a later commit covers these offsets too.
    failedCommits->fetch_add(1, std::memory_order_relaxed);
    if (shouldWarn(lastCommitErrorWarning))
    {
        NES_WARNING("Kafka source: offset commit for group '{}' {}", groupId, reason);
    }
}

void KafkaSource::CommitCallback::offset_commit_cb(RdKafka::ErrorCode err, std::vector<RdKafka::TopicPartition*>&)
{
    if (err != RdKafka::ERR_NO_ERROR && err != RdKafka::ERR__NO_OFFSET)
    {
        failedCommits->fetch_add(1, std::memory_order_relaxed);
        if (shouldWarn(lastErrorWarning))
        {
            NES_WARNING("Kafka source: offset commit for group '{}' failed: {}", groupId, RdKafka::err2str(err));
        }
    }
}

void KafkaSource::writeRecord(
    std::string_view record,
    std::string_view suffix,
    TupleBuffer& tupleBuffer,
    size_t& offset,
    const int32_t partition,
    const int64_t messageOffset)
{
    const auto bufferSize = tupleBuffer.getBufferSize();
    /// Copies what fits of `piece` and returns the rest. memcpy with a null source is undefined even for size 0, and an empty
    /// view may hold a null pointer.
    const auto copyWhatFits = [&](const std::string_view piece)
    {
        const auto fits = std::min(piece.size(), bufferSize - offset);
        if (fits > 0)
        {
            std::memcpy(tupleBuffer.getAvailableMemoryArea().data() + offset, piece.data(), fits);
            offset += fits;
        }
        return piece.substr(fits);
    };
    const auto recordRest = copyWhatFits(record);
    const auto suffixRest = recordRest.empty() ? copyWhatFits(suffix) : suffix;
    if (recordRest.empty() && suffixRest.empty())
    {
        markCompleted(partition, messageOffset);
        return;
    }
    if (recordRest.empty() || suffixRest.empty())
    {
        payloadStash.stash(recordRest.empty() ? suffixRest : recordRest);
    }
    else
    {
        /// Both spill (the record was cut and a suffix follows): the stash takes one contiguous string.
        payloadStash.stash(std::string{recordRest}.append(suffixRest));
    }
    stashedMessage = {partition, messageOffset};
}

void KafkaSource::handleConsumeError(const RdKafka::Message& message)
{
    const auto code = message.err();
    if (code == RdKafka::ERR__TIMED_OUT)
    {
        return;
    }
    if (isFatalError(code))
    {
        throw CannotReadFromSource("Kafka source: {} ({})", message.errstr(), RdKafka::err2str(code));
    }
    if (shouldWarn(lastConsumeErrorWarning))
    {
        NES_WARNING("Kafka source: {} ({}), waiting for the brokers", message.errstr(), RdKafka::err2str(code));
    }
}

Source::FillTupleBufferResult KafkaSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    /// SourceThread emitted the buffer of the previous fill before calling again: its offsets are safe to commit.
    takeEmittedOffsets();
    lastFillEmitted = false;
    commitCompleted(synchronousCommit, stopToken);

    const auto bufferSize = tupleBuffer.getBufferSize();
    size_t offset = 0;
    if (!payloadStash.empty())
    {
        const auto chunk = payloadStash.consume(bufferSize);
        std::memcpy(tupleBuffer.getAvailableMemoryArea().data(), chunk.data(), chunk.size());
        offset = chunk.size();
        if (payloadStash.empty() && stashedMessage)
        {
            markCompleted(stashedMessage->first, stashedMessage->second);
            stashedMessage.reset();
        }
    }

    std::optional<std::chrono::steady_clock::time_point> flushAt;
    if (flushInterval > std::chrono::milliseconds::zero())
    {
        flushAt = std::chrono::steady_clock::now() + flushInterval;
    }
    while (offset < bufferSize && !stopToken.stop_requested())
    {
        auto pollTimeout = POLL_TIMEOUT;
        if (flushAt)
        {
            const auto now = std::chrono::steady_clock::now();
            if (now >= *flushAt)
            {
                if (offset > 0)
                {
                    break;
                }
                /// Nothing to flush yet: restart the interval, an empty buffer is never emitted.
                flushAt = now + flushInterval;
            }
            pollTimeout = std::min(pollTimeout, std::chrono::duration_cast<std::chrono::milliseconds>(*flushAt - now));
        }

        /// Never negative: -1 is librdkafka's "wait forever", which would ignore the stop token.
        pollTimeout = std::max(pollTimeout, std::chrono::milliseconds::zero());
        const std::unique_ptr<RdKafka::Message> message{consumer->consume(static_cast<int>(pollTimeout.count()))};
        if (const auto fatal = eventCallback->fatalError.load(); fatal != RdKafka::ERR_NO_ERROR)
        {
            throw CannotReadFromSource(
                "Kafka source: connection error that retrying cannot fix ({}), details in the worker log", RdKafka::err2str(fatal));
        }
        if (message->err() != RdKafka::ERR_NO_ERROR)
        {
            handleConsumeError(*message);
            continue;
        }

        /// A tombstone (null payload, e.g. on compacted topics) is a delete marker, not a record. It still counts as consumed,
        /// otherwise the source would re-read it forever.
        if (message->payload() == nullptr)
        {
            ++skippedMessages;
            markCompleted(message->partition(), message->offset());
            if (shouldWarn(lastSkipWarning))
            {
                NES_WARNING(
                    "Kafka source: skipped {} tombstone message(s) (last: partition {} offset {})",
                    skippedMessages,
                    message->partition(),
                    message->offset());
            }
            continue;
        }
        const std::string_view payload{static_cast<const char*>(message->payload()), message->len()};
        writeRecord(payload, implicitDelimiter, tupleBuffer, offset, message->partition(), message->offset());
    }

    /// On a stop request SourceThread may drop the returned buffer instead of emitting it (emit sees the stop): never
    /// commit it then, the data is read again after a restart (at-least-once).
    lastFillEmitted = !stopToken.stop_requested();
    if (offset == 0 && stopToken.stop_requested())
    {
        /// Nothing to emit on a stop request.
        return FillTupleBufferResult::eos();
    }
    return FillTupleBufferResult::withBytes(offset);
}

void KafkaSource::markCompleted(const int32_t partition, const int64_t offset)
{
    auto& completed = completedInCurrentBuffer[partition];
    completed = std::max(completed, offset);
}

void KafkaSource::takeEmittedOffsets()
{
    /// A fill that returned was emitted by SourceThread; a fill that threw (or returned with a stop request) may not
    /// have been, so its offsets stay uncommitted (at-least-once).
    if (!lastFillEmitted)
    {
        return;
    }
    for (const auto& [partition, offset] : completedInCurrentBuffer)
    {
        auto [it, inserted] = completedInEmittedBuffer.try_emplace(partition, offset);
        if (!inserted)
        {
            it->second = std::max(it->second, offset);
        }
    }
    completedInCurrentBuffer.clear();
}

void KafkaSource::close()
{
    if (!consumer)
    {
        return;
    }
    takeEmittedOffsets();
    commitCompleted(true, std::stop_token{});
    closeConsumer();
}

}
