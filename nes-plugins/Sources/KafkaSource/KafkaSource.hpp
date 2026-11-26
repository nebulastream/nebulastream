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

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/PayloadStash.hpp>
#include <Sources/Source.hpp>
#include <librdkafka/rdkafkacpp.h>
#include <ErrorHandling.hpp>

namespace NES
{

struct KafkaSourceConfig
{
    std::string brokers;
    std::string topic;
    std::string groupId;
    std::string autoOffsetReset;
    bool synchronousCommit{};
    int64_t flushIntervalMs{};
    std::string implicitMessageDelimiter;

    static std::expected<KafkaSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Consumes all partitions of one topic with a static assignment. Each message becomes one record, terminated by
/// IMPLICIT_MESSAGE_DELIMITER. The offsets of the messages that a buffer fully contains are committed under GROUP_ID when the next fill
/// starts, i.e. after the buffer was handed to the pipeline: at-least-once up to the pipeline boundary.
class KafkaSource : public Source
{
public:
    static constexpr std::string_view NAME = "Kafka";

    explicit KafkaSource(const KafkaSourceConfig& config);
    ~KafkaSource() override;

    KafkaSource(const KafkaSource&) = delete;
    KafkaSource& operator=(const KafkaSource&) = delete;
    KafkaSource(KafkaSource&&) = delete;
    KafkaSource& operator=(KafkaSource&&) = delete;

    void open(std::shared_ptr<AbstractBufferProvider>) override;
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void close() override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    [[nodiscard]] uint64_t getSkippedMessages() const { return skippedMessages; }

    [[nodiscard]] uint64_t getFailedCommits() const { return failedCommits->load(); }

    /// The librdkafka configuration open() creates the consumer with. Needs no broker and has no side effect.
    [[nodiscard]] std::unique_ptr<RdKafka::Conf> buildConsumerConf() const;

    /// How long the startup metadata probe waits for a broker before open() fails.
    static constexpr std::chrono::milliseconds OPEN_TIMEOUT{5000};
    /// Upper bound for one consume() call, which bounds stop latency.
    static constexpr std::chrono::milliseconds POLL_TIMEOUT{100};
    /// Longest a SYNC commit (and the final commit in close()) waits for the broker. A commit that times out stays
    /// pending and is retried with the next one; it never blocks a stop or an outage indefinitely.
    static constexpr std::chrono::milliseconds COMMIT_TIMEOUT{5000};
    /// Least time between two re-assignments after the group coordinator was not ready to serve the committed offsets.
    static constexpr std::chrono::milliseconds REASSIGN_INTERVAL{1000};

    /// ERR__AUTHENTICATION, ERR__SSL and ERR__FATAL: a rejected login or certificate does not heal by retrying.
    static bool isFatalConnectionError(RdKafka::ErrorCode code);
    /// Errors after which the source cannot make progress: the connection errors above plus unknown topic or partition, authorization
    /// failures and an invalid argument. Everything else is transient.
    static bool isFatalError(RdKafka::ErrorCode code);

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    class EventCallback : public RdKafka::EventCb
    {
    public:
        void event_cb(RdKafka::Event& event) override;
        std::chrono::steady_clock::time_point lastWarning;
        /// First connection error that retrying cannot fix (isFatalConnectionError); set from whichever thread serves events.
        std::atomic<RdKafka::ErrorCode> fatalError{RdKafka::ERR_NO_ERROR};
    };

    /// Results of ASYNC commits arrive here, served by consume(). Self-contained (no reference back to KafkaSource):
    /// closeConsumer() can outlive the KafkaSource that created it, so nothing it is registered with may depend on KafkaSource.
    class CommitCallback : public RdKafka::OffsetCommitCb
    {
    public:
        CommitCallback(std::shared_ptr<std::atomic<uint64_t>> failedCommits, std::string groupId)
            : failedCommits(std::move(failedCommits)), groupId(std::move(groupId))
        {
        }

        void offset_commit_cb(RdKafka::ErrorCode err, std::vector<RdKafka::TopicPartition*>& offsets) override;

    private:
        std::shared_ptr<std::atomic<uint64_t>> failedCommits;
        std::string groupId;
        std::chrono::steady_clock::time_point lastErrorWarning;
    };

    /// Writes `record` followed by `suffix` into the buffer; what does not fit is stashed for the next fill.
    void writeRecord(
        std::string_view record,
        std::string_view suffix,
        TupleBuffer& tupleBuffer,
        size_t& offset,
        int32_t partition,
        int64_t messageOffset);
    void commitCompleted(bool wait, const std::stop_token& stopToken);
    void handleConsumeError(const RdKafka::Message& message);
    void countFailedCommit(std::string_view reason);
    void closeConsumer();
    /// (Re-)assigns every partition of the topic, starting at the committed offset of GROUP_ID (else AUTO_OFFSET_RESET).
    [[nodiscard]] RdKafka::ErrorCode assignPartitions();
    /// Assigns again once REASSIGN_INTERVAL passed since the last assignment, if handleConsumeError asked for it.
    void retryAssignmentIfDue();
    /// Moves the offsets of the last filled buffer to the committable set if SourceThread emitted it.
    void takeEmittedOffsets();
    /// Records that the current buffer fully contains `offset` of `partition`.
    void markCompleted(int32_t partition, int64_t offset);

    std::string brokers;
    std::string topic;
    std::string groupId;
    std::string autoOffsetReset;
    bool synchronousCommit;
    std::string implicitDelimiter;
    std::chrono::milliseconds flushInterval;

    /// Allocated in open() (not embedded members): registered with librdkafka by address, and closeConsumer() may
    /// need to keep them alive past this KafkaSource's own destruction.
    std::unique_ptr<EventCallback> eventCallback;
    std::unique_ptr<CommitCallback> commitCallback;
    std::unique_ptr<RdKafka::KafkaConsumer> consumer;
    PayloadStash payloadStash;
    std::vector<int32_t> partitionIds;
    std::chrono::steady_clock::time_point lastAssignment;
    bool assignmentRetryPending = false;

    /// The message whose record did not fit and is (partly) in the stash; complete once the stash drains.
    std::optional<std::pair<int32_t, int64_t>> stashedMessage;
    /// Highest offset per partition fully contained in the buffer being filled / in the last emitted buffer.
    std::map<int32_t, int64_t> completedInCurrentBuffer;
    std::map<int32_t, int64_t> completedInEmittedBuffer;
    /// Whether SourceThread emitted the last filled buffer: false if the fill threw or returned under a stop request.
    bool lastFillEmitted = false;

    uint64_t skippedMessages = 0;
    /// Shared with CommitCallback so async commit results can still be counted even if that callback outlives this KafkaSource.
    std::shared_ptr<std::atomic<uint64_t>> failedCommits = std::make_shared<std::atomic<uint64_t>>(0);
    std::chrono::steady_clock::time_point lastSkipWarning;
    std::chrono::steady_clock::time_point lastCommitErrorWarning;
    std::chrono::steady_clock::time_point lastConsumeErrorWarning;
};

}
