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

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <cstdint>
#include <utility>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <Async/HandoffChannel.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Thread.hpp>

namespace NES
{

/// The consumer half of an asynchronously executed operator, and the reason the whole
/// construction exists.
///
/// A source owns its thread and is the one place in NebulaStream where a long blocking
/// call is architecturally allowed. This source therefore does the slow work itself: it
/// takes buffers from the handoff channel, runs the operator's `AsyncOperatorExecutor` on
/// its own threads, and only hands finished records back to the engine. The engine's
/// worker pool never waits for the external service.
///
/// Sequence numbers, origin id and watermarks are taken from the input buffer verbatim,
/// which is why `addsMetadata()` is true. Renumbering here would either crash a downstream
/// watermark processor with an unknown origin or stall it forever on a gap.
///
/// One input buffer becomes one output buffer. When the output records do not fit — the
/// operator appends fields, so they are larger — the remainder is emitted as further
/// chunks of the same sequence number, which is precisely what chunk numbers are for.
class AsyncSource final : public Source
{
public:
    static const std::string& name()
    {
        static const std::string Instance = "Async";
        return Instance;
    }

    explicit AsyncSource(const SourceDescriptor& sourceDescriptor);
    ~AsyncSource() override;

    AsyncSource(const AsyncSource&) = delete;
    AsyncSource& operator=(const AsyncSource&) = delete;
    AsyncSource(AsyncSource&&) = delete;
    AsyncSource& operator=(AsyncSource&&) = delete;

    void open(std::shared_ptr<AbstractBufferProvider> provider) override;
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void close() override;

    /// The metadata of the incoming buffers is carried over unchanged.
    [[nodiscard]] bool addsMetadata() const override { return true; }

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// One processed input buffer: the buffer itself, kept alive because emitting still reads its
    /// variable-sized contents, and one result per record.
    struct Processed
    {
        TupleBuffer input;
        std::vector<AsyncRecordResult> results;
        /// Records already written out; non-zero once a buffer had to be split into chunks.
        uint64_t emittedRecords = 0;
    };

    /// An input buffer whose batches are still being worked on. `results` is sized to the record
    /// count up front, so each batch writes its own disjoint range without further locking.
    struct Pending
    {
        TupleBuffer input;
        std::vector<AsyncRecordResult> results;
        size_t outstandingBatches = 0;
    };

    /// One call to the executor: the records [begin, end) of the buffer under `ticket`. This, and
    /// not the input buffer, is the unit of work — `maxConcurrency` therefore means calls in
    /// flight, which is what it says.
    struct BatchWork
    {
        uint64_t ticket;
        uint64_t begin;
        uint64_t end;
    };

    /// Per (sequence number, origin id): how chunk numbers are handed out while one incoming
    /// buffer is spread over several outgoing ones.
    struct SequenceState
    {
        uint64_t nextChunkNumber = ChunkNumber::INITIAL;
        ChunkNumber lastIncomingChunk = INVALID<ChunkNumber>;
        uint64_t seenIncomingChunks = 0;
    };

    using SequenceKey = std::pair<uint64_t, uint64_t>;

    /// Takes buffers from the channel in order and turns each into batch work. One thread, so the
    /// ticket order is the order buffers arrived in, which is what `preserveOrder` restores.
    void intakeLoop(const std::stop_token& stopToken);
    void processingLoop(const std::stop_token& stopToken);
    std::optional<BatchWork> takeNextBatch(std::unique_lock<std::mutex>& lock, const std::stop_token& stopToken);
    std::optional<Processed> takeNextCompleted(const std::stop_token& stopToken);
    void assignChunkNumber(bool isEndOfIncomingChunk, const TupleBuffer& input, TupleBuffer& output);

    std::string channelId;
    size_t channelCapacity;
    std::string executorType;
    std::unordered_map<std::string, std::string> executorConfig;
    size_t batchSize;
    size_t maxConcurrency;
    bool preserveOrder;

    AsyncRecordLayout inputLayout;
    AsyncRecordLayout outputLayout;

    std::shared_ptr<HandoffChannel> channel;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::unique_ptr<AsyncOperatorExecutor> executor;
    std::vector<Thread> workers;
    /// Separate from the workers so that waiting for a buffer never occupies one of them.
    std::optional<Thread> intake;
    std::stop_source workerStop;

    std::mutex mutex;
    /// Raised when a batch becomes available, and when intake ends so idle workers can leave.
    std::condition_variable_any workArrived;
    /// Raised when a buffer is fully processed, when intake ends, and when a slot in `pending`
    /// frees up — the last one is what lets the intake thread continue.
    std::condition_variable_any completedArrived;

    /// Input buffers are numbered as they are taken from the channel, so that results can be
    /// handed on in input order when the operator asks for it.
    uint64_t nextTicket = 0;
    uint64_t nextToEmit = 0;
    std::deque<BatchWork> work;
    std::map<uint64_t, Pending> pending;
    std::map<uint64_t, Processed> completed;
    /// How far intake may read ahead: primarily in queued batches, which is what keeps the workers
    /// fed, and secondarily in buffers, which is what bounds the share of the buffer pool parked
    /// here. Both are set from `maxConcurrency` when the source opens.
    size_t queuedWorkHighWatermark = 1;
    size_t maxPendingBuffers = 1;
    bool intakeDone = false;
    bool inputExhausted = false;
    std::optional<Processed> partiallyEmitted;

    /// Only touched from fillTupleBuffer, i.e. from the source thread alone.
    std::map<SequenceKey, SequenceState> sequenceStates;
};

struct ConfigParametersAsyncSource
{
    static inline const DescriptorConfig::ConfigParameter<std::string> CHANNEL{
        "CHANNEL",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHANNEL, config); }};

    static inline const DescriptorConfig::ConfigParameter<size_t> CHANNEL_CAPACITY{
        "CHANNEL_CAPACITY",
        size_t{64},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHANNEL_CAPACITY, config); }};

    /// Key into AsyncExecutorRegistry — which operator this source actually runs.
    static inline const DescriptorConfig::ConfigParameter<std::string> EXECUTOR_TYPE{
        "EXECUTOR_TYPE",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(EXECUTOR_TYPE, config); }};

    /// The operator's own settings, JSON-encoded because a descriptor only takes declared keys.
    static inline const DescriptorConfig::ConfigParameter<std::string> EXECUTOR_CONFIG{
        "EXECUTOR_CONFIG",
        std::string{},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(EXECUTOR_CONFIG, config); }};

    /// Schema of the records arriving through the channel, JSON-encoded. The descriptor's own
    /// schema describes what this source produces, which is not the same thing.
    static inline const DescriptorConfig::ConfigParameter<std::string> INPUT_SCHEMA{
        "INPUT_SCHEMA",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INPUT_SCHEMA, config); }};

    /// Records per call to the executor.
    static inline const DescriptorConfig::ConfigParameter<size_t> BATCH_SIZE{
        "BATCH_SIZE",
        size_t{1},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BATCH_SIZE, config); }};

    /// How many input buffers are processed at the same time.
    static inline const DescriptorConfig::ConfigParameter<size_t> MAX_CONCURRENCY{
        "MAX_CONCURRENCY",
        size_t{1},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_CONCURRENCY, config); }};

    /// Hand results on in input order. Off means results leave as soon as they are done.
    static inline const DescriptorConfig::ConfigParameter<bool> PRESERVE_ORDER{
        "PRESERVE_ORDER",
        true,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(PRESERVE_ORDER, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, CHANNEL, CHANNEL_CAPACITY, EXECUTOR_TYPE, EXECUTOR_CONFIG, INPUT_SCHEMA, BATCH_SIZE,
            MAX_CONCURRENCY, PRESERVE_ORDER);
};

}
