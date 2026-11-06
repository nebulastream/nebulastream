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

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <random>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Sources/AsyncResourceLimiter.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <AsyncScheduling.hpp>
#include <HandoffChannel.hpp>

namespace NES
{

struct AsyncSourceConfig
{
    /// UUID of the channel the `Handoff` sink of the same stage hands its buffers to.
    std::string channel;
    /// JSON list of `AsyncCall`: the fields each call's values go to, the async function's type and configuration, and
    /// per argument the handed-off field it reads ({"field": ...}), a constant ({"constant": ...}) or a value of an
    /// earlier call ({"call": ..., "output": ...}).
    std::string calls;
    /// Comma-separated field order of the handed-off rows. They have the same fields as this source's
    /// output, possibly in a different order.
    std::string inputFields;
    /// Rows whose calls are in flight at the same time. A function of the stage may lower it.
    size_t maxConcurrency;
    /// Only used if the source is created before the Handoff sink, which then finds the channel this source created.
    size_t channelCapacity;
    /// Further attempts of a failed call, the n-th after 100 ms * 2^n.
    size_t maxRetries;
    /// Further attempts after transient failures (`AsyncTransientFailure`), with backoff and jitter, and the seconds from
    /// a call's first attempt within which they have to be made.
    size_t transientRetries = RetryPolicy{}.maxTransientRetries;
    size_t transientRetrySeconds = std::chrono::duration_cast<std::chrono::seconds>(RetryPolicy{}.transientTimeLimit).count();
    /// Rows the source holds at most, waiting, running or done but not yet emitted, before it stops taking input. 0
    /// means `DEFAULT_READ_AHEAD_FACTOR` rows per row the slots take at once.
    size_t readAheadRows = 0;

    /// Rows held per row the slots take at once, by default: enough for a keyed stage over input sorted by key to keep
    /// every slot busy with keys of up to that many rows.
    static constexpr size_t DEFAULT_READ_AHEAD_FACTOR = 64;

    static std::expected<AsyncSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Consumer half of an asynchronously executed operator: takes the buffers the `Handoff` sink with the
/// same channel handed off, evaluates the async calls and fills in their result slots.
///
/// The calls run on one event loop shared by all Async sources; the source thread only waits for them.
/// They run across buffer boundaries: up to `MAX_CONCURRENCY` requests are in flight at once, whichever handed-off
/// buffers their rows belong to.
///
/// Order: the Handoff sink runs on several worker threads, so buffers arrive out of order. The source takes them in
/// stream order again, per origin by sequence and chunk number (`StreamReorderer`), starts their rows in that order and
/// emits the buffers in that order.
///
/// Batches: a slot runs up to `maxBatchSize` rows of the stage's functions in one request, whatever rows are ready when
/// the slot is free. Rows queue up while every slot is busy, so batches fill under load and stay small when the
/// service keeps up; the end of the stream needs no flush.
///
/// Keys: rows that share the value of a function's `orderingKey` argument run one at a time and in stream order; rows
/// waiting for their key hold no slot (`KeyedRowQueue`).
///
/// The lifting rule reserves a slot per call in the handed-off rows, so input and output rows have
/// the same fields and one input buffer becomes exactly one output buffer. Everything that is not a
/// call result is copied byte for byte, VARSIZED included: the input's child buffers are stored on
/// the output in the same order, so the child indices in the copied fields stay valid.
class AsyncSource final : public Source
{
public:
    static constexpr std::string_view NAME = "Async";

    AsyncSource(const AsyncSourceConfig& config, const SourceDescriptor& sourceDescriptor);

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    /// Origin id, sequence and chunk number and watermark are taken over from the handed-off buffers.
    [[nodiscard]] bool addsMetadata() const override { return true; }

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// A field copied unchanged from the input row to the output row.
    struct CopiedField
    {
        size_t inputOffset;
        size_t outputOffset;
        size_t size;
    };

    /// The input offset of a VARSIZED field, a value of an earlier call, or else a constant.
    struct BoundArgument
    {
        std::optional<size_t> inputOffset;
        std::optional<size_t> call;
        size_t output = 0;
        std::string constant;
    };

    struct BoundCall
    {
        std::string function;
        std::string configuration;
        std::vector<BoundArgument> arguments;
        /// Values each row's result has to hold: one more than the highest that an output or a later call reads.
        size_t requiredValues = 1;
        /// 0 for calls that read no other call; otherwise one more than the highest level they read.
        size_t level = 0;
        /// The argument whose value is the call's ordering key, if the function names one.
        std::optional<size_t> orderingKey;
        /// Rows one request of this call takes.
        size_t maxBatchSize = 1;
        /// One per slot, so an instance serves one call at a time.
        std::vector<std::unique_ptr<AsyncFunction>> instances;
        /// The limit the function shares with other calls to its resource, if it names one.
        std::shared_ptr<AsyncResourceLimiter> limiter;
    };

    /// A field of the output that holds one value of a call.
    struct OutputValue
    {
        size_t call;
        size_t index;
        size_t outputOffset;
    };

    /// A handed-off buffer whose rows are called.
    struct InFlightBuffer
    {
        TupleBuffer input;
        size_t rows;
        /// Guarded by `Window::mutex`.
        size_t unfinishedRows;
        /// Per row, the values that go into output fields, in the order of `outputValues`. Each row's coroutine writes its own
        /// rows on the strand; the source thread reads them once `done` is set.
        std::vector<std::vector<std::string>> results;
        /// Guarded by `Window::mutex`.
        bool done = false;
    };

    /// One row of a buffer in the window.
    struct RowRef
    {
        std::shared_ptr<InFlightBuffer> buffer;
        size_t row;
    };

    /// What the source thread and the strand share; everything here is guarded by `mutex`.
    struct Window
    {
        std::mutex mutex;
        std::condition_variable_any changed;
        /// In stream order, which is the order they are emitted in.
        std::deque<std::shared_ptr<InFlightBuffer>> buffers;
        /// Rows of the buffers in the window.
        size_t heldRows = 0;
        /// The rows that wait to start, with their ordering keys.
        KeyedRowQueue<RowRef> queue;
        /// What `queue.startableRows()` was after its last change.
        size_t startableRows = 0;
        size_t runningRows = 0;
        std::exception_ptr failure;
        /// Set by the channel when a buffer arrives or it closes; reset by the source thread before it looks.
        bool channelChanged = false;
    };

    /// `results` holds the values of this row's earlier calls, one result per call.
    std::vector<std::string>
    readArguments(const BoundCall& call, const TupleBuffer& input, size_t row, const std::vector<AsyncCallResult>& results) const;
    /// The ordering keys of one handed-off row, one per call that names one.
    std::vector<std::string> readKeys(const TupleBuffer& input, size_t row) const;
    /// Retries a failed call up to `maxRetries` times, unless it failed because the query stops, and checks that the
    /// function produced the values the stage reads.
    boost::asio::awaitable<AsyncCallResult> callWithRetries(size_t callIndex, size_t slot, std::vector<std::string> arguments);
    /// One request for several rows, retried as a whole up to `maxRetries` times. Rows it leaves without a result, or all
    /// of them once the retries are used up, are called one by one (`callWithRetries`).
    boost::asio::awaitable<std::vector<AsyncCallResult>>
    callBatchWithRetries(size_t callIndex, size_t slot, std::vector<std::vector<std::string>> rows);
    /// One call for the rows of a batch, in requests of at most the call's `maxBatchSize` rows.
    boost::asio::awaitable<std::vector<AsyncCallResult>> callRows(size_t callIndex, size_t slot, std::vector<std::vector<std::string>> rows);
    /// Strand only: tells the function, counts the retry and waits `delay`.
    boost::asio::awaitable<void>
    backOff(const BoundCall& call, AsyncFunction& function, std::chrono::milliseconds delay, bool transient, size_t rows);
    /// Runs the calls of a batch of rows with the function instances of `slot`.
    boost::asio::awaitable<void> processBatch(size_t slot, std::vector<RowRef> rows);

    /// Source thread: hands a buffer that is next in its origin to the window.
    void admit(TupleBuffer input);
    void writeOutput(const InFlightBuffer& buffer, TupleBuffer& output) const;
    /// Cancels every row in flight and returns once none is running and no handler of this source is left on the strand.
    void cancelAndDrain();

    /// Strand only: starts rows while a slot is free.
    void startRows();
    void finishBatch(const std::vector<RowRef>& rows, const std::vector<std::string>& keys, size_t slot, const std::exception_ptr& error);

    std::string channelId;
    /// The configured bound, lowered to what the stage's functions allow.
    size_t maxConcurrency;
    RetryPolicy retryPolicy;
    size_t inputRowSize = 0;
    size_t outputRowSize = 0;
    std::vector<CopiedField> copiedFields;
    /// Ordered so that every call comes after the calls it reads.
    std::vector<BoundCall> calls;
    std::vector<OutputValue> outputValues;
    size_t levels = 0;
    /// The largest `maxBatchSize` of the stage's calls: the rows a slot takes at once.
    size_t batchSize = 1;
    /// Set from `AsyncSourceConfig::readAheadRows` once the batch size is known.
    size_t readAheadRows = 0;
    /// Whether any call names an ordering key.
    bool keyed = false;

    /// Taken when the source is created, so that a source that fails afterwards still releases the channel.
    HandoffChannel::Lease channel;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    /// All coroutines of this source run here, so its state is never touched by two loop threads at once.
    boost::asio::strand<boost::asio::io_context::executor_type> strand;

    /// Source thread only: the channel is closed and drained.
    bool inputExhausted = false;
    /// Source thread only: buffers that arrived before an earlier one of their origin.
    StreamReorderer<TupleBuffer> reorderer;
    size_t mostReordered = 0;
    /// Source thread only, for the debug line at close: what the window held, and how long the read-ahead bound kept the
    /// source from taking input although fewer rows were ready than the slots could take.
    size_t admittedBuffers = 0;
    size_t admittedRows = 0;
    size_t mostBuffersHeld = 0;
    std::chrono::steady_clock::duration boundedWhileShort{0};
    std::optional<std::chrono::steady_clock::time_point> boundedSince;
    Window window;
    /// Strand only. A batch in flight holds one slot: its function instances and its cancellation signal.
    std::vector<size_t> freeSlots;
    std::vector<boost::asio::cancellation_signal> cancelSignals;
    bool stopping = false;
    /// Strand only: picks the jitter of the backoff after transient failures.
    std::mt19937_64 jitter{std::random_device{}()};
    /// Strand only; read at close, once nothing runs any more.
    size_t transientRetriesMade = 0;
    size_t otherRetriesMade = 0;
    std::chrono::milliseconds backedOff{0};
};

}
