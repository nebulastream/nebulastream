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
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/UUID.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <HandoffChannel.hpp>

namespace NES
{

/// Consumer half of an asynchronously executed operator: takes the buffers the `Handoff` sink with the
/// same channel handed off, evaluates the async calls and fills in their result slots.
///
/// The calls run on one event loop shared by all Async sources; the source thread only waits for them.
/// They run across buffer boundaries: up to `MAX_CONCURRENCY` batches of rows are in flight at once, whichever
/// handed-off buffers they belong to, and the buffers are emitted in the order they were handed off. A batch holds as
/// many consecutive rows of one buffer as every function of the stage accepts in one call (`maxBatchSize`), so it is a
/// single row unless all of them batch.
///
/// The lifting rule reserves a slot per call in the handed-off rows, so input and output rows have
/// the same fields and one input buffer becomes exactly one output buffer. Everything that is not a
/// call result is copied byte for byte, VARSIZED included: the input's child buffers are stored on
/// the output in the same order, so the child indices in the copied fields stay valid.
class AsyncSource final : public Source
{
public:
    static constexpr std::string_view NAME = "Async";

    explicit AsyncSource(const SourceDescriptor& sourceDescriptor);

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    /// Origin id, sequence and chunk number and watermark are taken over from the handed-off buffers.
    [[nodiscard]] bool addsMetadata() const override { return true; }

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

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
        /// One per slot, so an instance serves one call at a time.
        std::vector<std::unique_ptr<AsyncFunction>> instances;
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
        size_t nextRow = 0;
        size_t unfinishedRows;
        /// Per row, the values that go into output fields, in the order of `outputValues`. Each batch writes its own
        /// rows on the strand; the source thread reads them once `done` is set.
        std::vector<std::vector<std::string>> results;
        /// Guarded by `Window::mutex`.
        bool done = false;
    };

    /// What the source thread and the strand share; everything here is guarded by `mutex`.
    struct Window
    {
        std::mutex mutex;
        std::condition_variable_any changed;
        /// In the order they were handed off, which is the order they are emitted in.
        std::deque<std::shared_ptr<InFlightBuffer>> buffers;
        size_t unstartedRows = 0;
        size_t runningBatches = 0;
        std::exception_ptr failure;
        /// Set by the channel when a buffer arrives or it closes; reset by the source thread before it looks.
        bool channelChanged = false;
    };

    /// `results` holds the values of this row's earlier calls, one result per call.
    std::vector<std::string>
    readArguments(const BoundCall& call, const TupleBuffer& input, size_t row, const std::vector<AsyncCallResult>& results) const;
    /// Retries a failed call up to `maxRetries` times, unless it failed because the query stops, and checks that the
    /// function answered every row with the values the stage reads.
    boost::asio::awaitable<std::vector<AsyncCallResult>>
    callWithRetries(size_t callIndex, size_t slot, std::vector<std::vector<std::string>> rows) const;
    /// Runs the calls of the rows [begin, end) of a buffer with the function instances of `slot`.
    boost::asio::awaitable<void> processBatch(size_t slot, std::shared_ptr<InFlightBuffer> buffer, size_t begin, size_t end);

    /// Source thread.
    void admit(TupleBuffer input);
    void writeOutput(const InFlightBuffer& buffer, TupleBuffer& output) const;
    /// Cancels every row in flight and returns once none is running and no handler of this source is left on the strand.
    void cancelAndDrain();

    /// Strand only: starts batches while a slot is free.
    void startBatches();
    void finishBatch(const std::shared_ptr<InFlightBuffer>& buffer, size_t slot, size_t rows, const std::exception_ptr& error);

    std::string channelId;
    size_t channelCapacity;
    /// The configured bound, lowered to what the stage's functions allow.
    size_t maxConcurrency;
    size_t maxRetries;
    /// Rows per call: the smallest `maxBatchSize` of the stage's functions.
    size_t batchSize = 1;
    size_t inputRowSize = 0;
    size_t outputRowSize = 0;
    std::vector<CopiedField> copiedFields;
    /// Ordered so that every call comes after the calls it reads.
    std::vector<BoundCall> calls;
    std::vector<OutputValue> outputValues;
    size_t levels = 0;

    std::shared_ptr<HandoffChannel> channel;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    /// All coroutines of this source run here, so its state is never touched by two loop threads at once.
    boost::asio::strand<boost::asio::io_context::executor_type> strand;

    /// Source thread only: the channel is closed and drained.
    bool inputExhausted = false;
    Window window;
    /// Strand only. A batch in flight holds one slot: its function instances and its cancellation signal.
    std::vector<size_t> freeSlots;
    std::vector<boost::asio::cancellation_signal> cancelSignals;
    bool stopping = false;
};

/// NOLINTBEGIN(cert-err58-cpp)
struct ConfigParametersAsync
{
    static inline const DescriptorConfig::ConfigParameter<std::string> CHANNEL{
        "CHANNEL",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            auto value = DescriptorConfig::tryGet(CHANNEL, config);
            if (value && !stringToUUID(*value))
            {
                return std::nullopt;
            }
            return value;
        }};

    /// JSON list of `AsyncCall`: the fields each call's values go to, the async function's type and configuration, and
    /// per argument the handed-off field it reads ({"field": ...}), a constant ({"constant": ...}) or a value of an
    /// earlier call ({"call": ..., "output": ...}).
    static inline const DescriptorConfig::ConfigParameter<std::string> CALLS{
        "CALLS",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CALLS, config); }};

    /// Comma-separated field order of the handed-off rows. They have the same fields as this source's
    /// output, possibly in a different order.
    static inline const DescriptorConfig::ConfigParameter<std::string> INPUT_FIELDS{
        "INPUT_FIELDS",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INPUT_FIELDS, config); }};

    /// Calls in flight at the same time, each for one batch of rows. A function of the stage may lower it.
    static inline const DescriptorConfig::ConfigParameter<size_t> MAX_CONCURRENCY{
        "MAX_CONCURRENCY",
        size_t{16},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_CONCURRENCY, config); }};

    /// Only used if the source opens before the Handoff sink, which then finds the channel this source created.
    static inline const DescriptorConfig::ConfigParameter<size_t> CHANNEL_CAPACITY{
        "CHANNEL_CAPACITY",
        HandoffChannel::DEFAULT_CAPACITY,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHANNEL_CAPACITY, config); }};

    /// Further attempts of a failed call, the n-th after 100 ms * 2^n.
    static inline const DescriptorConfig::ConfigParameter<size_t> MAX_RETRIES{
        "MAX_RETRIES",
        size_t{0},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_RETRIES, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, CHANNEL, CALLS, INPUT_FIELDS, MAX_CONCURRENCY, CHANNEL_CAPACITY, MAX_RETRIES);
};

/// NOLINTEND(cert-err58-cpp)

}
