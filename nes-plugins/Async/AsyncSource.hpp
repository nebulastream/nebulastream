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
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <AsyncGraphExecution.hpp>
#include <HandoffChannel.hpp>

namespace NES
{

struct AsyncSourceConfig
{
    /// UUID of the channel the `Handoff` sink of the same stage hands its buffers to.
    std::string channel;
    /// NES-reflected AsyncCallGraph: immutable producers, typed constants, scoped inputs and checked output handles.
    std::string calls;
    /// Rows whose calls are in flight at the same time. A function of the stage may lower it.
    size_t maxConcurrency;
    /// Only used if the source is created before the Handoff sink, which then finds the channel this source created.
    size_t channelCapacity;
    /// Further attempts of a failed call, the n-th after 100 ms * 2^n.
    size_t maxRetries;

    static std::expected<AsyncSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Consumer half of an asynchronously executed operator: takes the buffers the `Handoff` sink with the
/// same channel handed off, evaluates the async calls and fills in their result slots.
///
/// The calls run on one event loop shared by all Async sources; the source thread only waits for them.
/// They run across buffer boundaries: up to `MAX_CONCURRENCY` rows are in flight at once, whichever handed-off
/// buffers they belong to, and the buffers are emitted in the order they were handed off.
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

    struct BoundCall
    {
        AsyncFunctionBinding binding;
        /// A distinct stateful instance per admitted row slot, invoked only after its dependencies are ready.
        std::vector<std::unique_ptr<AsyncFunction>> instances;
    };

    /// A handed-off buffer whose rows are called.
    struct InFlightBuffer
    {
        TupleBuffer input;
        size_t rows;
        /// Guarded by `Window::mutex`.
        size_t nextRow = 0;
        size_t unfinishedRows;
        /// Per row, the values that go into output fields, in the order of `outputValues`. Each row's coroutine writes its own
        /// rows on the strand; the source thread reads them once `done` is set.
        std::vector<AsyncValues> results;
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
        size_t runningRows = 0;
        std::exception_ptr failure;
        /// Set by the channel when a buffer arrives or it closes; reset by the source thread before it looks.
        bool channelChanged = false;
    };

    RuntimeValue readInput(const InputFieldReference& field, const TupleBuffer& input, size_t row) const;
    /// Service retries belong to the shared producer invocation, never to its subscribers.
    boost::asio::awaitable<AsyncValues> callWithRetries(AsyncCall::Ptr node, size_t slot, AsyncValues arguments) const;
    /// Runs the calls of one row with the function instances of `slot`.
    boost::asio::awaitable<void> processRow(size_t slot, std::shared_ptr<InFlightBuffer> buffer, size_t row);

    /// Source thread.
    void admit(TupleBuffer input);
    void writeOutput(const InFlightBuffer& buffer, TupleBuffer& output) const;
    /// Cancels every row in flight and returns once none is running and no handler of this source is left on the strand.
    void cancelAndDrain();

    /// Strand only: starts rows while a slot is free.
    void startRows();
    void finishRow(const std::shared_ptr<InFlightBuffer>& buffer, size_t slot, const std::exception_ptr& error);

    std::string channelId;
    /// The configured bound, lowered to what the stage's functions allow.
    size_t maxConcurrency;
    size_t maxRetries;
    size_t inputRowSize = 0;
    size_t outputRowSize = 0;
    Schema<QualifiedUnboundField, Ordered> inputSchema;
    Schema<QualifiedUnboundField, Ordered> outputSchema;
    std::shared_ptr<TupleBufferRef> inputBufferRef;
    std::shared_ptr<TupleBufferRef> outputBufferRef;
    std::vector<CopiedField> copiedFields;
    std::shared_ptr<const AsyncCallGraph> graph;
    std::unordered_map<AsyncCall::Ptr, BoundCall> calls;
    std::vector<AsyncCallOutput> outputValues;

    /// Taken when the source is created, so that a source that fails afterwards still releases the channel.
    HandoffChannel::Lease channel;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    /// All coroutines of this source run here, so its state is never touched by two loop threads at once.
    boost::asio::strand<boost::asio::io_context::executor_type> strand;

    /// Source thread only: the channel is closed and drained.
    bool inputExhausted = false;
    Window window;
    /// Strand only. A row in flight holds one slot: its function instances and its cancellation signal.
    std::vector<size_t> freeSlots;
    std::vector<boost::asio::cancellation_signal> cancelSignals;
    std::vector<std::shared_ptr<AsyncGraphExecution>> executions;
    bool stopping = false;
};

}
