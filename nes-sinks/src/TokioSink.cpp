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

#include <Sinks/TokioSink.hpp>

#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <ostream>
#include <utility>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

// CXX runtime for rust::Vec, rust::Slice, rust::String
#include <rust/cxx.h>
// CXX-generated header for ffi_sink module
#include <nes-sink-runtime-bindings/lib.h>
#include <IORuntime.hpp>
// SinkBindings provides ErrorContext, on_sink_error_callback
#include "Sinks/TokioSink.hpp"


#include <nes-io-runtime-bindings/lib.h>
#include <rfl/json/Parser.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include "Sinks/PrintSink.hpp"
#include "Sinks/SinkDescriptor.hpp"
#include "SinksParsing/SchemaFormatter.hpp"

#include "QueryId.hpp"
#include "SinkRegistry.hpp"

namespace NES
{
class QueryId;


/// Retry interval for repeatTask when channel is full
static constexpr auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);

/// Backpressure handler for TokioSink.
class SinkBackpressureHandler
{
    struct State
    {
        bool hasBackpressure = false;
        std::deque<TupleBuffer> buffered;
        SequenceNumber pendingSequenceNumber = INVALID<SequenceNumber>;
        ChunkNumber pendingChunkNumber = INVALID<ChunkNumber>;
    };

    folly::Synchronized<State> stateLock;
    size_t upperThreshold;
    size_t lowerThreshold;

public:
    /// @param upperThreshold Number of buffered items at which backpressure is applied.
    /// @param lowerThreshold Number of buffered items at which backpressure is released.
    explicit SinkBackpressureHandler(size_t upperThreshold = 1, size_t lowerThreshold = 0);

    /// Called when sink_send_buffer returns Full.
    /// Buffers the TupleBuffer internally. Applies backpressure if threshold exceeded.
    /// Returns a buffer to retry via repeatTask, or nullopt if one is already pending.
    std::optional<TupleBuffer> onFull(TupleBuffer buffer, BackpressureController& bpc);

    /// Called when sink_send_buffer returns Success.
    /// Clears pending state, releases backpressure if below threshold.
    /// Returns next buffered item to send, or nullopt if backlog is empty.
    std::optional<TupleBuffer> onSuccess(BackpressureController& bpc);

    /// Returns true if internal backlog is empty.
    bool empty() const;

    /// Pop front item from backlog for drain during stop().
    /// Returns nullopt if empty.
    std::optional<TupleBuffer> popFront();
};

SinkBackpressureHandler::SinkBackpressureHandler(size_t upperThreshold, size_t lowerThreshold)
    : upperThreshold(upperThreshold), lowerThreshold(lowerThreshold)
{
    PRECONDITION(
        lowerThreshold < upperThreshold, "lowerThreshold ({}) must be less than upperThreshold ({})", lowerThreshold, upperThreshold);
}

std::optional<TupleBuffer> SinkBackpressureHandler::onFull(TupleBuffer buffer, BackpressureController& bpc)
{
    auto rstate = stateLock.ulock();
    if (buffer.getSequenceNumber() == rstate->pendingSequenceNumber && buffer.getChunkNumber() == rstate->pendingChunkNumber)
    {
        return buffer;
    }
    const auto wstate = rstate.moveFromUpgradeToWrite();
    wstate->buffered.emplace_back(std::move(buffer));
    if (!wstate->hasBackpressure && wstate->buffered.size() >= upperThreshold)
    {
        bpc.applyPressure();
        NES_DEBUG("TokioSink backpressure acquired: {} buffered (upper: {})", wstate->buffered.size(), upperThreshold);
        wstate->hasBackpressure = true;
    }
    if (wstate->pendingSequenceNumber == INVALID<SequenceNumber>)
    {
        auto pending = std::move(wstate->buffered.front());
        wstate->buffered.pop_front();
        wstate->pendingSequenceNumber = pending.getSequenceNumber();
        wstate->pendingChunkNumber = pending.getChunkNumber();
        return pending;
    }
    return {};
}

std::optional<TupleBuffer> SinkBackpressureHandler::onSuccess(BackpressureController& bpc)
{
    const auto state = stateLock.wlock();
    state->pendingSequenceNumber = INVALID<SequenceNumber>;
    state->pendingChunkNumber = INVALID<ChunkNumber>;
    if (state->hasBackpressure && state->buffered.size() <= lowerThreshold)
    {
        bpc.releasePressure();
        NES_DEBUG("TokioSink backpressure released: {} buffered (lower: {})", state->buffered.size(), lowerThreshold);
        state->hasBackpressure = false;
    }
    if (!state->buffered.empty())
    {
        auto next = std::move(state->buffered.front());
        state->buffered.pop_front();
        return {next};
    }
    return {};
}

bool SinkBackpressureHandler::empty() const
{
    return stateLock.rlock()->buffered.empty();
}

std::optional<TupleBuffer> SinkBackpressureHandler::popFront()
{
    const auto state = stateLock.wlock();
    if (!state->buffered.empty())
    {
        auto front = std::move(state->buffered.front());
        state->buffered.pop_front();
        return front;
    }
    return {};
}

// ---- TokioSink implementation ----
template <typename T>
void update_maximum(std::atomic<T>& maximum_value, const T& value) noexcept
{
    T prev_value = maximum_value;
    while (prev_value < value && !maximum_value.compare_exchange_weak(prev_value, value))
    {
    }
}

/// Serialize a DescriptorConfig::Config as flat {"key":"value",...} JSON
/// for the Rust FFI, which expects HashMap<String, String>.
static std::string configToFlatJson(const DescriptorConfig::Config& config)
{
    std::unordered_map<std::string, std::string> flat;
    for (const auto& [key, value] : config)
    {
        flat[key] = std::get<std::string>(value);
    }
    return rfl::json::write(flat);
}

static DescriptorConfig::Config addHeader(DescriptorConfig::Config config, std::shared_ptr<const Schema> schema)
{
    config.insert_or_assign(UppercaseString("HEADER"), SchemaFormatter(schema).getFormattedSchema());
    config.insert_or_assign(UppercaseString("TUPLE_SIZE"), std::to_string(schema->getSizeOfSchemaInBytes()));
    return config;
}

struct TokioSinkContext
{
    std::optional<size_t> flushEpoch;
    std::atomic_size_t epochCounter{0};
    std::atomic_bool gracefulStop{false};

    folly::Synchronized<std::optional<std::string>> asyncError;

    std::shared_ptr<SinkContext> context;
    rust::Box<SinkHandle> handle;

    TokioSinkContext(const QueryId& queryId, PipelineId pipelineId, const std::string& sinkType, const SinkDescriptor& descriptor)
        : context(std::make_shared<SinkContext>(
              [this](std::string_view error)
              {
                  NES_ERROR("Sink Failed: {}", error);
                  this->asyncError.wlock()->emplace(error);
              },
              [this](size_t epoch)
              {
                  NES_DEBUG("Sink Flushed: {}", epoch);
                  update_maximum(this->epochCounter, epoch);
              }))
        , handle(create_handle(
              QueryContext{
                  .query_id = queryId.getLocalQueryId().getRawValue(),
                  .distributed_query_id = queryId.getDistributedQueryId().getRawValue(),
                  .sink_id = pipelineId.getRawValue(),
                  .sink_type = sinkType,
                  .sink_config = configToFlatJson(addHeader(descriptor.getConfig(), descriptor.getSchema()))},
              context))
    {
    }

    /// Throws CannotOpenSink if the async sink task reported an error via on_error.
    void throwIfAsyncError() const
    {
        if (auto err = *asyncError.rlock())
        {
            throw CannotOpenSink("TokioSink failed: {}", *err);
        }
    }

    ~TokioSinkContext()
    {
        if (gracefulStop)
        {
            NES_DEBUG("Sink closed gracefully");
        }
        else
        {
            sink_fail(*handle, "Query Failed");
            NES_DEBUG("Sink was aborted");
        }
    }
};

TokioSink::TokioSink(BackpressureController backpressureController, SinkDescriptor descriptor, size_t upperThreshold, size_t lowerThreshold)
    : Sink(std::move(backpressureController))
    , descriptor(descriptor)
    , context(nullptr)
    , backpressureHandler(std::make_unique<SinkBackpressureHandler>(upperThreshold, lowerThreshold))
{
}

/// Sink Termination:
/// Sink Termination is tied to how Pipeline Termination works in the nebulastreams query engine.
/// A pipeline may be gracefully stopped if the query is terminated due to a stop request from a user or all sources have completed.
/// If a query fails, the pipeline stop is **not** invoked. The pipeline is simply destructed.
/// So a pipeline can differentiate between graceful termination and forceful termination by setting a flag in the stop method.
/// stop is guaranteed:
///     - to be called once from a single thread without any concurrent access to the sink. (no more pending tasks)
///
/// stop is also guaranteed to be called only if not a single pending (including delayed-repeated) tasks are in the queue. stop will be called without any concurrent modification to the sink
///
/// Graceful termination is a three-stage protocol over an mpsc queue to the rust sink task:
///   1. Drain the backpressure backlog via try_write.
///   2. Issue a Flush with an epoch tag and repeat the task until the rust
///      side acks the epoch via on_flush — at that point all queued data has
///      been written by the sink.
///   3. Issue a Close via sink_stop() and repeat the task until sink_stop
///      returns true — at that point the rust sink task has run AsyncSink::stop
///      to completion and the SinkHandle is safe to drop.
///
/// Forceful termination (query failure / destruction without stop()): the
/// SinkHandle is dropped while the rust controller is still live, which
/// drops the controller's Sender. The rust task observes the closed
/// controller and panics — mirroring the source runtime's enforcement of
/// the never-drop contract.
TokioSink::~TokioSink() = default;

void TokioSink::start(PipelineExecutionContext& pec)
{
    this->context = std::make_unique<TokioSinkContext>(pec.getQueryId(), pec.getPipelineId(), descriptor.getSinkType(), descriptor);
}

void TokioSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec)
{
    context->throwIfAsyncError();
    auto currentBuffer = std::optional(inputBuffer);
    while (currentBuffer)
    {
        switch (try_write(*context->handle, toRust(*currentBuffer)))
        {
            case ::WriteResult::Ok:
                currentBuffer = backpressureHandler->onSuccess(backpressureController);
                break;

            case ::WriteResult::Full:
                if (auto retry = backpressureHandler->onFull(*currentBuffer, backpressureController))
                {
                    pec.repeatTask(*retry, BACKPRESSURE_RETRY_INTERVAL);
                }
                return;
        }
    }
}

void TokioSink::stop(PipelineExecutionContext& pec)
{
    context->throwIfAsyncError();
    auto currentBuffer = backpressureHandler->popFront();
    while (currentBuffer)
    {
        switch (try_write(*context->handle, toRust(*currentBuffer)))
        {
            case ::WriteResult::Ok:
                currentBuffer = backpressureHandler->onSuccess(backpressureController);
                break;

            case ::WriteResult::Full:
                if (auto retry = backpressureHandler->onFull(*currentBuffer, backpressureController))
                {
                    pec.repeatTask(*retry, BACKPRESSURE_RETRY_INTERVAL);
                }
                return;
        }
    }

    // Stage 1: issue a Flush with an epoch tag and wait until the rust task
    // acks it via on_flush. Once acked, all previously-enqueued Data
    // commands have been processed by AsyncSink::execute and flushed.
    if (!context->flushEpoch)
    {
        NES_DEBUG("Start flush");
        auto epoch = sink_flush(*context->handle);
        if (epoch == 0)
        {
            NES_DEBUG("Could not flush, queue is full");
            pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
            return;
        }
        NES_DEBUG("Waiting for flush ack: {}", epoch);
        context->flushEpoch = epoch;
    }
    if (context->epochCounter < context->flushEpoch)
    {
        sink_sanity_check(*context->handle);
        NES_DEBUG("Repeating until flush ack");
        pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
        return;
    }

    // Stage 2: drive sink_stop() until it returns true. The first call
    // submits SinkCommand::Close; subsequent calls poll the stop_signal
    // oneshot. Once true, the rust task has run AsyncSink::stop to
    // completion and the SinkHandle is safe to drop.
    if (!sink_stop(*context->handle))
    {
        NES_DEBUG("Repeating until sink stop ack");
        pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
        return;
    }

    context->gracefulStop = true;
    NES_INFO("Sink Closed successfully");
}

std::ostream& TokioSink::toString(std::ostream& os) const
{
    return os << "TokioSink{sinkId=" << sinkId << "}";
}

SinkRegistryReturnType RegisterTokioSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<TokioSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

} // namespace NES
