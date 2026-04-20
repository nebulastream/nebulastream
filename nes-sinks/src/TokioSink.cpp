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
#include <nes-sink-validation-bindings/lib.h>
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
#include "SinkValidationRegistry.hpp"

namespace NES
{
class QueryId;

// ---- SinkBackpressureHandler implementation ----

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
    return config;
}

struct TokioSinkContext
{
    std::optional<size_t> stopEpoch;
    std::atomic_size_t epochCounter;
    std::shared_ptr<SinkContext> context;
    rust::Box<SinkHandle> handle;

    TokioSinkContext(const QueryId& queryId, PipelineId pipelineId, const std::string& sinkType, const SinkDescriptor& descriptor)
        : epochCounter(0)
        , context(std::make_shared<SinkContext>(
              [](std::string_view error) { NES_ERROR("Sink Failed: {}", error); },
              [this](size_t epoch)
              {
                  NES_INFO("Sink Flushed: {}", epoch);
                  update_maximum(this->epochCounter, epoch);
              }))
        , handle(create_handle(
              QueryContext{
                  .query_id = queryId.getLocalQueryId().getRawValue(),
                  .distributed_query_id = queryId.getDistributedQueryId().getRawValue(),
                  .sink_id = pipelineId.getRawValue(),
                  .sink_type = sinkType,
                  .sink_config = configToFlatJson(addHeader(descriptor.getConfig(), descriptor.getSchema()))},
              IORuntime::get().id(),
              context))
    {
    }

    ~TokioSinkContext() { NES_DEBUG("TokioSink::~Context()"); }
};

TokioSink::TokioSink(BackpressureController backpressureController, SinkDescriptor descriptor, size_t upperThreshold, size_t lowerThreshold)
    : Sink(std::move(backpressureController)), descriptor(descriptor), context(nullptr), backpressureHandler(upperThreshold, lowerThreshold)
{
}

TokioSink::~TokioSink()
{
    // rustHandle (rust::Box<SinkHandle>) dropped automatically by unique_ptr destructor.
    // If stop was never called, Rust sink sees broken channel -> implicit close (Phase 4).
}

void TokioSink::start(PipelineExecutionContext& pec)
{
    this->context = std::make_unique<TokioSinkContext>(pec.getQueryId(), pec.getPipelineId(), descriptor.getSinkType(), descriptor);
}

void TokioSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec)
{
    auto currentBuffer = std::optional(inputBuffer);
    while (currentBuffer)
    {
        switch (try_write(*context->handle, toRust(*currentBuffer)))
        {
            case ::WriteResult::Ok:
                currentBuffer = backpressureHandler.onSuccess(backpressureController);
                break;

            case ::WriteResult::Full:
                if (auto retry = backpressureHandler.onFull(*currentBuffer, backpressureController))
                {
                    pec.repeatTask(*retry, BACKPRESSURE_RETRY_INTERVAL);
                }
                return;
        }
    }
}

void TokioSink::stop(PipelineExecutionContext& pec)
{
    auto currentBuffer = backpressureHandler.popFront();
    while (currentBuffer)
    {
        switch (try_write(*context->handle, toRust(*currentBuffer)))
        {
            case ::WriteResult::Ok:
                currentBuffer = backpressureHandler.onSuccess(backpressureController);
                break;

            case ::WriteResult::Full:
                if (auto retry = backpressureHandler.onFull(*currentBuffer, backpressureController))
                {
                    pec.repeatTask(*retry, BACKPRESSURE_RETRY_INTERVAL);
                }
                return;
        }
    }

    if (!context->stopEpoch)
    {
        NES_DEBUG("Start flush");
        auto stopEpoch = flush(*context->handle);
        if (stopEpoch == 0)
        {
            NES_DEBUG("Could not flush queue is full");
            pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
            return;
        }
        NES_DEBUG("Waiting for flush ack: {}", stopEpoch);
        context->stopEpoch = stopEpoch;
    }
    if (context->epochCounter < context->stopEpoch)
    {
        NES_DEBUG("Repeating until flush ack");
        pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
    }
    else
    {
        NES_INFO("Sink Closed successfully");
    }
}

std::ostream& TokioSink::toString(std::ostream& os) const
{
    return os << "TokioSink{sinkId=" << sinkId << "}";
}

SinkValidationRegistryReturnType RegisterTokioSinkValidation(SinkValidationRegistryArguments args)
{
    std::unordered_map<std::string, std::string> flat;
    for (auto& [key, value] : args.config)
    {
        flat[key.getString()] = std::move(value);
    }
    auto result = validate(args.sinkType, rfl::json::write(flat));
    auto validatedConfig
        = rfl::json::read<std::unordered_map<std::string, std::string>>(std::string_view(result.begin(), result.end())).value();

    DescriptorConfig::Config config;
    for (auto& [key, value] : validatedConfig)
    {
        config.emplace(UppercaseString(std::move(key)), DescriptorConfig::ConfigType{std::move(value)});
    }
    return config;
}

SinkRegistryReturnType RegisterTokioSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<TokioSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

} // namespace NES
