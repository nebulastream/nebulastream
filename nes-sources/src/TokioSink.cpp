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

#include <Sources/TokioSink.hpp>

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

// CXX-generated header for ffi_sink module
#include <nes-source-lib/src/lib.h>
// IoBindings provides ErrorContext, on_sink_error_callback
#include <IoBindings.hpp>

namespace NES
{

// ---- SinkBackpressureHandler implementation ----

SinkBackpressureHandler::SinkBackpressureHandler(size_t upperThreshold, size_t lowerThreshold)
    : upperThreshold(upperThreshold), lowerThreshold(lowerThreshold)
{
    PRECONDITION(lowerThreshold < upperThreshold,
        "lowerThreshold ({}) must be less than upperThreshold ({})", lowerThreshold, upperThreshold);
}

std::optional<TupleBuffer> SinkBackpressureHandler::onFull(TupleBuffer buffer, BackpressureController& bpc)
{
    auto rstate = stateLock.ulock();
    if (buffer.getSequenceNumber() == rstate->pendingSequenceNumber
        && buffer.getChunkNumber() == rstate->pendingChunkNumber)
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

// ---- RustSinkHandleImpl PIMPL ----

struct TokioSink::RustSinkHandleImpl
{
    rust::Box<::SinkHandle> handle;
    explicit RustSinkHandleImpl(rust::Box<::SinkHandle> h) : handle(std::move(h)) {}
};

// ---- TokioSink implementation ----

TokioSink::TokioSink(
    BackpressureController backpressureController,
    SpawnFn spawnFn,
    uint32_t channelCapacity,
    size_t upperThreshold,
    size_t lowerThreshold)
    : Sink(std::move(backpressureController))
    , spawnFn(std::move(spawnFn))
    , channelCapacity(channelCapacity)
    , backpressureHandler(upperThreshold, lowerThreshold)
{
}

TokioSink::~TokioSink()
{
    // rustHandle (rust::Box<SinkHandle>) dropped automatically by unique_ptr destructor.
    // If stop was never called, Rust sink sees broken channel -> implicit close (Phase 4).
}

void TokioSink::start(PipelineExecutionContext& /*pec*/)
{
    if (started)
    {
        NES_ERROR("TokioSink: start() called but already started");
        return;
    }

    // Use a simple counter for sink_id. Phase 6 registry will assign meaningful IDs.
    static std::atomic<uint64_t> sinkIdCounter{1};
    sinkId = sinkIdCounter.fetch_add(1, std::memory_order_relaxed);

    try
    {
        errorContext = std::make_unique<ErrorContext>(ErrorContext{sinkId});

        rustHandle = spawnFn(
            sinkId,
            channelCapacity,
            reinterpret_cast<uintptr_t>(on_sink_error_callback),
            reinterpret_cast<uintptr_t>(errorContext.get())
        );

        started = true;
        NES_DEBUG("TokioSink {}: started successfully (channel capacity={})", sinkId, channelCapacity);
    }
    catch (const std::exception& e)
    {
        NES_ERROR("TokioSink: start() failed: {}", e.what());
        errorContext.reset();
        throw;
    }
}

void TokioSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec)
{
    // Pitfall 2: Check stopInitiated to prevent sends after stop begins.
    if (stopInitiated.load(std::memory_order_acquire))
    {
        NES_WARNING("TokioSink {}: dropping buffer after stop initiated (seq={})",
                    sinkId, inputBuffer.getSequenceNumber());
        return;
    }

    auto currentBuffer = std::optional(inputBuffer);
    while (currentBuffer)
    {
        // Heap-allocate a copy for Rust ownership transfer.
        // The copy constructor calls retain() on the underlying buffer.
        // On Success: Rust owns the heap copy and will release on TupleBufferHandle drop.
        // On Full/Closed: Rust forgot the message, so C++ deletes the heap copy
        //   (destructor calls release, undoing the copy constructor's retain).
        auto* heapBuf = new TupleBuffer(*currentBuffer);
        auto bufferPtr = reinterpret_cast<uintptr_t>(heapBuf);

        auto result = ::sink_send_buffer(*rustHandle->handle, bufferPtr);
        switch (result)
        {
            case ::SendResult::Success:
                // Heap buffer transferred to Rust. Rust will release on TupleBufferHandle drop.
                currentBuffer = backpressureHandler.onSuccess(backpressureController);
                break;

            case ::SendResult::Full:
                // Rust forgot the message; clean up the leaked heap copy.
                delete heapBuf;
                if (auto retry = backpressureHandler.onFull(*currentBuffer, backpressureController))
                {
                    pec.repeatTask(*retry, BACKPRESSURE_RETRY_INTERVAL);
                }
                return;

            case ::SendResult::Closed:
                // Rust forgot the message; clean up the leaked heap copy.
                delete heapBuf;
                NES_ERROR("TokioSink {}: channel closed unexpectedly", sinkId);
                throw CannotOpenSink("TokioSink channel closed unexpectedly");
        }
    }
}

void TokioSink::stop(PipelineExecutionContext& pec)
{
    if (!started)
    {
        return;
    }

    // Set stopInitiated atomically BEFORE any drain/close (Pitfall 2).
    stopInitiated.store(true, std::memory_order_release);

    switch (stopPhase)
    {
        case StopPhase::RUNNING:
            stopPhase = StopPhase::DRAINING;
            [[fallthrough]];

        case StopPhase::DRAINING:
        {
            // Drain BackpressureHandler's buffered items into the channel.
            while (auto item = backpressureHandler.popFront())
            {
                // Heap-allocate for Rust ownership (same pattern as execute).
                auto* heapBuf = new TupleBuffer(*item);
                auto bufferPtr = reinterpret_cast<uintptr_t>(heapBuf);
                auto result = ::sink_send_buffer(*rustHandle->handle, bufferPtr);
                if (result == ::SendResult::Full)
                {
                    delete heapBuf; // Rust forgot; clean up
                    // Can't drain yet, retry
                    pec.repeatTask(*item, BACKPRESSURE_RETRY_INTERVAL);
                    return;
                }
                else if (result == ::SendResult::Closed)
                {
                    delete heapBuf; // Rust forgot; clean up
                    NES_WARNING("TokioSink {}: channel closed during drain", sinkId);
                    stopPhase = StopPhase::WAITING_DONE;
                    break;
                }
                // Success: heap buffer transferred to Rust, continue draining
            }
            if (stopPhase == StopPhase::DRAINING)
            {
                stopPhase = StopPhase::CLOSING;
            }
            if (stopPhase == StopPhase::WAITING_DONE)
            {
                break; // Channel closed during drain, skip to waiting
            }
            [[fallthrough]];
        }

        case StopPhase::CLOSING:
        {
            // Try to send Close via stop_sink_bridge.
            bool sent = ::stop_sink_bridge(*rustHandle->handle);
            if (!sent)
            {
                // Channel full, retry
                pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
                return;
            }
            NES_DEBUG("TokioSink {}: Close sent", sinkId);
            stopPhase = StopPhase::WAITING_DONE;
            [[fallthrough]];
        }

        case StopPhase::WAITING_DONE:
        {
            // Poll until Rust sink confirms completion.
            if (!::is_sink_done_bridge(*rustHandle->handle))
            {
                pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
                return;
            }
            NES_DEBUG("TokioSink {}: sink done, cleanup complete", sinkId);
            break;
        }
    }
}

std::ostream& TokioSink::toString(std::ostream& os) const
{
    return os << "TokioSink{sinkId=" << sinkId << "}";
}

// ---- Factory helpers ----

TokioSink::SpawnFn makeDevNullSinkSpawnFn()
{
    return [](uint64_t sinkId, uint32_t channelCapacity,
              uintptr_t errorFnPtr, uintptr_t errorCtxPtr)
        -> std::unique_ptr<TokioSink::RustSinkHandleImpl>
    {
        auto boxedHandle = ::spawn_devnull_sink(sinkId, channelCapacity, errorFnPtr, errorCtxPtr);
        return std::make_unique<TokioSink::RustSinkHandleImpl>(std::move(boxedHandle));
    };
}

TokioSink::SpawnFn makeFileSinkSpawnFn(std::string filePath)
{
    return [filePath = std::move(filePath)](
        uint64_t sinkId, uint32_t channelCapacity,
        uintptr_t errorFnPtr, uintptr_t errorCtxPtr)
        -> std::unique_ptr<TokioSink::RustSinkHandleImpl>
    {
        auto boxedHandle = ::spawn_file_sink(sinkId, filePath, channelCapacity, errorFnPtr, errorCtxPtr);
        return std::make_unique<TokioSink::RustSinkHandleImpl>(std::move(boxedHandle));
    };
}

} // namespace NES
