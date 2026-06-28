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

#include <Async/AsyncSourceRunner.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <variant>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/this_coro.hpp>

#include <Async/IOThread.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/AsyncSource.hpp>
// #include <Sources/SourceExecutionContext.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

AsyncSourceRunner::AsyncSourceRunner(std::unique_ptr<AsyncSource> asyncSource, SourceReturnType::EmitFunction&& emitFn)
    : source(std::move(asyncSource)), emitFn(std::move(emitFn))
{
}

asio::awaitable<void, Executor>
AsyncSourceRunner::runningRoutine(OriginId sourceId, std::shared_ptr<AbstractBufferProvider> bufferProvider) const
{
    /// Helper lambda that we forward to the input formatter to emit data events on our behalf
    uint64_t sequenceNumberGenerator{SequenceNumber::INITIAL};
    auto stopToken = stopSource.get_token();
    const auto dataEmit = [this, sourceId, &sequenceNumberGenerator, &stopToken](TupleBuffer& buf) -> void
    {
        addBufferMetadata(sourceId, buf, sequenceNumberGenerator++);
        emitFn(sourceId, SourceReturnType::Data{std::move(buf)}, stopToken);
        // emitFn(sourceId, SourceReturnType::Data{std::move(buf)});
    };

    ///  RAII-wrapper around the source to ensure that close() is called on all code paths, even if an exception is thrown.
    const AsyncSourceWrapper sourceHandle{*source};
    try
    {
        /// Try to open the source. The source gets its own (inflight-sized) pool here so a deep-QD source can
        /// keep many reads in flight; single-buffer sources ignore it.
        co_await sourceHandle.source.open(bufferProvider);
    }
    catch (const Exception& exception)
    {
        auto backpressureListenerException = RunningRoutineFailure(exception.what());
        emitFn(sourceId, SourceReturnType::Error{std::move(backpressureListenerException)}, stopToken);
        co_return;
    }

    /// Prefill model (mirrors the blocking runner): the source owns its buffers (deep QD) and hands back the
    /// next completed one in order. We must NOT pre-acquire a buffer here -- that would contend with the
    /// source's own ring for the same pool and can deadlock. nullopt => end-of-stream (unless cancelled).
    if (sourceHandle.source.preFillsBuffers())
    {
        while (true)
        {
            if ((co_await asio::this_coro::cancellation_state).cancelled() == asio::cancellation_type::terminal)
            {
                break;
            }
            std::optional<TupleBuffer> preFilled;
            try
            {
                preFilled = co_await sourceHandle.source.takePreFilledBuffer();
            }
            catch (const Exception& exception)
            {
                auto failure = RunningRoutineFailure(exception.what());
                emitFn(sourceId, SourceReturnType::Error{std::move(failure)}, stopToken);
                co_return;
            }
            if (not preFilled.has_value())
            {
                /// A cancelled takePreFilledBuffer also returns nullopt -- only signal EoS if we were not stopped.
                if ((co_await asio::this_coro::cancellation_state).cancelled() == asio::cancellation_type::terminal)
                {
                    break;
                }
                emitFn(sourceId, SourceReturnType::EoS{}, stopToken);
                break;
            }
            dataEmit(*preFilled);
        }
        co_return;
    }

    /// Run forever, or until either:
    /// a) Cancellation is requested from the query engine (query stop)
    /// b) An error occurs within the source
    /// c) EoS is signalled by the source
    AsyncSource::InternalSourceResult internalSourceResult = AsyncSource::Continue{};
    /// Opt-in ingest-copy meter (see SourceUtility.hpp). Free when NES_INGEST_COPY_STATS is unset.
    IngestCopyMeter copyMeter;
    while (std::holds_alternative<AsyncSource::Continue>(internalSourceResult))
    {
        /// If we see the cancellation request here, we can safely terminate, as no IO completion handlers can be queued
        /// We know this because all IO operations' results are awaited within the source and new requests are only issued after the previous one completes.
        if ((co_await asio::this_coro::cancellation_state).cancelled() == asio::cancellation_type::terminal)
        {
            break;
        }
        /// 1. Acquire buffer
        /// TODO(yschroeder97): replace this with timeout-based approach
        /// In the future, we might replace this with an async call: when the system is running at capacity, blocking here can slow down all other sources.
        auto buffer = bufferProvider->getBufferBlocking();
        /// 2. Let source ingest raw bytes into buffer. Stamp sourceCreationTimestamp at READ-START (us)
        /// so the sink's e2e latency/throughput include the read/recv copy (not just in-engine time);
        /// bracket fillBuffer to accumulate the per-buffer copy cost.
        /// All IO-related errors are handled internally and returned via the result variant.
        const auto readStartMicros = ingestNowMicros();
        buffer.setSourceCreationTimestampInMS(Timestamp(readStartMicros));
        internalSourceResult = co_await sourceHandle.source.fillBuffer(buffer);
        if (copyMeter.enabled())
        {
            copyMeter.add(ingestNowMicros() - readStartMicros, buffer.getNumberOfTuples());
        }
        /// 3. Handle the result and communicate it to the query engine via the emitFn
        /// The returned value decides whether we should continue to ingest data.
        handleSourceResult(buffer, internalSourceResult, dataEmit, sourceId, stopToken);
    }
    copyMeter.writeOnClose(sourceId);
}

void AsyncSourceRunner::handleSourceResult(
    TupleBuffer& buffer,
    const AsyncSource::InternalSourceResult& internalSourceResult,
    const std::function<void(TupleBuffer&)>& dataEmit,
    const OriginId sourceId,
    const std::stop_token& stopToken) const
{
    std::visit(
        Overloaded{
            [&](const AsyncSource::Continue&)
            {
                /// Usual case, the source is ready to continue to ingest more data
                dataEmit(buffer);
                // context.inputFormatter->parseTupleBufferRaw(buffer, *context.bufferProvider, resultContinue.bytesRead, dataEmit);
            },
            [&](const AsyncSource::EndOfStream& resultEos)
            {
                /// The source signalled the end of the stream, i.e., because the external system closed the connection.
                /// Check for a partially filled buffer before signalling EoS and exiting
                if (resultEos.bytesRead)
                {
                    dataEmit(buffer);
                    // context.inputFormatter->parseTupleBufferRaw(buffer, *context.bufferProvider, resultEos.bytesRead, dataEmit);
                }
                emitFn(sourceId, SourceReturnType::EoS{}, stopToken);
                // emitFn(context.originId, EoS{});
            },
            [this, sourceId, &stopToken](const AsyncSource::Error& resultError)
            {
                /// The source returned an error that it is not able to recover from.
                /// Therefore, we propagate the error to the query engine via the emitFn.
                auto backpressureListenerException = RunningRoutineFailure(resultError.exception.what());
                emitFn(sourceId, SourceReturnType::Error{std::move(backpressureListenerException)}, stopToken);
                // emitFn(context.originId, Error{resultError.exception});
            },
            [](const AsyncSource::Cancelled&)
            {
                /// Cancellation was requested by the query engine.
                /// At this point, we can be sure that there are no more internal handlers for asio I/O requests queued or running.
                /// We can exit the coroutine gracefully.
                /// We do not emit any event here, as the query engine will handle the termination of the pipeline.
            }},
        internalSourceResult);
}

}
