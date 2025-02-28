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
#include <utility>
#include <variant>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/this_coro.hpp>

#include <Async/IOThread.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

AsyncSourceRunner::AsyncSourceRunner(SourceExecutionContext<AsyncSource> sourceExecutionContext, EmitFunction&& emitFn)
    : context(std::move(sourceExecutionContext)), emitFn(std::move(emitFn))
{
}

asio::awaitable<void, Executor> AsyncSourceRunner::runningRoutine() const
{
    /// Helper lambda that we forward to the input formatter to emit data events on our behalf
    uint64_t sequenceNumberGenerator{SequenceNumber::INITIAL};
    const std::function dataEmit = [&](IOBuffer& buf) -> void
    {
        addBufferMetadata(context.originId, buf, sequenceNumberGenerator++);
        emitFn(context.originId, Data{std::move(buf)});
    };

    ///  RAII-wrapper around the source to ensure that close() is called on all code paths, even if an exception is thrown.
    const AsyncSourceWrapper sourceHandle{*context.sourceImpl};
    try
    {
        /// Try to open the source.
        co_await sourceHandle.source.open();
    }
    catch (const Exception& exception)
    {
        emitFn(context.originId, Error{exception});
        co_return;
    }

    /// Run forever, or until either:
    /// a) Cancellation is requested from the query engine (query stop)
    /// b) An error occurs within the source
    /// c) EoS is signalled by the source
    AsyncSource::InternalSourceResult internalSourceResult = AsyncSource::Continue{};
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
        auto buffer = context.bufferProvider->getBufferBlocking();
        /// 2. Let source ingest raw bytes into buffer
        /// All IO-related errors are handled internally and returned via the result variant.
        internalSourceResult = co_await sourceHandle.source.fillBuffer(buffer);
        /// 3. Handle the result and communicate it to the query engine via the emitFn
        /// The returned value decides whether we should continue to ingest data.
        handleSourceResult(buffer, internalSourceResult, dataEmit);
    }
}

void AsyncSourceRunner::handleSourceResult(
    IOBuffer& buffer, const AsyncSource::InternalSourceResult& internalSourceResult, const std::function<void(IOBuffer&)>& dataEmit) const
{
    std::visit(
        Overloaded{
            [&](const AsyncSource::Continue& continue_)
            {
                /// Usual case, the source is ready to continue to ingest more data
                buffer.setNumberOfTuples(continue_.bytesRead);
                dataEmit(buffer);
            },
            [&](const AsyncSource::EndOfStream& eos)
            {
                /// The source signalled the end of the stream, i.e., because the external system closed the connection.
                /// Check for a partially filled buffer before signalling EoS and exiting
                if (eos.bytesRead)
                {
                    buffer.setNumberOfTuples(eos.bytesRead);
                    dataEmit(buffer);
                }
                emitFn(context.originId, EoS{});
            },
            [this](const AsyncSource::Error& error)
            {
                /// The source returned an error that it is not able to recover from.
                /// Therefore, we propagate the error to the query engine via the emitFn.
                emitFn(context.originId, Error{error.exception});
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
