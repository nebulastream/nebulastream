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

#include <Async/AsyncSourceCoroutineWrapper.hpp>

#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include <boost/asio/awaitable.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

AsyncSourceCoroutineWrapper::AsyncSourceCoroutineWrapper(
    SourceExecutionContext&& sourceExecutionContext, std::promise<void> terminationPromise, EmitFunction&& emitFn)
    : sourceExecutionContext(std::move(sourceExecutionContext))
    , terminationPromise(std::move(terminationPromise))
    , emitFn(std::move(emitFn))
{
}

asio::awaitable<void> AsyncSourceCoroutineWrapper::open() const
{
    co_await std::get<std::unique_ptr<AsyncSource>>(sourceExecutionContext.sourceImpl)->open();
}

asio::awaitable<AsyncSource::InternalSourceResult> AsyncSourceCoroutineWrapper::fillBuffer(IOBuffer& buffer) const
{
    const auto sourceResult = co_await std::get<std::unique_ptr<AsyncSource>>(sourceExecutionContext.sourceImpl)->fillBuffer(buffer);
    co_return sourceResult;
}

void AsyncSourceCoroutineWrapper::close() const
{
    std::get<std::unique_ptr<AsyncSource>>(sourceExecutionContext.sourceImpl)->close();
}


asio::awaitable<void> AsyncSourceCoroutineWrapper::runningRoutine()
{
    PRECONDITION(
        std::holds_alternative<std::unique_ptr<AsyncSource>>(sourceExecutionContext.sourceImpl),
        "Coroutine wrapper must hold an async source");

    /// Helper lambda that we forward to the input formatter to emit data events on our behalf
    uint64_t sequenceNumberGenerator{SequenceNumber::INITIAL};
    const std::function<void(IOBuffer&)> dataEmit = [&](IOBuffer& buf) -> void
    {
        addBufferMetadata(sourceExecutionContext.originId, buf, sequenceNumberGenerator++);
        emitFn(sourceExecutionContext.originId, Data{std::move(buf)});
    };

    try
    {
        /// Try to open the source. On failure, propagate the error to the query engine via the emitFn.
        co_await open();

        /// Run forever, or until either:
        /// a) Cancellation is requested from the query engine (query stop)
        /// b) An error occurs within the source
        AsyncSource::InternalSourceResult internalSourceResult = AsyncSource::Continue{};
        while (std::holds_alternative<AsyncSource::Continue>(internalSourceResult))
        {
            /// 1. Acquire buffer
            /// In the future, we might replace this with an async call: when the system is running at capacity, blocking here can slow down all other sources.
            auto buffer = sourceExecutionContext.bufferProvider->getBufferBlocking();
            /// 2. Let source ingest raw bytes into buffer
            internalSourceResult = co_await fillBuffer(buffer);
            /// 3. Handle the result and communicate it to the query engine via the emitFn
            /// The returned value decides whether we should continue to ingest data.
            handleSourceResult(buffer, internalSourceResult, dataEmit);
        }

        /// Close the underlying source.
        close();

        if (std::holds_alternative<AsyncSource::Error>(internalSourceResult))
        {
            auto exception = std::get<AsyncSource::Error>(internalSourceResult).exception;
            NES_ERROR("Source encountered an error: {}", exception.what());

            terminationPromise.set_exception(std::make_exception_ptr(std::move(exception)));
        }
        else
        {
            terminationPromise.set_value();
        }
    }
    catch (const Exception& exception)
    {
        NES_ERROR("Source encountered an error: {}", exception.what());
        emitFn(sourceExecutionContext.originId, Error{.ex = exception});
        terminationPromise.set_exception(std::make_exception_ptr(exception));
    }
}

void AsyncSourceCoroutineWrapper::cancel() const
{
    std::get<std::unique_ptr<AsyncSource>>(sourceExecutionContext.sourceImpl)->cancel();
}


void AsyncSourceCoroutineWrapper::handleSourceResult(
    IOBuffer& buffer, AsyncSource::InternalSourceResult result, const std::function<void(IOBuffer&)>& dataEmit)
{
    std::visit(
        [&](auto&& sourceResult) -> void
        {
            using T = std::decay_t<decltype(sourceResult)>;
            if constexpr (std::is_same_v<T, AsyncSource::Continue>)
            {
                /// Usual case, the source is ready to continue to ingest more data
                NES_DEBUG("Emitting Buffer to formatter...");
                sourceExecutionContext.inputFormatter->parseTupleBufferRaw(
                    buffer, *sourceExecutionContext.bufferProvider, sourceResult.bytesRead, dataEmit);
            }
            else if constexpr (std::is_same_v<T, AsyncSource::EndOfStream>)
            {
                /// The source signalled the end of the stream, i.e., because the external system closed the connection.
                /// Check for a partially filled buffer before signalling EoS and exiting
                if (sourceResult.bytesRead)
                {
                    sourceExecutionContext.inputFormatter->parseTupleBufferRaw(
                        buffer, *sourceExecutionContext.bufferProvider, sourceResult.bytesRead, dataEmit);
                }
                emitFn(sourceExecutionContext.originId, EoS{});
            }
            else if constexpr (std::is_same_v<T, AsyncSource::Error>)
            {
                /// The source returned an error that it is not able to recover from.
                /// Therefore, we propagate the error to the query engine via the emitFn.
                emitFn(sourceExecutionContext.originId, Error{sourceResult.exception});
            }
            else // if (std::is_same_v<T, AsyncSource::Cancelled>)
            {
                /// Cancellation was requested by the query engine.
                /// At this point, we can be sure that there are no more internal handlers for asio I/O requests queued or running.
                /// We can exit the coroutine gracefully.
                /// We do not emit any event here, as the query engine will handle the termination of the pipeline.
            }
        },
        result);
}

}
