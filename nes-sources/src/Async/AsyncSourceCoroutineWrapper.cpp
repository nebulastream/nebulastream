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

#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include <boost/asio/awaitable.hpp>
#include <boost/system/system_error.hpp>

#include <Sources/AsyncSource.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceReturnType.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

AsyncSourceCoroutineWrapper::AsyncSourceCoroutineWrapper(
    SourceExecutionContext sourceExecutionContext, std::shared_ptr<AsyncSourceExecutor> executor, SourceReturnType::EmitFunction&& emitFn)
    : sourceExecutionContext(std::move(sourceExecutionContext)), executor(std::move(executor)), emitFn(std::move(emitFn))
{
}

asio::awaitable<void> AsyncSourceCoroutineWrapper::open() const
{
    co_await std::get<std::unique_ptr<AsyncSource>>(sourceExecutionContext.sourceImpl)->open(executor->ioContext());
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


asio::awaitable<void> AsyncSourceCoroutineWrapper::start()
{
    INVARIANT(std::holds_alternative<std::unique_ptr<AsyncSource>>(sourceExecutionContext.sourceImpl));
    try
    {
        /// Try to open the source. On failure, propagate the error to the query engine via the emitFn.
        co_await open();
    }
    catch (const Exception& e)
    {
        emitFn(sourceExecutionContext.originId, SourceReturnType::Error{.ex = e});
    }

    bool shouldContinue = true;
    /// Run forever, or until either:
    /// a) Cancellation is requested from the query engine (query stop)
    /// b) An error occurs within the source
    while (shouldContinue)
    {
        /// TODO(yschroeder97): write `getBufferAsync()` machinery, this will slow down all sources when the system is running at capacity
        /// 1. Acquire buffer
        auto buffer = sourceExecutionContext.bufferProvider->getBufferBlocking();
        /// 2. Add buffer metadata
        sourceExecutionContext.addBufferMetadata(buffer);
        /// 3. Let source ingest raw bytes into buffer
        const auto result = co_await fillBuffer(buffer);
        /// 4. Handle the result and communicate it to the query engine via the emitFn
        shouldContinue = handleSourceResult(buffer, result);
    }
    close();
}

void AsyncSourceCoroutineWrapper::stop() const
{
    INVARIANT(std::holds_alternative<std::unique_ptr<AsyncSource>>(sourceExecutionContext.sourceImpl));
    std::get<std::unique_ptr<AsyncSource>>(sourceExecutionContext.sourceImpl)->cancel();
}

bool AsyncSourceCoroutineWrapper::handleSourceResult(IOBuffer& buffer, AsyncSource::InternalSourceResult result)
{
    const std::function dataEmit
        = [this](IOBuffer& buf) -> void { emitFn(sourceExecutionContext.originId, SourceReturnType::Data{std::move(buf)}); };

    return std::visit(
        [this, buffer, &dataEmit](auto&& result) -> bool
        {
            using T = std::decay_t<decltype(result)>;
            if constexpr (std::is_same_v<T, AsyncSource::Continue>)
            {
                /// Usual case, the source is ready to continue to ingest more data
                sourceExecutionContext.inputFormatter->parseTupleBufferRaw(
                    buffer, *sourceExecutionContext.bufferProvider, result.bytesRead, dataEmit);
                return true;
            }
            else if constexpr (std::is_same_v<T, AsyncSource::EndOfStream> || std::is_same_v<T, AsyncSource::Cancelled>)
            {
                /// The source signalled the end of the stream, i.e., because the external system closed the connection.
                /// Check for a partially filled buffer before signalling EoS and exiting
                if (result.bytesRead)
                {
                    sourceExecutionContext.inputFormatter->parseTupleBufferRaw(
                        buffer, *sourceExecutionContext.bufferProvider, result.bytesRead, dataEmit);
                }
                emitFn(sourceExecutionContext.originId, SourceReturnType::EoS{});
                return false;
            }
            else // if constexpr (std::is_same_v<T, AsyncSource::Error>)
            {
                const boost::system::system_error error = result.error;
                emitFn(sourceExecutionContext.originId, SourceReturnType::Error{Exception{std::string(error.what()), 0}});
                return false;
            }
        },
        result);
}

}
