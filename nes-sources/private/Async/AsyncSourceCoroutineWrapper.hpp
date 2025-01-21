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


#include <future>

#include <boost/asio/awaitable.hpp>

#include <Sources/AsyncSource.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceUtility.hpp>

namespace NES::Sources
{

class AsyncSourceCoroutineWrapper
{
public:
    AsyncSourceCoroutineWrapper() = delete;
    explicit AsyncSourceCoroutineWrapper(
        SourceExecutionContext&& sourceExecutionContext,
        std::promise<void> terminationPromise,
        EmitFunction&& emitFn);

    ~AsyncSourceCoroutineWrapper() = default;

    AsyncSourceCoroutineWrapper(const AsyncSourceCoroutineWrapper&) = delete;
    AsyncSourceCoroutineWrapper(AsyncSourceCoroutineWrapper&&) = default;

    AsyncSourceCoroutineWrapper& operator=(const AsyncSourceCoroutineWrapper&) = delete;
    AsyncSourceCoroutineWrapper& operator=(AsyncSourceCoroutineWrapper&&) = default;

    /// The root coroutine orchestrating the control flow in a source-agnostic way.
    /// This function is only called once, from an asio-internal I/O thread.
    asio::awaitable<void> runningRoutine();

    /// Request stop of the rootCoroutine and blocks until it is finished (block on the future).
    /// Called from one of the query engine's worker threads upon a stopQuery() request.
    void cancel() const;

private:
    SourceExecutionContext sourceExecutionContext;
    std::promise<void> terminationPromise;
    EmitFunction emitFn;

    [[nodiscard]] asio::awaitable<void> open() const;
    asio::awaitable<AsyncSource::InternalSourceResult> fillBuffer(IOBuffer& buffer) const;
    void handleSourceResult(IOBuffer& buffer, AsyncSource::InternalSourceResult result, const std::function<void(IOBuffer&)>& dataEmit);
    void close() const;

};

}