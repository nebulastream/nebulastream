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

#include <functional>
#include <future>

#include <boost/asio/awaitable.hpp>

#include <Async/IOThread.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceUtility.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

class AsyncSourceRunner
{
public:
    AsyncSourceRunner() = delete;
    explicit AsyncSourceRunner(SourceExecutionContext<AsyncSource> sourceExecutionContext, EmitFunction&& emitFn);

    ~AsyncSourceRunner() = default;

    AsyncSourceRunner(const AsyncSourceRunner&) = delete;
    AsyncSourceRunner(AsyncSourceRunner&&) = delete;

    AsyncSourceRunner& operator=(const AsyncSourceRunner&) = delete;
    AsyncSourceRunner& operator=(AsyncSourceRunner&&) = delete;

    /// The root coroutine orchestrating the control flow in a source-agnostic way.
    /// This function is only called once, from an asio-internal I/O thread.
    [[nodiscard]] asio::awaitable<void, Executor> runningRoutine() const;

private:
    SourceExecutionContext<AsyncSource> context;
    EmitFunction emitFn;

    void
    handleSourceResult(const IOBuffer& buffer, const AsyncSource::InternalSourceResult& internalSourceResult, const std::function<void(IOBuffer&)>& dataEmit) const;

    struct AsyncSourceWrapper
    {
        ~AsyncSourceWrapper()
        {
            /// Throwing in a destructor would terminate the application
            try
            {
                source.close();
            }
            catch (...)
            {
                tryLogCurrentException();
            }
        }

        AsyncSource& source;
    };
};

}
