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

#include <memory>

#include <boost/asio/awaitable.hpp>

#include <Sources/AsyncSource.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceReturnType.hpp>

namespace NES::Sources
{

class AsyncSourceCoroutineWrapper
{
public:
    explicit AsyncSourceCoroutineWrapper(
        SourceExecutionContext sourceExecutionContext,
        std::shared_ptr<AsyncSourceExecutor> executor,
        SourceReturnType::EmitFunction&& emitFn);

    ~AsyncSourceCoroutineWrapper() = default;

    AsyncSourceCoroutineWrapper(const AsyncSourceCoroutineWrapper&) = delete;
    AsyncSourceCoroutineWrapper(AsyncSourceCoroutineWrapper&&) = delete;
    AsyncSourceCoroutineWrapper& operator=(const AsyncSourceCoroutineWrapper&) = delete;
    AsyncSourceCoroutineWrapper& operator=(AsyncSourceCoroutineWrapper&&) = delete;

    asio::awaitable<void> start();
    void stop() const;

    asio::awaitable<void> open() const;
    asio::awaitable<AsyncSource::InternalSourceResult> AsyncSourceCoroutineWrapper::fillBuffer(IOBuffer& buffer) const;
    void close() const;

private:
    SourceExecutionContext sourceExecutionContext;
    std::shared_ptr<AsyncSourceExecutor> executor;
    SourceReturnType::EmitFunction emitFn;

    bool handleSourceResult(IOBuffer& buffer, AsyncSource::InternalSourceResult result);
};

}