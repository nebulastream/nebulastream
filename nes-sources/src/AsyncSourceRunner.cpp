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

#include <cstddef>
#include <memory>
#include <type_traits>
#include <variant>

#include <boost/asio/awaitable.hpp>
#include <boost/system/system_error.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/AsyncSourceExecutor.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <AsyncSourceRunner.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

namespace asio = boost::asio;


AsyncSourceRunner::AsyncSourceRunner(
    OriginId originId,
    std::shared_ptr<Memory::AbstractPoolProvider> poolProvider,
    SourceReturnType::EmitFunction&& emitFn,
    size_t numSourceLocalBuffers,
    std::unique_ptr<Source> sourceImpl,
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter,
    std::shared_ptr<AsyncSourceExecutor> executor)
    : originId(originId)
    , maxSequenceNumber(0)
    , bufferProvider(poolProvider->createFixedSizeBufferPool(numSourceLocalBuffers))
    , emitFn(std::move(emitFn))
    , sourceImpl(std::move(sourceImpl))
    , inputFormatter(std::move(inputFormatter))
    , executor(std::move(executor))
{
}


void AsyncSourceRunner::start()
{
    INVARIANT(executor != nullptr, "Executor is null");
    INVARIANT(state == RunnerState::Running, "start() called twice");

    auto self = shared_from_this();
    /// Let go of the coroutine and let the executor run it.
    executor->dispatch([self] -> asio::awaitable<void> { co_await self->coroutine(); });
    state = RunnerState::Running;
}

void AsyncSourceRunner::stop()
{
    INVARIANT(executor != nullptr, "Executor is null");
    INVARIANT(state == RunnerState::Running, "start() not called");

    state = RunnerState::Stopped;
}

asio::awaitable<void> AsyncSourceRunner::coroutine()
{
    co_await sourceImpl->open(executor->ioContext());
    while (state == RunnerState::Running)
    {
        auto buffer = bufferProvider->getBufferBlocking();

        auto result = co_await sourceImpl->fillBuffer(buffer);

        std::visit(
            [this, &buffer](auto&& result)
            {
                using T = std::decay_t<decltype(result)>;
                if constexpr (std::is_same_v<T, Source::Continue>)
                {
                    emitFn(originId, SourceReturnType::Data(std::move(buffer)));
                }
                else if constexpr (std::is_same_v<T, Source::EoS>)
                {
                    if (result.dataAvailable)
                    {
                        emitFn(originId, SourceReturnType::Data(std::move(buffer)));
                    }
                    emitFn(originId, SourceReturnType::EoS{});
                }
                else if constexpr (std::is_same_v<T, Source::Error>)
                {
                    boost::system::system_error error = result.error;
                    emitFn(originId, SourceReturnType::Error{Exception{std::string(error.what()), 0}});
                }
            },
            result);
    }
    co_await sourceImpl->close(executor->ioContext());
}

}
