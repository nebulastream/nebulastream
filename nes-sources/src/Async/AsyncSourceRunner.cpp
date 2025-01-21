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

#include <functional>
#include <future>
#include <memory>
#include <ostream>
#include <utility>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <Async/AsyncSourceCoroutineWrapper.hpp>
#include <Async/AsyncTaskExecutor.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceUtility.hpp>
#include <Sources/SourceRunner.hpp>

namespace NES::Sources
{

AsyncSourceRunner::AsyncSourceRunner(SourceExecutionContext sourceExecutionContext)
    : SourceRunner(sourceExecutionContext.originId)
    , executor(AsyncTaskExecutor::getOrCreate())
    , state(std::make_unique<AsyncSourceRunnerState>(Initial{.sourceContext = std::move(sourceExecutionContext)}))
{
}

bool AsyncSourceRunner::start(EmitFunction&& emitFn)
{
    return state->transition(
        [this, &emitFn](Initial&& initialState) -> Running
        {
            NES_DEBUG("AsyncSourceRunner: Initial -> Running");
            std::promise<void> terminationPromise;
            std::future<void> terminationFuture = terminationPromise.get_future();

            /// Create wrapper around the root coroutine that drives the source to completion
            auto coro = std::make_shared<AsyncSourceCoroutineWrapper>(
                std::move(initialState.sourceContext), std::move(terminationPromise), std::move(emitFn)
            );

            /// Dispatch the rootCoroutine member function to the executor where it runs on an internal I/O thread
            /// asio::detached indicates that we are not interested in the return value of the root coroutine.
            /// co_spawn tells asio to execute the given function as a coroutine on the io_context inside the executor.
            executor.dispatch<std::function<void()>>(
                [this, coro]() -> void
                { asio::co_spawn(executor.ioContext(), coro->runningRoutine(), asio::detached); });
            /// Finally: transition to the running state and store a pointer to the coroutine wrapper in the state.
            return Running{.coroutineWrapper = coro, .terminationFuture = std::move(terminationFuture)};
        });
}

bool AsyncSourceRunner::stop()
{
    NES_DEBUG("AsyncSourceRunner: Running -> Stopped");
    /// After calling stop once atomically, this runner can not be used anymore (no further transitions possible)
    /// Any attempt will fail and return false
    return state->transition(
        [this](Running&& runningState) -> Stopped
        {
            /// Stop coroutine and block until it is finished
            executor.dispatch<std::function<void()>>([coro = runningState.coroutineWrapper]() -> void
            {
                coro->cancel();
            });
            /// Block on the future to wait for the coroutine to finish
            runningState.terminationFuture.get();
            return Stopped{};
        });
}

std::ostream& AsyncSourceRunner::toString(std::ostream& str) const
{
    return str;
}

}
