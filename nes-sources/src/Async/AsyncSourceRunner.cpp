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

#include <format>
#include <memory>
#include <ostream>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <Async/AsyncSourceCoroutineWrapper.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Sources/SourceRunner.hpp>
#include "Async/AsyncSourceExecutor.hpp"

namespace NES::Sources
{

AsyncSourceRunner::AsyncSourceRunner(SourceExecutionContext sourceExecutionContext)
    : SourceRunner(sourceExecutionContext.originId)
    , state(std::make_unique<SourceRunnerState>(Initial{.sourceContext = std::move(sourceExecutionContext)}))
{
}

bool AsyncSourceRunner::start(SourceReturnType::EmitFunction&& emitFn)
{
    return state->transition(
        [emitFn](Initial&& initialState) -> Running
        {
            /// Instantiate or retrieve a pointer to the executor
            const auto executor = AsyncSourceExecutor::getOrCreate();
            /// We create a shared_ptr to the coroutine wrapper to keep it alive for long enough.
            /// We have two possible scenarios with regards to lifetime:
            /// 1. The source finishes execution with an EoS or an error.
            /// In this case, the runner (and therefore data such as the execution context) outlives the coroutine wrapper.
            /// After propagating this information to the query engine, we just wait to be destroyed.
            /// 2. The source is stopped from the query engine (via the stop() member function).
            /// In this case, there might be asynchronous handlers (the coroutines in the wrapper or asios internal coroutines) queued or even running.
            /// The stop() member function on this runner does an asynchronous stop.
            /// Internally, the coroutine will invoke the cancel() method on the underlying async source, which in turn will cancel its I/O object.
            /// After this, stop returns and the runner might be destroyed.
            /// To ensure that all queued or running handlers can finish gracefully and emit final EoS and data events to the query engine,
            /// the coroutine wrapper must be kept alive until all handlers have finished, which we do by passing a copy of the shared pointer
            /// into the coroutine.
            auto coroutineWrapper = std::make_shared<AsyncSourceCoroutineWrapper>(std::move(initialState.sourceContext), executor, std::move(emitFn));
            executor->execute(
                [&executor, coroutineWrapper]() -> void
                { asio::co_spawn(executor->ioContext(), coroutineWrapper->start(), asio::detached); });
            return Running{.coroutineWrapper = coroutineWrapper};
        });
}

bool AsyncSourceRunner::stop()
{
    return state->transition(
        [](Running&& runningState) -> Stopped
        {
            /// Stop coroutine
            runningState.coroutineWrapper->stop();
            return Stopped{};
        });
}

std::ostream& AsyncSourceRunner::toString(std::ostream& str) const
{
    str << std::format("\nAsyncSourceRunner(state: {})", *this->state);
    return str;
}

}
