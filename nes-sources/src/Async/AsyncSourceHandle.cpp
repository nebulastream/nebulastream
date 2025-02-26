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

#include <Async/AsyncSourceHandle.hpp>

#include <future>
#include <memory>
#include <ostream>
#include <utility>

#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp>

#include <Async/IOThread.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Sources
{

AsyncSourceHandle::AsyncSourceHandle(SourceExecutionContext<AsyncSource> sourceExecutionContext)
    : SourceHandle{sourceExecutionContext.originId}, state{AsyncSourceState{Initial{std::move(sourceExecutionContext)}}}
{
}

bool AsyncSourceHandle::start(EmitFunction&& emitFn)
{
    return state.transition(
        [&emitFn](Initial&& initialState) -> Running
        {
            NES_DEBUG("Initial -> Running");
            return Running{std::move(initialState.sourceContext), std::move(emitFn)};
        });
}

bool AsyncSourceHandle::stop()
{
    /// After calling stop once atomically, this runner can not be used anymore (no further transitions possible)
    /// Any attempt will fail and return false
    return state.transition(
        [](Running&& runningState) -> Stopped
        {
            NES_DEBUG("Running -> Stopped");
            /// Emit a cancellation signal to the coroutine
            /// If the source already finished by itself, this is a no-op
            runningState.cancellationSignal->emit(asio::cancellation_type::terminal);
            /// Block on the future to wait for the coroutine to finish
            runningState.terminationFuture.wait();
            /// Transition to stopped state
            return Stopped{};
        });
}

std::ostream& AsyncSourceHandle::toString(std::ostream& str) const
{
    return str;
}

}
