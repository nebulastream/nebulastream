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
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

AsyncSourceHandle::AsyncSourceHandle(
    BackpressureListener,
    OriginId originId, /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
    SourceRuntimeConfiguration configuration,
    std::shared_ptr<AbstractBufferProvider> bufferPool,
    std::unique_ptr<AsyncSource> sourceImplementation,
    const bool pinThreads,
    const size_t numberOfIOThreads,
    const size_t ioSlot)
    : SourceHandle(configuration, originId)
    , state{AsyncSourceState{Initial{std::move(sourceImplementation), pinThreads, numberOfIOThreads, ioSlot}}}
    , bufferPool(std::move(bufferPool))
{
}

bool AsyncSourceHandle::start(SourceReturnType::EmitFunction&& emitFn)
{
    return state.transition(
        [&emitFn, this](Initial&& initialState) -> Running
        {
            NES_DEBUG("Initial -> Running");
            return Running{
                std::move(initialState.sourceContext),
                std::move(emitFn),
                getSourceId(),
                this->bufferPool,
                initialState.pinThreads,
                initialState.numberOfIOThreads,
                initialState.ioSlot};
        });
}

bool AsyncSourceHandle::internalStop()
{
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

void AsyncSourceHandle::stop()
{
    /// After calling stop once atomically, this runner can not be used anymore (no further transitions possible)
    /// Any attempt will fail and return false
    internalStop();
}

SourceReturnType::TryStopResult AsyncSourceHandle::tryStop(const std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::system_clock::now() + timeout;
    bool successfullyStopped = internalStop();
    while (not successfullyStopped and std::chrono::system_clock::now() < deadline)
    {
        successfullyStopped = internalStop();
    }
    return (successfullyStopped) ? SourceReturnType::TryStopResult::SUCCESS : SourceReturnType::TryStopResult::TIMEOUT;
}

std::ostream& AsyncSourceHandle::toString(std::ostream& str) const
{
    return str;
}

}
