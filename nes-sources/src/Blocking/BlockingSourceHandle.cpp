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

#include <Blocking/BlockingSourceHandle.hpp>

#include <exception>
#include <future>
#include <memory>
#include <ostream>
#include <thread>
#include <utility>

#include <Blocking/BlockingSourceRunner.hpp>
// #include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

BlockingSourceHandle::BlockingSourceHandle(
    BackpressureListener backpressureListener,
    OriginId originId,
    SourceRuntimeConfiguration configuration,
    std::shared_ptr<AbstractBufferProvider> bufferPool,
    std::unique_ptr<BlockingSource> sourceImplementation)
    : SourceHandle(configuration, originId)
{
    this->sourceThread = std::make_unique<BlockingSourceRunner>(
        std::move(backpressureListener), std::move(originId), std::move(bufferPool), std::move(sourceImplementation));
}

bool BlockingSourceHandle::start(SourceReturnType::EmitFunction&& emitFn)
{
    return this->sourceThread->start(std::move(emitFn));
    // return state.transition(
    //     [&emitFn](Initial&& initialState) -> Running
    //     {
    //         NES_DEBUG("BlockingSourceHandle: Initial -> Running");
    //         std::promise<void> terminationPromise;
    //            return Running{
    //             .terminationFuture = terminationPromise.get_future(),
    //             .thread = std::jthread(
    //                 BlockingSourceRunner{
    //                     std::move(terminationPromise),
    //                     std::move(emitFn),
    //                     std::make_unique<SourceExecutionContext<BlockingSource>>(std::move(initialState.sourceContext)),
    //                 })};
    //     });
}

void BlockingSourceHandle::stop()
{
    this->sourceThread->stop();
    // return state.transition(
    //     [](Running&& runningState) -> Stopped
    //     {
    //         PRECONDITION(runningState.thread.get_id() != std::this_thread::get_id(), "stop() must not be called from the source thread.");
    //         NES_DEBUG("BlockingSourceHandle: Running -> Stopped");
    //
    //         runningState.thread.request_stop();
    //         try
    //         {
    //             runningState.terminationFuture.get();
    //         }
    //         catch (const std::exception& exception)
    //         {
    //             NES_ERROR("Source encountered an error: {}", exception.what());
    //         }
    //         return Stopped{};
    //     });
}

SourceReturnType::TryStopResult BlockingSourceHandle::tryStop(const std::chrono::milliseconds timeout)
{
    return this->sourceThread->tryStop(timeout);
    // const auto deadline = std::chrono::system_clock::now() + timeout;
    // bool successfullyStopped = stop();
    // while (not successfullyStopped and std::chrono::system_clock::now() < deadline)
    // {
    //     successfullyStopped = stop();
    // }
    // return (successfullyStopped) ? TryStopResult::SUCCESS : TryStopResult::TIMEOUT;
}

std::ostream& BlockingSourceHandle::toString(std::ostream& str) const
{
    return str;
}
}