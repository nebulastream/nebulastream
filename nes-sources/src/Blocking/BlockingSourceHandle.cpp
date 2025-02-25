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
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

BlockingSourceHandle::BlockingSourceHandle(SourceExecutionContext<BlockingSource> ctx)
    : SourceHandle{ctx.originId}, state{BlockingSourceState{Initial{std::move(ctx)}}}
{
}

bool BlockingSourceHandle::start(EmitFunction&& emitFn)
{
    return state.transition(
        [&emitFn](Initial&& initialState) -> Running
        {
            NES_DEBUG("BlockingSourceHandle: Initial -> Running");

            std::promise<void> terminationPromise;
            return Running{
                .terminationFuture = terminationPromise.get_future(),
                .thread = std::jthread(
                    BlockingSourceRunner{
                        std::move(terminationPromise),
                        std::move(emitFn),
                        std::make_unique<SourceExecutionContext<BlockingSource>>(std::move(initialState.sourceContext)),
                    })};
        });
}

bool BlockingSourceHandle::stop()
{
    return state.transition(
        [](Running&& runningState) -> Stopped
        {
            PRECONDITION(runningState.thread.get_id() != std::this_thread::get_id(), "stop() must not be called from the source thread.");
            NES_DEBUG("BlockingSourceHandle: Running -> Stopped");

            runningState.thread.request_stop();
            try
            {
                runningState.terminationFuture.get();
            }
            catch (const std::exception& exception)
            {
                NES_ERROR("Source encountered an error: {}", exception.what());
            }
            return Stopped{};
        });
}

std::ostream& BlockingSourceHandle::toString(std::ostream& str) const
{
    return str;
}
}