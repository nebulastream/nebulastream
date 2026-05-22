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

#include <RunningSource.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <semaphore>
#include <stop_token>
#include <utility>
#include <variant>
#include <vector>
#include <ittnotify.h>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Overloaded.hpp>
#include <oneapi/tbb/profiling.h>
#include <EngineLogger.hpp>
#include <ErrorHandling.hpp>
#include <Interfaces.hpp>
#include <PipelineExecutionContext.hpp>
#include <RunningQueryPlan.hpp>
#include <scope_guard.hpp>

namespace NES
{

namespace
{
__itt_domain* sourceDomain = __itt_domain_create("engine.source");
__itt_string_handle* queueWrite = __itt_string_handle_create("Blocking Write");
__itt_string_handle* waitForInflightBuffers = __itt_string_handle_create("Wait for inflight buffers");

__itt_string_handle* asyncWriteTask = __itt_string_handle_create("Blocked Async Write");
__itt_string_handle* asyncWriteCallbackTask = __itt_string_handle_create("Blocked Async Write Completed");

[[maybe_unused]] __itt_id makeTaskId()
{
    thread_local uint64_t counter = 0;
    return __itt_id_make(reinterpret_cast<void*>(pthread_self()), counter++);
}

/// Free coroutine function so that captured state (queryId, successor, source) lives in the
/// coroutine frame rather than in a lambda closure. A coroutine *lambda*'s closure object is not
/// stored in the frame — the frame references it via the implicit `this`. If the closure owner
/// (e.g. DataHandler::emitFunction) is destroyed while an emit chain is still suspended in
/// backpressure or the admission queue, the captures become dangling.
coro::task<void> runAsyncEmit(
    QueryId queryId,
    std::shared_ptr<RunningQueryPlanNode> successor,
    std::weak_ptr<RunningSource> source,
    QueryLifetimeController& controller,
    WorkEmitter& emitter,
    OriginId sourceId,
    SourceReturnType::SourceReturnType event)
{
    __itt_task_begin(sourceDomain, __itt_null, __itt_null, asyncWriteTask);
    SCOPE_EXIT
    {
        __itt_task_end(sourceDomain);
    };
    co_await std::visit(
        Overloaded{
            [&](SourceReturnType::Data data)
            {
                return emitter.emitWorkAsync(
                    queryId,
                    std::move(successor),
                    std::move(data.buffer),
                    TaskCallback{TaskCallback::OnComplete([onComplete = std::move(data.onComplete)] { onComplete(); })});
            },
            [&](SourceReturnType::EoS) { return controller.initializeSourceStopAsync(queryId, sourceId, source); },
            [&](SourceReturnType::Error error)
            { return controller.initializeSourceFailureAsync(queryId, sourceId, source, std::move(error.ex)); }},
        std::move(event));
}

SourceReturnType::AsyncEmitFunction asyncEmit(
    QueryId queryId,
    std::weak_ptr<RunningSource> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    INVARIANT(successors.size() == 1, "I dont know how to implement that");
    auto successor = std::move(successors.at(0));

    return [&controller, successor = std::move(successor), source = std::move(source), &emitter, queryId](
               const OriginId sourceId, SourceReturnType::SourceReturnType event)
    { return runAsyncEmit(queryId, successor, source, controller, emitter, sourceId, std::move(event)); };
}

SourceReturnType::EmitFunction emitFunction(
    QueryId queryId,
    std::weak_ptr<RunningSource> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    return [&controller, successors = std::move(successors), source, &emitter, queryId](
               const OriginId sourceId,
               SourceReturnType::SourceReturnType event,
               const std::stop_token& stopToken) -> SourceReturnType::EmitResult
    {
        return std::visit(
            Overloaded{
                [&](const SourceReturnType::Data& data)
                {
                    for (const auto& successor : successors)
                    {
                        {
                            if (stopToken.stop_requested())
                            {
                                return SourceReturnType::EmitResult::STOP_REQUESTED;
                            }
                        }
                        __itt_task_begin(sourceDomain, __itt_null, __itt_null, queueWrite);
                        /// The admission queue might be full, we have to reattempt
                        while (not emitter.emitWork(
                            queryId,
                            successor,
                            data.buffer,
                            TaskCallback{TaskCallback::OnComplete([callback = std::move(data.onComplete)] { callback(); })},
                            PipelineExecutionContext::ContinuationPolicy::NEVER))
                        {
                            if (stopToken.stop_requested())
                            {
                                __itt_task_end(sourceDomain);
                                return SourceReturnType::EmitResult::STOP_REQUESTED;
                            }
                        }
                        __itt_task_end(sourceDomain);
                        ENGINE_LOG_DEBUG("Source Emitted Data to successor: {}-{}", queryId, successor->id);
                    }
                    return SourceReturnType::EmitResult::SUCCESS;
                },
                [&](SourceReturnType::EoS)
                {
                    ENGINE_LOG_DEBUG("Source with OriginId {} reached end of stream for query {}", sourceId, queryId);
                    controller.initializeSourceStop(queryId, sourceId, source);
                    return SourceReturnType::EmitResult::SUCCESS;
                },
                [&](SourceReturnType::Error error)
                {
                    controller.initializeSourceFailure(queryId, sourceId, source, std::move(error.ex));
                    return SourceReturnType::EmitResult::SUCCESS;
                }},
            std::move(event));
    };
}
}

OriginId RunningSource::getOriginId() const
{
    return source->getSourceId();
}

RunningSource::RunningSource(
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    std::unique_ptr<SourceHandle> source,
    std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> onSourceStopped,
    std::function<void(Exception)> onSourceFailure)
    : successors(std::move(successors))
    , source(std::move(source))
    , onSourceStopped(std::move(onSourceStopped))
    , onSourceFailure(std::move(onSourceFailure))
{
}

std::shared_ptr<RunningSource> RunningSource::create(
    QueryId queryId,
    std::unique_ptr<SourceHandle> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> onSourceStopped,
    std::function<void(Exception)> onSourceFailure,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    auto runningSource = std::shared_ptr<RunningSource>(
        new RunningSource(successors, std::move(source), std::move(onSourceStopped), std::move(onSourceFailure)));
    ENGINE_LOG_DEBUG("Starting Running Source");
    {
        const std::scoped_lock lock(runningSource->mutex);
        auto sync = emitFunction(queryId, runningSource, successors, controller, emitter);
        auto async = asyncEmit(std::move(queryId), runningSource, std::move(successors), controller, emitter);
        runningSource->source->start(std::move(queryId), std::move(sync), std::move(async));
    }
    return runningSource;
}

RunningSource::~RunningSource()
{
    if (source)
    {
        LogContext logContext("Source", source->getSourceId());
        ENGINE_LOG_DEBUG("Stopping Running Source");
        ///TODO: This effectivly blocks the current thread... The proper solution is to ensure that the RunningSource is
        ///      never destructed before it has been stopped. A stopping query plan may still have running sources, which
        ///      at least have been instructed to stop.

        auto backoff = std::chrono::milliseconds(1);
        for (size_t i = 0; i < 20; i++)
        {
            if (source->tryStop(std::chrono::milliseconds(0)) == SourceReturnType::TryStopResult::TIMEOUT)
            {
                NES_DEBUG("Shit not working");
                std::this_thread::sleep_for(backoff);
                backoff *= 2;
                backoff = std::min(backoff, std::chrono::milliseconds(250));
            }
            else
            {
                return;
            }
        }
        INVARIANT(false, "Source did not stop in time the source does not seem to respect the stop request");
    }
}

bool RunningSource::attemptUnregister()
{
    const auto result = tryStop();
    if (result == SourceReturnType::TryStopResult::NOT_RUNNING)
    {
        /// Source was already stopped, callback was already called
        return true;
    }
    if (result != SourceReturnType::TryStopResult::SUCCESS)
    {
        return false;
    }

    if (onSourceStopped(std::move(this->successors)))
    {
        /// Since we moved the content of the successors vector out of the successors vector above,
        /// we clear it to avoid accidentally working with null values
        this->successors.clear();
        return true;
    }
    return false;
}

SourceReturnType::TryStopResult RunningSource::tryStop() const
{
    const std::scoped_lock lock(mutex);
    return this->source->tryStop(std::chrono::milliseconds(0));
}

void RunningSource::fail(Exception exception) const
{
    onSourceFailure(std::move(exception));
}

}
