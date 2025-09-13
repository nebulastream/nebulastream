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
#include <semaphore>
#include <stop_token>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Overloaded.hpp>
#include <EngineLogger.hpp>
#include <ErrorHandling.hpp>
#include <Interfaces.hpp>
#include <PipelineExecutionContext.hpp>
#include <RunningQueryPlan.hpp>

namespace NES
{

namespace
{
SourceReturnType::EmitFunction emitFunction(
    QueryId queryId,
    size_t numberOfInflightBuffers,
    std::weak_ptr<RunningSource> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    auto availableBuffer = std::make_shared<std::counting_semaphore<>>(
        std::min(numberOfInflightBuffers, static_cast<size_t>(std::numeric_limits<int32_t>::max())));
    return [&controller, successors = std::move(successors), source, &emitter, queryId, availableBuffer = std::move(availableBuffer)](
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
                            /// release the semaphore in case the source wants to terminate
                            const std::stop_callback callback(stopToken, [&]() { availableBuffer->release(); });
                            availableBuffer->acquire();
                            if (stopToken.stop_requested())
                            {
                                return SourceReturnType::EmitResult::STOP_REQUESTED;
                            }
                        }
                        /// The admission queue might be full, we have to reattempt
                        while (not emitter.emitWork(
                            queryId,
                            successor,
                            data.buffer,
                            [availableBuffer] { availableBuffer->release(); },
                            {},
                            PipelineExecutionContext::ContinuationPolicy::NEVER))
                        {
                            if (stopToken.stop_requested())
                            {
                                return SourceReturnType::EmitResult::STOP_REQUESTED;
                            }
                        }
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
    std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> tryUnregister,
    std::function<void(Exception)> unregisterWithError)
    : successors(std::move(successors))
    , source(std::move(source))
    , tryUnregister(std::move(tryUnregister))
    , unregisterWithError(std::move(unregisterWithError))
{
}

std::shared_ptr<RunningSource> RunningSource::create(
    QueryId queryId,
    std::unique_ptr<SourceHandle> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> tryUnregister,
    std::function<void(Exception)> unregisterWithError,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    const auto maxInflightBuffers = source->getRuntimeConfiguration().inflightBufferLimit;
    auto runningSource = std::shared_ptr<RunningSource>(
        new RunningSource(successors, std::move(source), std::move(tryUnregister), std::move(unregisterWithError)));
    ENGINE_LOG_DEBUG("Starting Running Source");
    runningSource->source->start(emitFunction(queryId, maxInflightBuffers, runningSource, std::move(successors), controller, emitter));
    return runningSource;
}

RunningSource::~RunningSource()
{
    if (source)
    {
        ENGINE_LOG_DEBUG("Stopping Running Source");
        if (source->tryStop(std::chrono::milliseconds(0)) == SourceReturnType::TryStopResult::TIMEOUT)
        {
            ENGINE_LOG_DEBUG("Source was requested to stop. Stop will happen asynchronously.");
        }
    }
}

bool RunningSource::attemptUnregister()
{
    if (tryUnregister(std::move(this->successors)))
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
    return this->source->tryStop(std::chrono::milliseconds(0));
}

void RunningSource::fail(Exception exception) const
{
    unregisterWithError(std::move(exception));
}

}
