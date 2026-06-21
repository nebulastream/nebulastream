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
#include <condition_variable>
#include <mutex>
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
#include <InflightThrottle.hpp>
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
    WorkEmitter& emitter,
    const bool adaptiveInflight)
{
    /// #1713: max = the configured inflight limit, so the adaptive cap never exceeds the non-adaptive baseline. When
    /// adaptive, the throttle starts low (max/4) and the AIMD policy grows it toward max on stall / decays it on idle.
    const auto maxLimit = std::min(numberOfInflightBuffers, static_cast<size_t>(std::numeric_limits<int32_t>::max()));
    const auto initialLimit = adaptiveInflight ? std::max<size_t>(1, maxLimit / 4) : maxLimit;
    auto availableBuffer = std::make_shared<InflightThrottle>(initialLimit);
    auto policy = std::make_shared<InflightPolicy>(InflightPolicy{
        .min = std::max<size_t>(1, maxLimit / 8),
        .max = maxLimit,
        .current = initialLimit,
        .additiveStep = std::max<size_t>(1, maxLimit / 8)});
    auto lastActivity = std::make_shared<std::chrono::steady_clock::time_point>(std::chrono::steady_clock::now());
    return [&controller,
            successors = std::move(successors),
            source,
            &emitter,
            queryId,
            adaptiveInflight,
            availableBuffer = std::move(availableBuffer),
            policy = std::move(policy),
            lastActivity = std::move(lastActivity)](
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
                        /// Acquire an inflight slot; acquire() returns acquired=false (without taking a slot) if the
                        /// source is asked to terminate while waiting, so there is no permit imbalance on the stop path.
                        const auto acquireResult = availableBuffer->acquire(stopToken);
                        if (not acquireResult.acquired)
                        {
                            return SourceReturnType::EmitResult::STOP_REQUESTED;
                        }
                        if (adaptiveInflight)
                        {
                            /// #1713 AIMD: grow the cap when we had to wait (saturated), decay it after an idle gap.
                            const auto now = std::chrono::steady_clock::now();
                            if (acquireResult.waited)
                            {
                                availableBuffer->setLimit(policy->onStall());
                                *lastActivity = now;
                            }
                            else if (now - *lastActivity > std::chrono::milliseconds(500))
                            {
                                availableBuffer->setLimit(policy->onIdleDecay());
                                *lastActivity = now;
                            }
                        }
                        /// The admission queue might be full, we have to reattempt
                        while (not emitter.emitWork(
                            queryId,
                            successor,
                            data.buffer,
                            TaskCallback{TaskCallback::OnComplete([availableBuffer] { availableBuffer->release(); })},
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
    const auto maxInflightBuffers = source->getRuntimeConfiguration().inflightBufferLimit;
    const auto adaptiveInflight = source->getRuntimeConfiguration().adaptiveInflight;
    auto runningSource = std::shared_ptr<RunningSource>(
        new RunningSource(successors, std::move(source), std::move(onSourceStopped), std::move(onSourceFailure)));
    ENGINE_LOG_DEBUG("Starting Running Source");
    {
        const std::scoped_lock lock(runningSource->mutex);
        runningSource->source->start(
            emitFunction(queryId, maxInflightBuffers, runningSource, std::move(successors), controller, emitter, adaptiveInflight));
    }
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
