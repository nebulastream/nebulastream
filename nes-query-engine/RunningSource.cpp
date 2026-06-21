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
#include <PipelineExecutionContext.hpp>
#include <RunningQueryPlan.hpp>

namespace NES
{

namespace
{
/// #1713: bounded counter with a dynamically adjustable limit, replacing std::counting_semaphore as the per-source
/// inflight-buffer throttle so the cap can later grow/shrink at runtime (adaptive provisioning). Stage 1 keeps the
/// limit fixed, so behaviour is identical to the semaphore. condition_variable_any is required to compose with
/// std::stop_callback (std::condition_variable cannot).
class InflightThrottle
{
public:
    explicit InflightThrottle(const std::size_t initialLimit) : limit(initialLimit) { }

    /// Blocks until inUse < limit OR the stop_token is signalled. Returns true if a slot was acquired (inUse
    /// incremented); false if it returned due to stop (inUse NOT incremented -- the caller must not release()).
    [[nodiscard]] bool acquire(const std::stop_token& token)
    {
        std::unique_lock lock(mutex);
        const std::stop_callback callback(token, [this] { cv.notify_all(); });
        cv.wait(lock, [&] { return inUse < limit || token.stop_requested(); });
        if (token.stop_requested())
        {
            return false;
        }
        ++inUse;
        return true;
    }

    /// Returns one slot. Safe to call from any thread (e.g. the task-completion callback).
    void release()
    {
        bool notify = false;
        {
            const std::lock_guard lock(mutex);
            if (inUse > 0)
            {
                --inUse;
            }
            notify = inUse < limit;
        }
        if (notify)
        {
            cv.notify_one();
        }
    }

    /// Adjust the cap (Stage 2 AIMD will call this; Stage 1 never does). Growing wakes waiters; shrinking below inUse
    /// simply blocks new acquires until releases drain it -- in-flight buffers are never forcibly reclaimed.
    void setLimit(const std::size_t newLimit)
    {
        bool grew = false;
        {
            const std::lock_guard lock(mutex);
            grew = newLimit > limit;
            limit = newLimit;
        }
        if (grew)
        {
            cv.notify_all();
        }
    }

    [[nodiscard]] std::size_t currentLimit() const
    {
        const std::lock_guard lock(mutex);
        return limit;
    }

private:
    mutable std::mutex mutex;
    std::condition_variable_any cv;
    std::size_t inUse = 0;
    std::size_t limit;
};

SourceReturnType::EmitFunction emitFunction(
    QueryId queryId,
    size_t numberOfInflightBuffers,
    std::weak_ptr<RunningSource> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    auto availableBuffer = std::make_shared<InflightThrottle>(
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
                        /// Acquire an inflight slot; acquire() returns false (without taking a slot) if the source is
                        /// asked to terminate while waiting, so there is no permit imbalance on the stop path.
                        if (not availableBuffer->acquire(stopToken))
                        {
                            return SourceReturnType::EmitResult::STOP_REQUESTED;
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
    auto runningSource = std::shared_ptr<RunningSource>(
        new RunningSource(successors, std::move(source), std::move(onSourceStopped), std::move(onSourceFailure)));
    ENGINE_LOG_DEBUG("Starting Running Source");
    {
        const std::scoped_lock lock(runningSource->mutex);
        runningSource->source->start(emitFunction(queryId, maxInflightBuffers, runningSource, std::move(successors), controller, emitter));
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
