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

#include <chrono>
#include <functional>
#include <memory>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/Overloaded.hpp>
#include <EngineLogger.hpp>
#include <ErrorHandling.hpp>
#include <Interfaces.hpp>
#include <PipelineExecutionContext.hpp>
#include <RunningQueryPlan.hpp>

#include "PipelineExecutionContext.hpp"

namespace NES
{


struct DefaultSourcePEC final : PipelineExecutionContext
{
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* operatorHandlers = nullptr;
    std::shared_ptr<Memory::AbstractBufferProvider> bm;
    size_t numberOfThreads;
    QueryId queryId;
    PipelineId pipelineId;
    std::function<bool(const Memory::TupleBuffer& tb, ContinuationPolicy)> handler;

    DefaultSourcePEC(
        QueryId queryId,
        std::shared_ptr<RunningQueryPlanNode> inputFormatterPipeline,
        std::shared_ptr<Memory::AbstractBufferProvider> bm,
        WorkEmitter& emitter)
        : bm(std::move(bm)), numberOfThreads(1), queryId(queryId), pipelineId(inputFormatterPipeline->id)
    {
        this->handler = [&](const Memory::TupleBuffer& tupleBuffer, auto continuationPolicy)
        {
            ENGINE_LOG_DEBUG(
                "Task emitted tuple buffer {}-{}. Tuples: {}", task.queryId, task.pipelineId, tupleBuffer.getNumberOfTuples());
            /// If the current WorkTask is a 'repeat' task, re-emit the same tuple buffer and the same pipeline as a WorkTask.
            INVARIANT(continuationPolicy != PipelineExecutionContext::ContinuationPolicy::REPEAT, "A single threaded source must never repeat a task");
            /// Otherwise, get the successor of the pipeline, and emit a work task for it.
            return std::ranges::all_of(
                inputFormatterPipeline->successors,
                [&](const auto& successor)
                {
                    return emitter.emitWork(queryId, successor, tupleBuffer, {}, {}, continuationPolicy);
                });
        };
    }

    /// The BlockingSourceRunnerThread is not a valid worker thread
    [[nodiscard]] WorkerThreadId getId() const override { return INVALID<WorkerThreadId>; };

    Memory::TupleBuffer allocateTupleBuffer() override { return bm->getBufferBlocking(); }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return numberOfThreads; }

    bool emitBuffer(const Memory::TupleBuffer& buffer, ContinuationPolicy policy) override { return handler(buffer, policy); }

    std::shared_ptr<Memory::AbstractBufferProvider> getBufferManager() const override { return bm; }

    PipelineId getPipelineId() const override { return pipelineId; }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override
    {
        PRECONDITION(operatorHandlers, "OperatorHandlers were not set");
        return *operatorHandlers;
    }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& handlers) override
    {
        operatorHandlers = std::addressof(handlers);
    }
};

namespace
{
Sources::EmitFunction emitFunction(
    QueryId queryId,
    std::weak_ptr<RunningSource> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    return [&controller, successors = std::move(successors), source, &emitter, queryId](
               const OriginId sourceId, Sources::SourceReturnType event)
    {
        std::visit(
            Overloaded{
                [&](const Sources::InPlaceData& inPlaceData)
                {
                    // Todo: think of a better way to place the 'bufferProvider'
                    // -> currently we pass the sources' buffer provider, which is a fixed-size buffer pool
                    // -> this can easily lead to a deadlock (window build in InputFormatterPipeline that requires > 64 buffers)
                    DefaultSourcePEC test{queryId, successors.front(), inPlaceData.bufferProvider, emitter};
                    successors.front()->stage->execute(inPlaceData.buffer, test);
                },
                [&](const Sources::Data& data)
                {
                    for (const auto& successor : successors)
                    {
                        /// The admission queue might be full, we have to reattempt
                        while (not emitter.emitWork(
                            queryId, successor, data.buffer, {}, {}, PipelineExecutionContext::ContinuationPolicy::NEVER))
                        {
                        }
                        ENGINE_LOG_DEBUG("Source Emitted Data to successor: {}-{}", queryId, successor->id);
                    }
                },
                [&](Sources::EoS)
                {
                    ENGINE_LOG_DEBUG("Source with OriginId {} reached end of stream for query {}", sourceId, queryId);
                    controller.initializeSourceStop(queryId, sourceId, source);
                },
                [&](Sources::Error error)
                { controller.initializeSourceFailure(queryId, sourceId, source, std::move(error.ex)); }},
            std::move(event));
    };
}
}

OriginId RunningSource::getOriginId() const
{
    return source->getOriginId();
}

RunningSource::RunningSource(
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    std::unique_ptr<Sources::SourceHandle> source,
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
    std::unique_ptr<Sources::SourceHandle> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    std::function<bool(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> tryUnregister,
    std::function<void(Exception)> unregisterWithError,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    auto runningSource = std::shared_ptr<RunningSource>(
        new RunningSource(successors, std::move(source), std::move(tryUnregister), std::move(unregisterWithError)));
    // Todo: ideally successors contains 'InputFormatterTaskPipeline' as first successor here, if 'blocking'
    ENGINE_LOG_DEBUG("Starting Running Source");
    runningSource->source->start(emitFunction(queryId, runningSource, std::move(successors), controller, emitter));
    return runningSource;
}

RunningSource::~RunningSource()
{
    if (source)
    {
        ENGINE_LOG_DEBUG("Stopping Running Source");
        if (source->tryStop(std::chrono::milliseconds(0)) == Sources::TryStopResult::TIMEOUT)
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

Sources::TryStopResult RunningSource::tryStop() const
{
    return this->source->tryStop(std::chrono::milliseconds(0));
}

void RunningSource::fail(Exception exception) const
{
    unregisterWithError(std::move(exception));
}

}
