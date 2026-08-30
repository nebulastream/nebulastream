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
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
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

struct DefaultPEC final : PipelineExecutionContext
{
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* operatorHandlers = nullptr;
    std::function<bool(const TupleBuffer& tb, ContinuationPolicy)> handler;
    std::function<void(const TupleBuffer& tb, std::chrono::milliseconds duration)> repeatHandler;
    std::shared_ptr<AbstractBufferProvider> bm;
    size_t numberOfThreads;
    WorkerThreadId threadId;
    PipelineId pipelineId;
    /// We want to ensure that the address of the TupleBuffer is always the same. If we would simply store the object directly in the vector,
    /// the address might change as the vector might be resized and thus, the object have a different address.
    std::vector<std::unique_ptr<TupleBuffer>> pinnedBuffers;

#ifndef NO_ASSERT
    bool wasRepeated = false;
#endif

    DefaultPEC(
        size_t numberOfThreads,
        WorkerThreadId threadId,
        PipelineId pipelineId,
        std::shared_ptr<AbstractBufferProvider> bm,
        std::function<bool(const TupleBuffer& tb, ContinuationPolicy)> handler,
        std::function<void(const TupleBuffer& tb, std::chrono::milliseconds)> repeatHandler)
        : handler(std::move(handler))
        , repeatHandler(std::move(repeatHandler))
        , bm(std::move(bm))
        , numberOfThreads(numberOfThreads)
        , threadId(threadId)
        , pipelineId(pipelineId)
    {
    }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override
    {
        PRECONDITION(!wasRepeated, "A task should terminate after repeating");
        return threadId;
    }

    TupleBuffer allocateTupleBuffer() override
    {
        PRECONDITION(!wasRepeated, "A task should terminate after repeating");
        return bm->getBufferBlocking();
    }

    TupleBuffer& pinBuffer(TupleBuffer&& tupleBuffer) override
    {
        PRECONDITION(!wasRepeated, "A task should terminate after repeating");
        pinnedBuffers.emplace_back(std::make_unique<TupleBuffer>(tupleBuffer));
        return *pinnedBuffers.back();
    }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override
    {
        PRECONDITION(!wasRepeated, "A task should terminate after repeating");
        return numberOfThreads;
    }

    bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy policy) override
    {
        PRECONDITION(!wasRepeated, "A task should terminate after repeating");
        return handler(buffer, policy);
    }

    void repeatTask(const TupleBuffer& buffer, std::chrono::milliseconds duration) override
    {
        PRECONDITION(!wasRepeated, "A task should terminate after repeating");
#ifndef NO_ASSERT
        wasRepeated = true;
#endif

        repeatHandler(buffer, duration);
    }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override
    {
        PRECONDITION(!wasRepeated, "A task should terminate after repeating");
        return bm;
    }

    [[nodiscard]] PipelineId getPipelineId() const override
    {
        PRECONDITION(!wasRepeated, "A task should terminate after repeating");
        return pipelineId;
    }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override
    {
        PRECONDITION(operatorHandlers, "OperatorHandlers were not set");
        PRECONDITION(!wasRepeated, "A task should terminate after repeating");
        return *operatorHandlers;
    }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& handlers) override
    {
        PRECONDITION(!wasRepeated, "A task should terminate after repeating");
        operatorHandlers = std::addressof(handlers);
    }
};

namespace
{
/// Source-emit batch size (env NES_SOURCE_EMIT_BATCH, default 1 = publish every buffer immediately == the pre-batching
/// behavior). With value K>1 the source queues up to K per-buffer tasks into the admission queue unpublished, then wakes
/// K workers with a single counting_semaphore::release(K) -- amortizing the per-buffer task-admission machinery and
/// creating the burst worker concurrency the SequenceShredder needs to contend. Read once (a source stream property).
size_t sourceEmitBatchSize()
{
    static const size_t value = []
    {
        const char* env = std::getenv("NES_SOURCE_EMIT_BATCH");
        return std::max<size_t>(1, env != nullptr ? std::strtoul(env, nullptr, 10) : 1);
    }();
    return value;
}

/// Per-source accumulator for batched task admission (see WorkEmitter::emitWorkUnpublished). The source queues up to
/// `capacity` tasks WITHOUT publishing them, then flush() releases all of their permits in one burst. `pending` is only
/// ever touched by the single source thread. The destructor is the safety net: every queued task already incremented
/// its node's pendingTasks at build time, so if the source thread exits without an explicit flush (e.g. an external stop
/// before end-of-stream skips the EoS handler) the tail MUST still be published -- otherwise a graceful pipeline stop
/// would wait on pendingTasks forever. flush() is idempotent and a no-op when pending == 0.
struct SourceEmitBatch
{
    WorkEmitter& emitter;
    size_t capacity;
    size_t pending{0};

    SourceEmitBatch(WorkEmitter& emitter, size_t capacity) : emitter(emitter), capacity(capacity) { }

    void flush()
    {
        if (pending > 0)
        {
            emitter.publishAdmittedWork(pending);
            pending = 0;
        }
    }

    ~SourceEmitBatch() { flush(); }
};

SourceReturnType::EmitFunction emitFunction(
    QueryId queryId,
    size_t numberOfInflightBuffers,
    bool preFillsBuffers,
    std::weak_ptr<RunningSource> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    auto availableBuffer = std::make_shared<std::counting_semaphore<>>(
        std::min(numberOfInflightBuffers, static_cast<size_t>(std::numeric_limits<int32_t>::max())));
    /// Source-emit batching is gated on prefill sources only (see SourceRuntimeConfiguration::preFillsBuffers): they
    /// deliver buffers saturated and drain-then-EoS, so a partial batch is always resolved by the EoS/destructor flush.
    /// A non-prefill / slow source stays at capacity 1 (publish per buffer) so a consumer never waits on an unpublished
    /// partial batch.
    const size_t batchCapacity = preFillsBuffers ? sourceEmitBatchSize() : 1;
    auto batch = std::make_shared<SourceEmitBatch>(emitter, batchCapacity);
    return [&controller,
            successors = std::move(successors),
            source,
            &emitter,
            queryId,
            availableBuffer = std::move(availableBuffer),
            batch = std::move(batch)](const OriginId sourceId, SourceReturnType::SourceReturnType event, const std::stop_token& stopToken)
               -> SourceReturnType::EmitResult
    {
        return std::visit(
            Overloaded{
                [&](const SourceReturnType::Execute& executeData)
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
                        DefaultPEC pec(
                            1, // number of threads
                            WorkerThreadId{static_cast<uint32_t>(executeData.originId.getRawValue())},
                            successor->id,
                            executeData.bufferProvider,
                            [&](const TupleBuffer& tupleBuffer, PipelineExecutionContext::ContinuationPolicy) //continuationPolicy
                            {
                                ENGINE_LOG_DEBUG(
                                    "Task emitted tuple buffer {}-{}. Tuples: {}",
                                    task.queryId,
                                    task.pipelineId,
                                    tupleBuffer.getNumberOfTuples());
                                /// Emit all subsequent buffers into the admission / task queue
                                return std::ranges::all_of(
                                    successor->successors,
                                    [&](const auto& successor)
                                    {
                                        {
                                            /// release the semaphore in case the source wants to terminate
                                            const std::stop_callback callback(stopToken, [&]() { availableBuffer->release(); });
                                            availableBuffer->acquire();
                                            if (stopToken.stop_requested())
                                            {
                                                return false;
                                            }
                                        }
                                        /// The admission queue might be full, we have to reattempt
                                        while (not emitter.emitWork(
                                            queryId,
                                            successor,
                                            tupleBuffer,
                                            TaskCallback{TaskCallback::OnComplete([availableBuffer] { availableBuffer->release(); })},
                                            PipelineExecutionContext::ContinuationPolicy::NEVER))
                                        {
                                            if (stopToken.stop_requested())
                                            {
                                                return false;
                                            }
                                        }
                                        ENGINE_LOG_DEBUG("Source Emitted Data to successor: {}-{}", queryId, successor->id);
                                        return true;
                                    });
                            },
                            [&](const TupleBuffer&, std::chrono::milliseconds)
                            { throw SkippingDelayedTaskDuringShutdown("Cannot execute delayed task as follow-up to source"); }

                        );
                        successor->stage->execute(executeData.buffer, pec);
                        availableBuffer->release();
                    }
                    return SourceReturnType::EmitResult::SUCCESS;
                },
                [&](const SourceReturnType::Data& data)
                {
                    for (const auto& successor : successors)
                    {
                        /// Backpressure: reserve an in-flight slot. Fast path is a non-blocking try_acquire that
                        /// needs no per-buffer stop_callback. On the slow path (pool momentarily exhausted) we FIRST
                        /// publish the accumulated-but-unreleased batch so those tasks can run and return their slots
                        /// -- otherwise we could block forever waiting for a slot that only frees once an unpublished
                        /// task completes -- then block on acquire with the stop_callback that releases on stop.
                        if (!availableBuffer->try_acquire())
                        {
                            batch->flush();
                            const std::stop_callback callback(stopToken, [&]() { availableBuffer->release(); });
                            availableBuffer->acquire();
                            if (stopToken.stop_requested())
                            {
                                return SourceReturnType::EmitResult::STOP_REQUESTED;
                            }
                        }
                        /// Queue the task WITHOUT publishing it; the whole batch is published with a single burst
                        /// release once it fills (below), at end-of-stream, or in the batch destructor.
                        /// emitWorkUnpublished blocks internally until the admission write succeeds (the admission
                        /// queue is far larger than the in-flight limit), so it does not return false here.
                        emitter.emitWorkUnpublished(
                            queryId,
                            successor,
                            data.buffer,
                            TaskCallback{TaskCallback::OnComplete([availableBuffer] { availableBuffer->release(); })});
                        if (++batch->pending >= batch->capacity)
                        {
                            batch->flush();
                        }
                        ENGINE_LOG_DEBUG("Source Emitted Data to successor: {}-{}", queryId, successor->id);
                    }
                    return SourceReturnType::EmitResult::SUCCESS;
                },
                [&](SourceReturnType::EoS)
                {
                    /// Publish the tail before the stop task so all data tasks are admitted ahead of it.
                    batch->flush();
                    ENGINE_LOG_DEBUG("Source with OriginId {} reached end of stream for query {}", sourceId, queryId);
                    controller.initializeSourceStop(queryId, sourceId, source);
                    return SourceReturnType::EmitResult::SUCCESS;
                },
                [&](SourceReturnType::Error error)
                {
                    batch->flush();
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
    const auto preFillsBuffers = source->getRuntimeConfiguration().preFillsBuffers;
    auto runningSource = std::shared_ptr<RunningSource>(
        new RunningSource(successors, std::move(source), std::move(onSourceStopped), std::move(onSourceFailure)));
    ENGINE_LOG_DEBUG("Starting Running Source");
    {
        const std::scoped_lock lock(runningSource->mutex);
        runningSource->source->start(
            emitFunction(queryId, maxInflightBuffers, preFillsBuffers, runningSource, std::move(successors), controller, emitter));
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
