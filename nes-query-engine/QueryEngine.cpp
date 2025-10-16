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

#include <QueryEngine.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/AbstractQueryStatusListener.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/AtomicState.hpp>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <DelayedTaskSubmitter.hpp>
#include <EngineLogger.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <ExecutableQueryPlan.hpp>
#include <Interfaces.hpp>
#include <PipelineExecutionContext.hpp>
#include <QueryEngineConfiguration.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <RunningQueryPlan.hpp>
#include <Task.hpp>
#include <TaskQueue.hpp>
#include <Thread.hpp>

namespace NES
{

namespace
{

/// Graceful pipeline shutdown can only happen if no task depends on the pipeline anymore.
/// It could happen that tasks are waiting within the admission queue and do not get a chance to execute as long as the
/// PendingPipelineStop task is repeatedly added to the internal queue.
/// This backoff interval is used when a pending pipeline stop has repeatedly (PIPELINE_STOP_BACKOFF_THRESHOLD) failed to allow pending
/// tasks to make progress.
constexpr auto PIPELINE_STOP_BACKOFF_INTERVAL = std::chrono::milliseconds(25);
constexpr auto PIPELINE_STOP_BACKOFF_THRESHOLD = 2;

/// This function is unsafe because it requires the lifetime of the RunningQueryPlanNode exceed the lifetime of the callback
auto injectQueryFailureUnsafe(RunningQueryPlanNode& node, TaskCallback::onFailure failure)
{
    return [failure = std::move(failure), &node](Exception exception) mutable
    {
        if (failure)
        {
            failure(exception);
        }
        node.fail(std::move(exception));
    };
}

auto injectQueryFailure(std::weak_ptr<RunningQueryPlanNode> node, TaskCallback::onFailure failure)
{
    return [failure = std::move(failure), node = std::move(node)](Exception exception) mutable
    {
        const auto strongReference = node.lock();
        if (!strongReference)
        {
            ENGINE_LOG_ERROR(
                "Query Failure could not be reported as query has already been terminated. Original Error: {}", exception.what());
            return;
        }

        if (failure)
        {
            failure(exception);
        }
        strongReference->fail(exception);
    };
}

auto injectReferenceCountReducer(
    ENGINE_IF_LOG_DEBUG(LocalQueryId qid, ) std::weak_ptr<RunningQueryPlanNode> node, TaskCallback::onComplete innerFunction)
{
    return [ENGINE_IF_LOG_DEBUG(qid, ) innerFunction = std::move(innerFunction), node = std::weak_ptr(std::move(node))]() mutable
    {
        if (innerFunction)
        {
            innerFunction();
        }
        if (auto existingNode = node.lock())
        {
            auto updatedCount = existingNode->pendingTasks.fetch_sub(1) - 1;
            ENGINE_LOG_DEBUG("Decreasing number of pending tasks on pipeline {}-{} to {}", qid, existingNode->id, updatedCount);
            INVARIANT(updatedCount >= 0, "ThreadPool returned a negative number of pending tasks.");
        }
        else
        {
            ENGINE_LOG_WARNING("Node Expired and pendingTasks could not be reduced");
        }
    };
}

}

/// The Query has not been started yet. But a slot in the QueryCatalog has been reserved.
struct Reserved
{
};

/// The ExecutableQueryPlan moved into a RunningQueryPlan.
/// Pipelines and Sources in the RunningQueryPlan have been scheduled to be initialized
/// Once all initialization is done the query transitions into the running state.
/// If the Query is stopped during initialization, the running query plan is dropped. Which causes all initialized pipelines
/// to be terminated and pending initializations to be skipped. The query is moved directly into the stopping state.
/// Failures during initialization will drop the running query plan and transition into the failed state.
struct Starting
{
    std::unique_ptr<RunningQueryPlan> plan;
};

struct Running
{
    std::unique_ptr<RunningQueryPlan> plan;
};

/// If the running query plan is dropped:
/// 1. All sources are stopped, via the RunningSource in the sources vector
/// 2. Dropping all sources will drop the reference count to all pipelines
/// 3. During the drop of the pipeline termination tasks will be emitted into the pipeline
/// 4. Once all terminations are done the callback will be invoked which moves this into the Idle state
struct Stopping
{
    std::unique_ptr<StoppingQueryPlan> plan;
};

struct Terminated
{
    enum TerminationReason
    {
        Failed,
        Stopped
    };

    TerminationReason reason;
};

class QueryCatalog
{
public:
    using State = std::shared_ptr<AtomicState<Reserved, Starting, Running, Stopping, Terminated>>;
    using WeakStateRef = State::weak_type;
    using StateRef = State::element_type;

    void start(
        LocalQueryId queryId,
        std::unique_ptr<ExecutableQueryPlan> plan,
        const std::shared_ptr<AbstractQueryStatusListener>& listener,
        const std::shared_ptr<QueryEngineStatisticListener>& statistic,
        QueryLifetimeController& controller,
        WorkEmitter& emitter);
    void stopQuery(LocalQueryId queryId);

    void clear()
    {
        const std::scoped_lock lock(mutex);
        queryStates.clear();
    }

private:
    std::recursive_mutex mutex;
    std::unordered_map<LocalQueryId, State> queryStates;
};

namespace detail
{
using Queue = folly::MPMCQueue<Task>;
}

struct DefaultPEC final : PipelineExecutionContext
{
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* operatorHandlers = nullptr;
    std::function<bool(const TupleBuffer& tb, ContinuationPolicy)> handler;
    std::function<void(const TupleBuffer& tb, std::chrono::milliseconds duration)> repeatHandler;
    std::shared_ptr<AbstractBufferProvider> bm;
    size_t numberOfThreads;
    WorkerThreadId threadId;
    PipelineId pipelineId;

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

    [[nodiscard]] WorkerThreadId getId() const override
    {
        PRECONDITION(!wasRepeated, "A task should terminate after repeating");
        return threadId;
    }

    TupleBuffer allocateTupleBuffer() override
    {
        PRECONDITION(!wasRepeated, "A task should terminate after repeating");
        return bm->getBufferBlocking();
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

/// Lifetime of the ThreadPool:
/// - ThreadPool is owned by the QueryEngine
/// - ThreadPool owns the TaskQueue.
///     - As long as any thread is alive the TaskQueue Needs to Exist
/// - ThreadPool has to outlive all Queries
class ThreadPool : public WorkEmitter, public QueryLifetimeController
{
public:
    void addThread(WorkerId workerId);

    bool emitWork(
        LocalQueryId qid,
        const std::shared_ptr<RunningQueryPlanNode>& node,
        TupleBuffer buffer,
        TaskCallback callback,
        const PipelineExecutionContext::ContinuationPolicy continuationPolicy) override
    {
        [[maybe_unused]] auto updatedCount = node->pendingTasks.fetch_add(1) + 1;
        ENGINE_LOG_DEBUG("Increasing number of pending tasks on pipeline {}-{} to {}", qid, node->id, updatedCount);
        auto [complete, failure, success] = std::move(callback).take();
        /// Create a new callback that wraps the reference count reducer
        auto wrappedCallback = TaskCallback{
            TaskCallback::OnComplete(injectReferenceCountReducer(ENGINE_IF_LOG_DEBUG(qid, ) node, std::move(complete.callback))),
            std::move(success),
            TaskCallback::OnFailure(injectQueryFailure(node, std::move(failure.callback))),
        };

        auto task = WorkTask(qid, node->id, node, std::move(buffer), std::move(wrappedCallback));
        if (WorkerThread::id == INVALID<WorkerThreadId>)
        {
            /// Non-WorkerThread
            taskQueue.addAdmissionTaskBlocking({}, std::move(task));
            ENGINE_LOG_DEBUG("Task written to AdmissionQueue");
            return true;
        }

        /// WorkerThread
        switch (continuationPolicy)
        {
            case PipelineExecutionContext::ContinuationPolicy::POSSIBLE:
            case PipelineExecutionContext::ContinuationPolicy::NEVER:
                addInternalTask(std::move(task));
                return true;
        }
        std::unreachable();
    }

    void emitPipelineStart(LocalQueryId qid, const std::shared_ptr<RunningQueryPlanNode>& node, TaskCallback callback) override
    {
        auto [complete, failure, success] = std::move(callback).take();
        auto wrappedCallback = TaskCallback{
            std::move(complete),
            std::move(success),
            TaskCallback::OnFailure(injectQueryFailure(node, std::move(failure.callback))),
        };
        addInternalTask(StartPipelineTask(qid, node->id, std::move(wrappedCallback), node));
    }

    void emitPipelineStop(LocalQueryId qid, std::unique_ptr<RunningQueryPlanNode> node, TaskCallback callback) override
    {
        auto [complete, failure, success] = std::move(callback).take();
        auto wrappedCallback = TaskCallback{
            std::move(complete),
            std::move(success),
            /// Calling the Unsafe version of injectQueryFailure is required here because the RunningQueryPlan is a unique ptr.
            /// However the StopPipelineTask takes ownership of the Node and thus guarantees that it is alive when the callback is invoked.
            TaskCallback::OnFailure(injectQueryFailureUnsafe(*node, std::move(failure.callback))),
        };
        addInternalTask(StopPipelineTask(qid, std::move(node), std::move(wrappedCallback)));
    }

    void initializeSourceFailure(LocalQueryId id, OriginId sourceId, std::weak_ptr<RunningSource> source, Exception exception) override
    {
        PRECONDITION(ThreadPool::WorkerThread::id == INVALID<WorkerThreadId>, "This should only be called from a non-worker thread");
        taskQueue.addAdmissionTaskBlocking(
            {},
            FailSourceTask{
                id,
                std::move(source),
                std::move(exception),
                TaskCallback{TaskCallback::OnSuccess(
                    [id, sourceId, listener = listener]
                    { listener->logSourceTermination(id, sourceId, QueryTerminationType::Failure, std::chrono::system_clock::now()); })}});
    }

    void initializeSourceStop(LocalQueryId id, OriginId sourceId, std::weak_ptr<RunningSource> source) override
    {
        PRECONDITION(ThreadPool::WorkerThread::id == INVALID<WorkerThreadId>, "This should only be called from a non-worker thread");
        taskQueue.addAdmissionTaskBlocking(
            {},
            StopSourceTask{
                id,
                std::move(source),
                0,
                TaskCallback{TaskCallback::OnSuccess(
                    [id, sourceId, listener = listener]
                    { listener->logSourceTermination(id, sourceId, QueryTerminationType::Graceful, std::chrono::system_clock::now()); })}});
    }

    void emitPendingPipelineStop(LocalQueryId queryId, std::shared_ptr<RunningQueryPlanNode> node, TaskCallback callback) override
    {
        ENGINE_LOG_DEBUG("Inserting Pending Pipeline Stop for {}-{}", queryId, node->id);
        addInternalTask(PendingPipelineStopTask{queryId, std::move(node), 0, std::move(callback)});
    }

    ThreadPool(
        std::shared_ptr<AbstractQueryStatusListener> listener,
        std::shared_ptr<QueryEngineStatisticListener> stats,
        std::shared_ptr<AbstractBufferProvider> bufferProvider,
        const size_t admissionQueueSize)
        : listener(std::move(listener))
        , statistic(std::move(stats))
        , bufferProvider(std::move(bufferProvider))
        , taskQueue(admissionQueueSize)
        , delayedTaskSubmitter([this](Task&& task) noexcept { taskQueue.addInternalTaskNonBlocking(std::move(task)); })
    {
    }

    /// Reserves the initial WorkerThreadId for the terminator thread, which is the thread which is calling shutdown.
    /// This allows the thread to access into the internal task queue, which is prohibited for non-worker threads.
    /// The terminator thread does not count towards the numberOfThreads
    constexpr static WorkerThreadId terminatorThreadId = INITIAL<WorkerThreadId>;

    [[nodiscard]] size_t numberOfThreads() const { return numberOfThreads_.load(); }

    struct WorkerThread
    {
        static thread_local WorkerThreadId id;

        [[nodiscard]] WorkerThread(ThreadPool& pool, bool terminating) : pool(pool), terminating(terminating) { }

        /// Handler for different Pipeline Tasks
        /// Boolean return value indicates if the onSuccess should be called
        bool operator()(WorkTask& task) const;
        bool operator()(StopQueryTask& stopQuery) const;
        bool operator()(StartQueryTask& startQuery) const;
        bool operator()(StartPipelineTask& startPipeline) const;
        bool operator()(PendingPipelineStopTask& pendingPipelineStop) const;
        bool operator()(StopPipelineTask& stopPipelineTask) const;
        bool operator()(StopSourceTask& stopSource) const;
        bool operator()(FailSourceTask& failSource) const;

    private:
        ThreadPool& pool; ///NOLINT The ThreadPool will always outlive the worker and not move.
        bool terminating{};
    };

private:
    void addInternalTask(Task&& task)
    {
        PRECONDITION(ThreadPool::WorkerThread::id != INVALID<WorkerThreadId>, "This should only be called from a worker thread");
        taskQueue.addInternalTaskNonBlocking(std::move(task)); /// NOLINT no move will happen if tryWriteUntil has failed
    }

    /// Order of destruction matters: TaskQueue has to outlive the pool
    std::shared_ptr<AbstractQueryStatusListener> listener;
    std::shared_ptr<QueryEngineStatisticListener> statistic;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::atomic<TaskId::Underlying> taskIdCounter;

    TaskQueue<Task> taskQueue;
    DelayedTaskSubmitter<> delayedTaskSubmitter;

    /// Class Invariant: numberOfThreads == pool.size().
    /// We don't want to expose the vector directly to anyone, as this would introduce a race condition.
    /// The number of threads is only available via the atomic.
    std::vector<Thread> pool;
    std::atomic<int32_t> numberOfThreads_;

    friend class QueryEngine;
};

/// Marks every Thread which has not explicitly been created by the ThreadPool as a non-worker thread
thread_local WorkerThreadId ThreadPool::WorkerThread::id = INVALID<WorkerThreadId>;

bool ThreadPool::WorkerThread::operator()(WorkTask& task) const
{
    LogContext logContextQuery("Query", task.queryId);
    LogContext logContextPipeline("Pipeline", task.pipelineId);
    if (terminating)
    {
        ENGINE_LOG_WARNING("Skipped Task for {}-{} during termination", task.queryId, task.pipelineId);
        return false;
    }

    const auto taskId = TaskId(pool.taskIdCounter++);
    if (auto pipeline = task.pipeline.lock())
    {
        ENGINE_LOG_DEBUG("Handle Task for {}-{}. Tuples: {}", task.queryId, pipeline->id, task.buf.getNumberOfTuples());
        DefaultPEC pec(
            pool.numberOfThreads(),
            WorkerThread::id,
            pipeline->id,
            pool.bufferProvider,
            [&](const TupleBuffer& tupleBuffer, PipelineExecutionContext::ContinuationPolicy continuationPolicy)
            {
                ENGINE_LOG_DEBUG(
                    "Task emitted tuple buffer {}-{}. Tuples: {}", task.queryId, task.pipelineId, tupleBuffer.getNumberOfTuples());
                return std::ranges::all_of(
                    pipeline->successors,
                    [&](const auto& successor)
                    {
                        pool.statistic->onEvent(
                            TaskEmit{id, task.queryId, pipeline->id, successor->id, taskId, tupleBuffer.getNumberOfTuples()});
                        return pool.emitWork(task.queryId, successor, tupleBuffer, TaskCallback{}, continuationPolicy);
                    });
            },
            [&](const TupleBuffer& tupleBuffer, std::chrono::milliseconds duration)
            {
                if (duration.count() > 0)
                {
                    pool.delayedTaskSubmitter.submitTaskIn(
                        WorkTask(task.queryId, pipeline->id, pipeline, tupleBuffer, std::move(task.callback)), duration);
                }
                else
                {
                    pool.addInternalTask(WorkTask(task.queryId, pipeline->id, pipeline, tupleBuffer, std::move(task.callback)));
                }
                pool.statistic->onEvent(TaskEmit{id, task.queryId, pipeline->id, pipeline->id, taskId, tupleBuffer.getNumberOfTuples()});
            }

        );
        pool.statistic->onEvent(TaskExecutionStart{WorkerThread::id, task.queryId, pipeline->id, taskId, task.buf.getNumberOfTuples()});
        pipeline->stage->execute(task.buf, pec);
        pool.statistic->onEvent(TaskExecutionComplete{WorkerThread::id, task.queryId, pipeline->id, taskId});
        return true;
    }

    ENGINE_LOG_WARNING(
        "Task {} for Query {}-{} is expired. Tuples: {}", taskId, task.queryId, task.pipelineId, task.buf.getNumberOfTuples());
    pool.statistic->onEvent(TaskExpired{WorkerThread::id, task.queryId, task.pipelineId, taskId});
    return false;
}

bool ThreadPool::WorkerThread::operator()(StartPipelineTask& startPipeline) const
{
    LogContext logContextQuery("Query", startPipeline.queryId);
    LogContext logContextPipeline("Pipeline", startPipeline.pipelineId);
    if (terminating)
    {
        ENGINE_LOG_WARNING("Pipeline Start {}-{} was skipped during Termination", startPipeline.queryId, startPipeline.pipelineId);
        return false;
    }

    if (auto pipeline = startPipeline.pipeline.lock())
    {
        ENGINE_LOG_DEBUG("Setup Pipeline Task for {}-{}", startPipeline.queryId, pipeline->id);
        DefaultPEC pec(
            pool.numberOfThreads(),
            WorkerThread::id,
            pipeline->id,
            pool.bufferProvider,
            [](const TupleBuffer&, PipelineExecutionContext::ContinuationPolicy)
            {
                /// Catch Emits, that are currently not supported during pipeline stage initialization.
                INVARIANT(
                    false,
                    "Currently we assume that a pipeline cannot emit data during setup. All pipeline initializations happen "
                    "concurrently and there is no guarantee that the successor pipeline has been initialized");
                return false;
            },
            [&](const TupleBuffer&, std::chrono::milliseconds)
            {
                INVARIANT(
                    false,
                    "Repeat pipeline setup is currently not supported. Although there is no inherit reason this wouldn't work, but its not "
                    "tested");
            });
        pipeline->stage->start(pec);
        pool.statistic->onEvent(PipelineStart{WorkerThread::id, startPipeline.queryId, pipeline->id});
        return true;
    }

    ENGINE_LOG_WARNING("Setup pipeline is expired for {}-{}", startPipeline.queryId, startPipeline.pipelineId);
    return false;
}

bool ThreadPool::WorkerThread::operator()(PendingPipelineStopTask& pendingPipelineStop) const
{
    LogContext logContextQuery("Query", pendingPipelineStop.queryId);
    LogContext logContextPipeline("Pipeline", pendingPipelineStop.pipeline->id);
    INVARIANT(
        pendingPipelineStop.pipeline->pendingTasks >= 0,
        "Pending Pipeline Stop must have pending tasks, but had {} pending tasks.",
        pendingPipelineStop.pipeline->pendingTasks);

    if (!pendingPipelineStop.pipeline->requiresTermination)
    {
        /// The decision for a soft stop might have been overruled by a hardstop or system shutdown
        return false;
    }

    if (pendingPipelineStop.pipeline->pendingTasks > 0)
    {
        ENGINE_LOG_TRACE(
            "Pipeline {}-{} is still active: {}. Seen for {}th time",
            pendingPipelineStop.queryId,
            pendingPipelineStop.pipeline->id,
            pendingPipelineStop.pipeline->pendingTasks,
            pendingPipelineStop.attempts);

        PendingPipelineStopTask repeatTask(
            pendingPipelineStop.queryId,
            pendingPipelineStop.pipeline,
            pendingPipelineStop.attempts + 1,
            std::move(pendingPipelineStop.callback));
        /// If we have seen this pipeline for the third time, we will add some work from the admission queue to the internal queue.
        /// We need to do this, as the pipeline might be stuck in a deadlock as it is waiting for data from a source which has not been moved into the internal queue.
        if (pendingPipelineStop.attempts >= PIPELINE_STOP_BACKOFF_THRESHOLD)
        {
            pool.delayedTaskSubmitter.submitTaskIn(std::move(repeatTask), PIPELINE_STOP_BACKOFF_INTERVAL);
        }
        else
        {
            pool.addInternalTask(std::move(repeatTask));
        }
        return false;
    }

    return true;
}

bool ThreadPool::WorkerThread::operator()(StopPipelineTask& stopPipelineTask) const
{
    LogContext logContextQuery("Query", stopPipelineTask.queryId);
    LogContext logContextPipeline("Pipeline", stopPipelineTask.pipeline->id);
    ENGINE_LOG_DEBUG("Stop Pipeline Task for {}-{}", stopPipelineTask.queryId, stopPipelineTask.pipeline->id);
    DefaultPEC pec(
        pool.numberOfThreads(),
        WorkerThread::id,
        stopPipelineTask.pipeline->id,
        pool.bufferProvider,
        [&](const TupleBuffer& tupleBuffer, PipelineExecutionContext::ContinuationPolicy policy)
        {
            if (terminating)
            {
                ENGINE_LOG_WARNING("Dropping tuple buffer during query engine termination");
                return true;
            }

            for (const auto& successor : stopPipelineTask.pipeline->successors)
            {
                /// The Termination Exceution Context appends a strong reference to the successer into the Task.
                /// This prevents the successor nodes to be destructed before they were able process tuplebuffer generated during
                /// pipeline termination.
                pool.emitWork(stopPipelineTask.queryId, successor, tupleBuffer, TaskCallback{}, policy);
            }
            return true;
        },
        [&](const TupleBuffer&, std::chrono::milliseconds duration)
        {
            StopPipelineTask repeatedTask(
                stopPipelineTask.queryId, std::move(stopPipelineTask.pipeline), std::move(stopPipelineTask.callback));
            if (duration.count() > 0)
            {
                pool.delayedTaskSubmitter.submitTaskIn(std::move(repeatedTask), duration);
            }
            else
            {
                pool.addInternalTask(std::move(repeatedTask));
            }
        });

    ENGINE_LOG_DEBUG("Stopping Pipeline {}-{}", stopPipelineTask.queryId, stopPipelineTask.pipeline->id);
    auto pipelineId = stopPipelineTask.pipeline->id;
    auto queryId = stopPipelineTask.queryId;
    stopPipelineTask.pipeline->stage->stop(pec);
    pool.statistic->onEvent(PipelineStop{WorkerThread::id, queryId, pipelineId});
    return true;
}

bool ThreadPool::WorkerThread::operator()(StopQueryTask& stopQuery) const
{
    LogContext logContext("Query", fmt::format("{}", stopQuery.queryId));
    ENGINE_LOG_INFO("Terminate Query Task for Query {}", stopQuery.queryId);
    if (auto queryCatalog = stopQuery.catalog.lock())
    {
        queryCatalog->stopQuery(stopQuery.queryId);
        pool.statistic->onEvent(QueryStopRequest{WorkerThread::id, stopQuery.queryId});
        return true;
    }
    return false;
}

bool ThreadPool::WorkerThread::operator()(StartQueryTask& startQuery) const
{
    LogContext logContext("Query", fmt::format("{}", startQuery.queryId));
    ENGINE_LOG_INFO("Start Query Task for Query {}", startQuery.queryId);
    if (auto queryCatalog = startQuery.catalog.lock())
    {
        queryCatalog->start(startQuery.queryId, std::move(startQuery.queryPlan), pool.listener, pool.statistic, pool, pool);
        pool.statistic->onEvent(QueryStart{WorkerThread::id, startQuery.queryId});
        return true;
    }
    return false;
}

bool ThreadPool::WorkerThread::operator()(StopSourceTask& stopSource) const
{
    LogContext logContext("Query", fmt::format("{}", stopSource.queryId));
    if (auto source = stopSource.target.lock())
    {
        ENGINE_LOG_DEBUG("Stop Source Task for Query {} Source {}", stopSource.queryId, source->getOriginId());
        if (!source->attemptUnregister())
        {
            ENGINE_LOG_WARNING(
                "Could not immediately stop source. Reattempting at a later point. Query: {}, Source: {}",
                stopSource.queryId,
                source->getOriginId());

            StopSourceTask repeatTask{
                stopSource.queryId, std::move(stopSource.target), stopSource.attempts + 1, std::move(stopSource.callback)};

            if (stopSource.attempts >= 2)
            {
                const auto delay = std::chrono::milliseconds(25) * stopSource.attempts;
                pool.delayedTaskSubmitter.submitTaskIn(std::move(repeatTask), delay);
            }
            else
            {
                pool.addInternalTask(std::move(repeatTask));
            }
            return false;
        }
        return true;
    }

    ENGINE_LOG_WARNING("Stop Source Task for Query {} and expired source", stopSource.queryId);
    return false;
}

bool ThreadPool::WorkerThread::operator()(FailSourceTask& failSource) const
{
    LogContext logContext("Query", fmt::format("{}", failSource.queryId));
    if (auto source = failSource.target.lock())
    {
        ENGINE_LOG_DEBUG("Fail Source Task for Query {} Source {}", failSource.queryId, source->getOriginId());
        source->fail(std::move(*failSource.exception));
        return true;
    }
    return false;
}

void ThreadPool::addThread(WorkerId workerId)
{
    pool.emplace_back(
        fmt::format("WorkerThread-{}", numberOfThreads_),
        workerId,
        [this, id = numberOfThreads_++](const std::stop_token& stopToken)
        {
            WorkerThread::id = WorkerThreadId(WorkerThreadId::INITIAL + id);
            const WorkerThread worker{*this, false};
            while (!stopToken.stop_requested())
            {
                if (auto task = taskQueue.getNextTaskBlocking(stopToken))
                {
                    handleTask(worker, std::move(*task));
                }
            }

            ENGINE_LOG_INFO("WorkerThread {} shutting down", id);
            /// Worker in termination mode will not emit further work and eventually clear the task queue and terminate.
            const WorkerThread terminatingWorker{*this, true};
            while (auto task = taskQueue.getNextTaskNonBlocking())
            {
                handleTask(terminatingWorker, std::move(*task));
            }
        });
}

QueryEngine::QueryEngine(
    const QueryEngineConfiguration& config,
    std::shared_ptr<QueryEngineStatisticListener> statListener,
    std::shared_ptr<AbstractQueryStatusListener> listener,
    std::shared_ptr<BufferManager> bm,
    WorkerId workerId)
    : bufferManager(std::move(bm))
    , statusListener(std::move(listener))
    , statisticListener(std::move(statListener))
    , queryCatalog(std::make_shared<QueryCatalog>())
    , threadPool(std::make_unique<ThreadPool>(statusListener, statisticListener, bufferManager, config.admissionQueueSize.getValue()))
    , workerId(workerId)
{
    for (size_t i = 0; i < config.numberOfWorkerThreads.getValue(); ++i)
    {
        threadPool->addThread(workerId);
    }
}

/// NOLINTNEXTLINE Intentionally non-const
void QueryEngine::stop(LocalQueryId queryId)
{
    ENGINE_LOG_INFO("Stopping Query: {}", queryId);
    threadPool->taskQueue.addAdmissionTaskBlocking({}, StopQueryTask{queryId, queryCatalog, TaskCallback{}});
}

/// NOLINTNEXTLINE Intentionally non-const
void QueryEngine::start(LocalQueryId queryId, std::unique_ptr<ExecutableQueryPlan> executableQueryPlan)
{
    threadPool->taskQueue.addAdmissionTaskBlocking(
        {}, StartQueryTask{queryId, std::move(executableQueryPlan), queryCatalog, TaskCallback{}});
}

QueryEngine::~QueryEngine()
{
    ThreadPool::WorkerThread::id = ThreadPool::terminatorThreadId;
    queryCatalog->clear();
}

void QueryCatalog::start(
    LocalQueryId queryId,
    std::unique_ptr<ExecutableQueryPlan> plan,
    const std::shared_ptr<AbstractQueryStatusListener>& listener,
    const std::shared_ptr<QueryEngineStatisticListener>& statistic,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    const std::scoped_lock lock(mutex);

    struct RealQueryLifeTimeListener : QueryLifetimeListener
    {
        RealQueryLifeTimeListener(
            LocalQueryId queryId,
            std::shared_ptr<AbstractQueryStatusListener> listener,
            std::shared_ptr<QueryEngineStatisticListener> statistic)
            : listener(std::move(listener)), statistic(std::move(statistic)), queryId(queryId)
        {
        }

        void onRunning() override
        {
            ENGINE_LOG_DEBUG("Query {} onRunning", queryId);
            const auto timestamp = std::chrono::system_clock::now();
            if (const auto locked = state.lock())
            {
                locked->transition(
                    [](Reserved&&)
                    {
                        INVARIANT(false, "Bug: Jumping from reserved to running state should be impossible.");
                        return Terminated{Terminated::Failed};
                    },
                    [](Starting&& starting) { return Running{std::move(starting.plan)}; });
                listener->logQueryStatusChange(queryId, QueryState::Running, timestamp);
            }
        }

        void onFailure(Exception exception) override
        {
            ENGINE_LOG_DEBUG("Query {} onFailure", queryId);
            const auto timestamp = std::chrono::system_clock::now();
            if (const auto locked = state.lock())
            {
                /// Regardless of its current state the query should move into the Terminated::Failed state.
                locked->transition(
                    [](Reserved&&)
                    {
                        ENGINE_LOG_DEBUG("Query was stopped before all pipeline starts were submitted");
                        return Terminated{Terminated::Failed};
                    },
                    [](Starting&& starting)
                    {
                        RunningQueryPlan::dispose(std::move(starting.plan));
                        return Terminated{Terminated::Failed};
                    },
                    [](Running&& running)
                    {
                        RunningQueryPlan::dispose(std::move(running.plan));
                        return Terminated{Terminated::Failed};
                    },
                    [](Stopping&& stopping)
                    {
                        StoppingQueryPlan::dispose(std::move(stopping.plan));
                        return Terminated{Terminated::Failed};
                    },
                    [](Terminated&&) { return Terminated{Terminated::Failed}; });

                exception.what() += fmt::format(" in Query {}.", queryId);
                ENGINE_LOG_ERROR("Query Failed: {}", exception.what());
                listener->logQueryFailure(queryId, std::move(exception), timestamp);
                statistic->onEvent(QueryFail(ThreadPool::WorkerThread::id, queryId));
            }
        }

        /// OnDestruction is called when the entire query graph is terminated.
        void onDestruction() override
        {
            ENGINE_LOG_DEBUG("Query {} onDestruction", queryId);
            const auto timestamp = std::chrono::system_clock::now();
            if (const auto locked = state.lock())
            {
                const auto didTransition = locked->transition(
                    [](Starting&& starting) /// NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
                    {
                        RunningQueryPlan::dispose(std::move(starting.plan));
                        return Terminated{Terminated::Stopped};
                    },
                    [](Running&& running) /// NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
                    {
                        RunningQueryPlan::dispose(std::move(running.plan));
                        return Terminated{Terminated::Stopped};
                    },
                    [](Stopping&& stopping)
                    {
                        StoppingQueryPlan::dispose(std::move(stopping.plan));
                        return Terminated{Terminated::Stopped};
                    });
                if (didTransition)
                {
                    listener->logQueryStatusChange(queryId, QueryState::Stopped, timestamp);
                    statistic->onEvent(QueryStop(ThreadPool::WorkerThread::id, queryId));
                }
            }
        }

        std::shared_ptr<AbstractQueryStatusListener> listener;
        std::shared_ptr<QueryEngineStatisticListener> statistic;
        LocalQueryId queryId;
        WeakStateRef state;
    };

    auto queryListener = std::make_shared<RealQueryLifeTimeListener>(queryId, listener, statistic);
    const auto startTimestamp = std::chrono::system_clock::now();
    auto state = std::make_shared<StateRef>(Reserved{});
    this->queryStates.emplace(queryId, state);
    queryListener->state = state;

    auto [runningQueryPlan, callback] = RunningQueryPlan::start(queryId, std::move(plan), controller, emitter, queryListener);

    if (state->transition([&](Reserved&&)
                          { return Starting{std::move(runningQueryPlan)}; })) /// NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
    {
        listener->logQueryStatusChange(queryId, QueryState::Started, startTimestamp);
    }
    else
    {
        /// The move did not happen.
        INVARIANT(
            state->is<Terminated>(),
            "Bug: There is no other option for the state. The only transition from reserved to Starting happens here. Starting will "
            "not transition into running until the callback is dropped.");
        RunningQueryPlan::dispose(std::move(runningQueryPlan));
    }
}

void QueryCatalog::stopQuery(LocalQueryId id)
{
    const std::unique_ptr<RunningQueryPlan> toBeDeleted;
    {
        const std::scoped_lock lock(mutex);
        if (auto it = queryStates.find(id); it != queryStates.end())
        {
            auto& state = *it->second;
            state.transition(
                [](Starting&& starting) /// NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
                {
                    auto stoppingQueryPlan = RunningQueryPlan::stop(std::move(starting.plan));
                    return Stopping{std::move(stoppingQueryPlan)};
                },
                [](Running&& running) /// NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
                {
                    auto stoppingQueryPlan = RunningQueryPlan::stop(std::move(running.plan));
                    return Stopping{std::move(stoppingQueryPlan)};
                });
        }
        else
        {
            ENGINE_LOG_WARNING("Attempting to stop query {} failed. Query was not submitted to the engine.", id);
        }
    }
}
}
