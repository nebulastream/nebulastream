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
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/AtomicState.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <folly/MPMCQueue.h>
#include <EngineLogger.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <ExecutableQueryPlan.hpp>
#include <Interfaces.hpp>
#include <PipelineExecutionContext.hpp>
#include <QueryEngine.hpp>
#include <QueryEngineConfiguration.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <RunningQueryPlan.hpp>
#include <Task.hpp>

namespace NES::Runtime
{


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
        QueryId queryId,
        std::unique_ptr<ExecutableQueryPlan> plan,
        const std::shared_ptr<AbstractQueryStatusListener>& listener,
        QueryLifetimeController& controller,
        WorkEmitter& emitter);
    QueryId registerQuery(std::unique_ptr<ExecutableQueryPlan>);
    void stopQuery(QueryId queryId);

    void clear()
    {
        const std::scoped_lock lock(mutex);
        queryStates.clear();
    }

private:
    std::atomic<QueryId::Underlying> queryIdCounter = QueryId::INITIAL;
    std::recursive_mutex mutex;
    std::unordered_map<QueryId, State> queryStates;
};

namespace detail
{
using Queue = folly::MPMCQueue<Task>;
}

struct DefaultPEC final : Execution::PipelineExecutionContext
{
    std::vector<std::shared_ptr<Execution::OperatorHandler>>* operatorHandlers = nullptr;
    std::function<bool(const Memory::TupleBuffer& tb, ContinuationPolicy)> handler;
    std::shared_ptr<Memory::AbstractBufferProvider> bm;
    size_t numberOfThreads;
    WorkerThreadId threadId;
    PipelineId pipelineId;

    DefaultPEC(
        size_t numberOfThreads,
        WorkerThreadId threadId,
        PipelineId pipelineId,
        std::shared_ptr<Memory::AbstractBufferProvider> bm,
        std::function<bool(const Memory::TupleBuffer& tb, ContinuationPolicy)> handler)
        : handler(std::move(handler)), bm(std::move(bm)), numberOfThreads(numberOfThreads), threadId(threadId), pipelineId(pipelineId)
    {
    }

    [[nodiscard]] WorkerThreadId getId() const override { return threadId; }

    Memory::TupleBuffer allocateTupleBuffer() override { return bm->getBufferBlocking(); }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return numberOfThreads; }

    bool emitBuffer(const Memory::TupleBuffer& buffer, ContinuationPolicy policy) override { return handler(buffer, policy); }

    std::shared_ptr<Memory::AbstractBufferProvider> getBufferManager() const override { return bm; }

    PipelineId getPipelineId() const override { return pipelineId; }

    std::vector<std::shared_ptr<Execution::OperatorHandler>>& getOperatorHandlers() override
    {
        PRECONDITION(operatorHandlers, "Operator Handlers were not set");
        return *operatorHandlers;
    }

    void setOperatorHandlers(std::vector<std::shared_ptr<Execution::OperatorHandler>>& handlers) override
    {
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
    void addThread();

    /// This function is unsafe because it requires the lifetime of the RunningQueryPlanNode exceed the lifetime of the callback
    std::function<void(Exception)> injectQueryFailureUnsafe(RunningQueryPlanNode* node, std::function<void(Exception)> failure)
    {
        return [failure, node = std::move(node)](Exception exception)
        {
            if (failure)
            {
                failure(exception);
            }
            node->fail(exception);
        };
    }

    std::function<void(Exception)> injectQueryFailure(std::weak_ptr<RunningQueryPlanNode> node, std::function<void(Exception)> failure)
    {
        return [failure, node = std::move(node)](Exception exception)
        {
            if (failure)
            {
                failure(exception);
            }
            if (auto strongReference = node.lock())
            {
                strongReference->fail(exception);
            }
        };
    }

    template <typename... Args>
    auto injectReferenceCountReducer(
        ENGINE_IF_LOG_DEBUG(QueryId qid, ) std::weak_ptr<RunningQueryPlanNode> node, std::function<void(Args...)> innerFunction)
    {
        return [ENGINE_IF_LOG_DEBUG(qid, ) innerFunction = std::move(innerFunction), node = std::weak_ptr(node)](Args... args)
        {
            if (innerFunction)
            {
                innerFunction(args...);
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

    bool emitWork(
        QueryId qid,
        const std::shared_ptr<RunningQueryPlanNode>& node,
        Memory::TupleBuffer buffer,
        BaseTask::onComplete complete,
        BaseTask::onFailure failure,
        const Execution::PipelineExecutionContext::ContinuationPolicy continuationPolicy) override
    {
        [[maybe_unused]] auto updatedCount = node->pendingTasks.fetch_add(1) + 1;
        ENGINE_LOG_DEBUG("Increasing number of pending tasks on pipeline {}-{} to {}", qid, node->id, updatedCount);
        auto task = WorkTask(
            qid,
            node->id,
            node,
            buffer,
            injectReferenceCountReducer(ENGINE_IF_LOG_DEBUG(qid, ) node, std::move(complete)),
            injectQueryFailure(node, injectReferenceCountReducer(ENGINE_IF_LOG_DEBUG(qid, ) node, std::move(failure))));
        if (WorkerThread::id == INVALID<WorkerThreadId>)
        {
            /// Non-WorkerThread
            admissionQueue.blockingWrite(std::move(task));
            ENGINE_LOG_DEBUG("Task written to AdmissionQueue");
            return true;
        }

        /// WorkerThread
        switch (continuationPolicy)
        {
            case Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE:
                addTaskOrDoItInPlace(std::move(task));
                return true;

            case Execution::PipelineExecutionContext::ContinuationPolicy::REPEAT:
            case Execution::PipelineExecutionContext::ContinuationPolicy::NEVER:
                if (not internalTaskQueue.tryWriteUntil(
                        std::chrono::high_resolution_clock::now() + std::chrono::seconds(1), std::move(task)))
                {
                    node->pendingTasks.fetch_sub(1);
                    ENGINE_LOG_DEBUG("TaskQueue is full, could not write within 1 second.");
                    return false;
                }
                return true;
        }
    }

    void emitPipelineStart(
        QueryId qid, const std::shared_ptr<RunningQueryPlanNode>& node, BaseTask::onComplete complete, BaseTask::onFailure failure) override
    {
        addTaskOrDoNextTask(StartPipelineTask(qid, node->id, complete, injectQueryFailure(node, failure), node));
    }

    void emitPipelineStop(
        QueryId qid, std::unique_ptr<RunningQueryPlanNode> node, BaseTask::onComplete complete, BaseTask::onFailure failure) override
    {
        auto nodePtr = node.get();
        /// Calling the Unsafe version of injectQueryFailure is required here because the RunningQueryPlan is a unique ptr.
        /// However the StopPipelineTask takes ownership of the Node and thus guarantees that it is alive when the callback is invoked.
        addTaskOrDoNextTask(StopPipelineTask(qid, std::move(node), complete, injectQueryFailureUnsafe(nodePtr, failure)));
    }

    void initializeSourceFailure(QueryId id, OriginId sourceId, std::weak_ptr<RunningSource> source, Exception exception) override
    {
        PRECONDITION(ThreadPool::WorkerThread::id == INVALID<WorkerThreadId>, "This should only be called from a non-worker thread");
        admissionQueue.blockingWrite(FailSourceTask{
            id,
            std::move(source),
            std::move(exception),
            [id, sourceId, listener = listener]
            { listener->logSourceTermination(id, sourceId, QueryTerminationType::Failure, std::chrono::system_clock::now()); },
            {}});
    }

    void initializeSourceStop(QueryId id, OriginId sourceId, std::weak_ptr<RunningSource> source) override
    {
        PRECONDITION(ThreadPool::WorkerThread::id == INVALID<WorkerThreadId>, "This should only be called from a non-worker thread");
        admissionQueue.blockingWrite(StopSourceTask{
            id,
            std::move(source),
            [id, sourceId, listener = listener]
            { listener->logSourceTermination(id, sourceId, QueryTerminationType::Graceful, std::chrono::system_clock::now()); },
            {}});
    }

    void emitPendingPipelineStop(
        QueryId queryId, std::shared_ptr<RunningQueryPlanNode> node, BaseTask::onComplete complete, BaseTask::onFailure failure) override
    {
        ENGINE_LOG_DEBUG("Inserting Pending Pipeline Stop for {}-{}", queryId, node->id);
        addTaskOrDoNextTask(PendingPipelineStopTask{queryId, std::move(node), 0, std::move(complete), std::move(failure)});
    }

    ThreadPool(
        std::shared_ptr<AbstractQueryStatusListener> listener,
        std::shared_ptr<QueryEngineStatisticListener> stats,
        std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider,
        const size_t internalTaskQueueSize,
        const size_t admissionQueueSize)
        : listener(std::move(listener))
        , statistic(std::move(std::move(stats)))
        , bufferProvider(std::move(bufferProvider))
        , admissionQueue(admissionQueueSize)
        , internalTaskQueue(internalTaskQueueSize)
    {
    }

    /// Reserves the initial WorkerThreadId for the terminator thread, which is the thread which is calling shutdown.
    /// This allows the thread to access into the internalTaskQueue, which is prohibited for non-worker threads.
    /// The terminator thread does not count towards numberOfThreads
    constexpr static WorkerThreadId terminatorThreadId = INITIAL<WorkerThreadId>;

    [[nodiscard]] size_t numberOfThreads() const { return numberOfThreads_.load(); }

private:
    struct WorkerThread
    {
        static thread_local WorkerThreadId id;
        /// Handler for different Pipeline Tasks
        /// Boolean return value indicates if the onComplete should be called
        bool operator()(const WorkTask& task) const;
        bool operator()(const StopQueryTask& stopQuery) const;
        bool operator()(StartQueryTask& startQuery) const;
        bool operator()(const StartPipelineTask& startPipeline) const;
        bool operator()(PendingPipelineStopTask pendingPipelineStop) const;
        bool operator()(const StopPipelineTask& stopPipelineTask) const;
        bool operator()(const StopSourceTask& stopSource) const;
        bool operator()(const FailSourceTask& failSource) const;

        [[nodiscard]] WorkerThread(ThreadPool& pool, bool terminating) : pool(pool), terminating(terminating) { }

    private:
        ThreadPool& pool; ///NOLINT The ThreadPool will always outlive the worker and not move.
        bool terminating{};
    };

    void doTaskInPlace(Task&& task)
    {
        WorkerThread worker{*this, false};
        try
        {
            if (std::visit(worker, task))
            {
                completeTask(task);
            }
        }
        catch (const Exception& exception)
        {
            failTask(task, exception);
        }
    }

    void addTaskOrDoItInPlace(Task&& task)
    {
        PRECONDITION(ThreadPool::WorkerThread::id != INVALID<WorkerThreadId>, "This should only be called from a worker thread");
        if (not internalTaskQueue.write(std::move(task))) /// NOLINT no move will happen if tryWriteUntil has failed
        {
            doTaskInPlace(std::move(task)); /// NOLINT no move will happen
        }
    }

    void addTaskOrDoNextTask(Task&& task, uint64_t stackLevel = 0)
    {
        PRECONDITION(ThreadPool::WorkerThread::id != INVALID<WorkerThreadId>, "This should only be called from a worker thread");

        /// It might happen that the task queue is full and we are not able to write the task into the queue.
        /// If this happens for a lot of cases, we would end up in a stack overflow, as we recursively call this function.
        /// To mitigate this, we will check if we have reached a certain stack level and if so, we will fail the task.
        if (constexpr auto MAX_STACK_LEVEL = 5000; stackLevel >= MAX_STACK_LEVEL)
        {
            throw TooMuchWork("TaskQueue is always full. We have tried for {} times to write the task into the queue", stackLevel);
        }


        if (not internalTaskQueue.writeIfNotFull(std::move(task))) /// NOLINT no move will happen if writeIfNotFull has failed
        {
            /// The order below is important. We want to make sure that we pick up a next task before we write the current task into the queue.
            Task nextTask;
            const auto hasNextTask = internalTaskQueue.read(nextTask);
            addTaskOrDoNextTask(std::move(task), stackLevel + 1); /// NOLINT no move will happen if tryWriteUntil has failed
            if (hasNextTask)
            {
                addTaskOrDoItInPlace(std::move(nextTask));
            }
        }
    }

    /// Order of destruction matters: TaskQueue has to outlive the pool
    std::shared_ptr<AbstractQueryStatusListener> listener;
    std::shared_ptr<QueryEngineStatisticListener> statistic;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    std::atomic<TaskId::Underlying> taskIdCounter;

    detail::Queue admissionQueue;
    detail::Queue internalTaskQueue;

    /// Class Invariant: numberOfThreads == pool.size().
    /// We don't want to expose the vector directly to anyone, as this would introduce a race condition.
    /// The number of threads is only available via the atomic.
    std::vector<std::jthread> pool;
    std::atomic<int32_t> numberOfThreads_;

    friend class QueryEngine;
};

/// Marks every Thread which has not explicitly been created by the ThreadPool as a non-worker thread
thread_local WorkerThreadId ThreadPool::WorkerThread::id = INVALID<WorkerThreadId>;

bool ThreadPool::WorkerThread::operator()(const WorkTask& task) const
{
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
            [&](const Memory::TupleBuffer& tupleBuffer, auto continuationPolicy)
            {
                ENGINE_LOG_DEBUG(
                    "Task emitted tuple buffer {}-{}. Tuples: {}", task.queryId, task.pipelineId, tupleBuffer.getNumberOfTuples());
                /// If the current WorkTask is a 'repeat' task, re-emit the same tuple buffer and the same pipeline as a WorkTask.
                if (continuationPolicy == Execution::PipelineExecutionContext::ContinuationPolicy::REPEAT)
                {
                    pool.statistic->onEvent(
                        TaskEmit{id, task.queryId, pipeline->id, pipeline->id, taskId, tupleBuffer.getNumberOfTuples()});
                    return pool.emitWork(task.queryId, pipeline, tupleBuffer, {}, {}, continuationPolicy);
                }
                /// Otherwise, get the successor of the pipeline, and emit a work task for it.
                return std::ranges::all_of(
                    pipeline->successors,
                    [&](const auto& successor)
                    {
                        pool.statistic->onEvent(
                            TaskEmit{id, task.queryId, pipeline->id, successor->id, taskId, tupleBuffer.getNumberOfTuples()});
                        return pool.emitWork(task.queryId, successor, tupleBuffer, {}, {}, continuationPolicy);
                    });
            });
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

bool ThreadPool::WorkerThread::operator()(const StartPipelineTask& startPipeline) const
{
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
            [](const auto&, auto)
            {
                /// Catch Emits, that are currently not supported during pipeline stage initialization.
                INVARIANT(
                    false,
                    "Currently we assume that a pipeline cannot emit data during setup. All pipeline initializations happen "
                    "concurrently and there is no guarantee that the successor pipeline has been initialized");
                return false;
            });
        pipeline->stage->start(pec);
        pool.statistic->onEvent(PipelineStart{WorkerThread::id, startPipeline.queryId, pipeline->id});
        return true;
    }

    ENGINE_LOG_WARNING("Setup pipeline is expired for {}-{}", startPipeline.queryId, startPipeline.pipelineId);
    return false;
}

bool ThreadPool::WorkerThread::operator()(PendingPipelineStopTask pendingPipelineStop) const
{
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
        pendingPipelineStop.attempts += 1;

        /// If we have seen this pipeline for the third time, we will add some work from the admission queue to the internal queue.
        /// We need to do this, as the pipeline might be stuck in a deadlock as it is waiting for data from a source which has not been moved into the internal queue.
        if (pendingPipelineStop.attempts >= 2)
        {
            Task admissionTask;
            if (pool.admissionQueue.read(admissionTask))
            {
                pool.addTaskOrDoItInPlace(std::move(admissionTask));
            }
        }

        pool.addTaskOrDoNextTask(std::move(pendingPipelineStop));
    }

    return true;
}

bool ThreadPool::WorkerThread::operator()(const StopPipelineTask& stopPipelineTask) const
{
    ENGINE_LOG_DEBUG("Stop Pipeline Task for {}-{}", stopPipelineTask.queryId, stopPipelineTask.pipeline->id);
    DefaultPEC pec(
        pool.numberOfThreads(),
        WorkerThread::id,
        stopPipelineTask.pipeline->id,
        pool.bufferProvider,
        [&](const Memory::TupleBuffer& tupleBuffer, auto policy)
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
                pool.emitWork(stopPipelineTask.queryId, successor, tupleBuffer, [ref = successor] { }, {}, policy);
            }
            return true;
        });

    ENGINE_LOG_DEBUG("Stopping Pipeline {}-{}", stopPipelineTask.queryId, stopPipelineTask.pipeline->id);
    stopPipelineTask.pipeline->stage->stop(pec);
    pool.statistic->onEvent(PipelineStop{WorkerThread::id, stopPipelineTask.queryId, stopPipelineTask.pipeline->id});
    return true;
}

bool ThreadPool::WorkerThread::operator()(const StopQueryTask& stopQuery) const
{
    ENGINE_LOG_INFO("Terminate Query Task for Query {}", stopQuery.queryId);
    if (auto queryCatalog = stopQuery.catalog.lock())
    {
        queryCatalog->stopQuery(stopQuery.queryId);
        pool.statistic->onEvent(QueryStop{WorkerThread::id, stopQuery.queryId});
        return true;
    }
    return false;
}

bool ThreadPool::WorkerThread::operator()(StartQueryTask& startQuery) const
{
    ENGINE_LOG_INFO("Start Query Task for Query {}", startQuery.queryId);
    if (auto queryCatalog = startQuery.catalog.lock())
    {
        queryCatalog->start(startQuery.queryId, std::move(startQuery.queryPlan), pool.listener, pool, pool);
        pool.statistic->onEvent(QueryStart{WorkerThread::id, startQuery.queryId});
        return true;
    }
    return false;
}

bool ThreadPool::WorkerThread::operator()(const StopSourceTask& stopSource) const
{
    if (auto source = stopSource.target.lock())
    {
        ENGINE_LOG_DEBUG("Stop Source Task for Query {} Source {}", stopSource.queryId, source->getOriginId());
        if (!source->attemptUnregister())
        {
            ENGINE_LOG_DEBUG("Could not immediately stop source. Reattempting at a later point");
            pool.addTaskOrDoNextTask(stopSource);
            return false;
        }
        return true;
    }

    ENGINE_LOG_WARNING("Stop Source Task for Query {} and expired source", stopSource.queryId);
    return false;
}

bool ThreadPool::WorkerThread::operator()(const FailSourceTask& failSource) const
{
    if (auto source = failSource.target.lock())
    {
        ENGINE_LOG_DEBUG("Fail Source Task for Query {} Source {}", failSource.queryId, source->getOriginId());
        source->fail(failSource.exception);
        return true;
    }
    return false;
}

void ThreadPool::addThread()
{
    pool.emplace_back(
        [this, id = numberOfThreads_++](const std::stop_token& stopToken)
        {
            WorkerThread::id = WorkerThreadId(WorkerThreadId::INITIAL + id);
            setThreadName(fmt::format("WorkerThread-{}", id));
            WorkerThread worker{*this, false};
            while (!stopToken.stop_requested())
            {
                Task task;
                /// This timeout controls how often a thread needs to wake up from polling on the TaskQueue to check the stopToken
                const auto shallPickTaskFromAdmissionQueue = internalTaskQueue.size() < ((static_cast<ssize_t>(numberOfThreads())) * 3);
                if (shallPickTaskFromAdmissionQueue)
                {
                    if (admissionQueue.read(task))
                    {
                        ENGINE_LOG_TRACE(
                            "Task picked from AdmissionQueue and shallPickTaskFromAdmissionQueue={}", shallPickTaskFromAdmissionQueue);
                        addTaskOrDoItInPlace(std::move(task));
                    }
                }
                if (not internalTaskQueue.read(task))
                {
                    continue;
                }
                try
                {
                    if (std::visit(worker, task))
                    {
                        completeTask(task);
                    }
                }
                catch (const Exception& exception)
                {
                    failTask(task, exception);
                }
            }

            ENGINE_LOG_INFO("WorkerThread {} shutting down", id);
            /// Worker in termination mode will emit further work and eventually clear the task queue and terminate.
            WorkerThread terminatingWorker{*this, true};
            while (true)
            {
                Task task;
                if (!internalTaskQueue.readIfNotEmpty(task))
                {
                    break;
                }

                try
                {
                    if (std::visit(terminatingWorker, task))
                    {
                        completeTask(task);
                    }
                }
                catch (const Exception& exception)
                {
                    failTask(task, exception);
                }
            }
        });
}

QueryEngine::QueryEngine(
    const QueryEngineConfiguration& config,
    std::shared_ptr<QueryEngineStatisticListener> statListener,
    std::shared_ptr<AbstractQueryStatusListener> listener,
    std::shared_ptr<Memory::BufferManager> bm)
    : bufferManager(std::move(bm))
    , statusListener(std::move(listener))
    , statisticListener(std::move(statListener))
    , queryCatalog(std::make_shared<QueryCatalog>())
    , threadPool(std::make_unique<ThreadPool>(
          statusListener, statisticListener, bufferManager, config.taskQueueSize.getValue(), config.admissionQueueSize.getValue()))
{
    for (size_t i = 0; i < config.numberOfWorkerThreads.getValue(); ++i)
    {
        threadPool->addThread();
    }
}

/// NOLINTNEXTLINE Intentionally non-const
void QueryEngine::stop(QueryId queryId)
{
    ENGINE_LOG_INFO("Stopping Query: {}", queryId);
    threadPool->admissionQueue.blockingWrite(StopQueryTask{queryId, queryCatalog, {}, {}});
}

/// NOLINTNEXTLINE Intentionally non-const
void QueryEngine::start(std::unique_ptr<ExecutableQueryPlan> instantiatedQueryPlan)
{
    ENGINE_LOG_INFO("Starting Query: {}", fmt::streamed(*instantiatedQueryPlan));
    threadPool->admissionQueue.blockingWrite(
        StartQueryTask{instantiatedQueryPlan->queryId, std::move(instantiatedQueryPlan), queryCatalog, {}, {}});
}

QueryEngine::~QueryEngine()
{
    ThreadPool::WorkerThread::id = ThreadPool::terminatorThreadId;
    queryCatalog->clear();
}

void QueryCatalog::start(
    QueryId queryId,
    std::unique_ptr<ExecutableQueryPlan> plan,
    const std::shared_ptr<AbstractQueryStatusListener>& listener,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    const std::scoped_lock lock(mutex);

    struct RealQueryLifeTimeListener : QueryLifetimeListener
    {
        RealQueryLifeTimeListener(QueryId queryId, std::shared_ptr<AbstractQueryStatusListener> listener)
            : listener(std::move(listener)), queryId(queryId)
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
                listener->logQueryStatusChange(queryId, Execution::QueryStatus::Running, timestamp);
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

                exception.what() += fmt::format(" In Query {}.", queryId);
                ENGINE_LOG_ERROR("Query Failed: {}", exception.what());
                listener->logQueryFailure(queryId, std::move(exception), timestamp);
            }
        }

        /// OnDestruction is called when the entire query graph is terminated.
        void onDestruction() override
        {
            ENGINE_LOG_DEBUG("Query {} onDestruction", queryId);
            const auto timestamp = std::chrono::system_clock::now();
            if (const auto locked = state.lock())
            {
                locked->transition(
                    [](Starting&& starting)
                    {
                        RunningQueryPlan::dispose(std::move(starting.plan));
                        return Terminated{Terminated::Stopped};
                    },
                    [](Running&& running)
                    {
                        RunningQueryPlan::dispose(std::move(running.plan));
                        return Terminated{Terminated::Stopped};
                    },
                    [](Stopping&& stopping)
                    {
                        StoppingQueryPlan::dispose(std::move(stopping.plan));
                        return Terminated{Terminated::Stopped};
                    });
                listener->logQueryStatusChange(queryId, Execution::QueryStatus::Stopped, timestamp);
            }
        }

        std::shared_ptr<AbstractQueryStatusListener> listener;
        QueryId queryId;
        WeakStateRef state;
    };

    auto queryListener = std::make_shared<RealQueryLifeTimeListener>(queryId, listener);
    const auto startTimestamp = std::chrono::system_clock::now();
    auto state = std::make_shared<StateRef>(Reserved{});
    this->queryStates.emplace(queryId, state);
    queryListener->state = state;

    auto [runningQueryPlan, callback] = RunningQueryPlan::start(queryId, std::move(plan), controller, emitter, queryListener);

    if (state->transition([&](Reserved&&) { return Starting{std::move(runningQueryPlan)}; }))
    {
        listener->logQueryStatusChange(queryId, Execution::QueryStatus::Started, startTimestamp);
    }
    else
    {
        /// The move did not happen.
        INVARIANT(
            state->is<Terminated>(),
            "Bug: There is no other option for the state. The only transition from reserved to Starting happens here. Starting will "
            "not "
            "transition into running until the callback is dropped.");
        RunningQueryPlan::dispose(std::move(runningQueryPlan));
    }
}

void QueryCatalog::stopQuery(QueryId id)
{
    const std::unique_ptr<RunningQueryPlan> toBeDeleted;
    {
        CallbackRef ref;
        const std::scoped_lock lock(mutex);
        if (auto it = queryStates.find(id); it != queryStates.end())
        {
            auto& state = *it->second;
            state.transition(
                [&ref](Starting&& starting)
                {
                    auto [stoppingQueryPlan, callback] = RunningQueryPlan::stop(std::move(starting.plan));
                    ref = callback;
                    return Stopping{std::move(stoppingQueryPlan)};
                },
                [&ref](Running&& running)
                {
                    auto [stoppingQueryPlan, callback] = RunningQueryPlan::stop(std::move(running.plan));
                    ref = callback;
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
