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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
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
    using State = std::shared_ptr<AtomicState<Starting, Running, Stopping, Terminated>>;
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
        bool potentiallyProcessTheWorkInPlace) override
    {
        [[maybe_unused]] auto updatedCount = node->pendingTasks.fetch_add(1) + 1;
        ENGINE_LOG_DEBUG("Increasing number of pending tasks on pipeline {}-{} to {}", qid, node->id, updatedCount);
        auto task = WorkTask(
            qid,
            node->id,
            node,
            buffer,
            injectReferenceCountReducer(node, std::move(complete)),
            injectQueryFailure(node, injectReferenceCountReducer(node, std::move(failure))));
        if (WorkerThread::id == INVALID<WorkerThreadId>)
        {
            /// Non-WorkerThread
            admissionQueue.blockingWrite(std::move(task));
            return true;
        }

        /// WorkerThread
        if (potentiallyProcessTheWorkInPlace)
        {
            addTaskOrDoItInPlace(std::move(task));
            return true;
        }
        if (internalTaskQueue.tryWriteUntil(std::chrono::high_resolution_clock::now() + std::chrono::seconds(1), std::move(task)))
        {
            return true;
        }
        node->pendingTasks.fetch_sub(1);
        ENGINE_LOG_DEBUG("TaskQueue is full, could not write within 1 second.");
        return false;
    }

    void emitPipelineStart(
        QueryId qid, const std::shared_ptr<RunningQueryPlanNode>& node, BaseTask::onComplete complete, BaseTask::onFailure failure) override
    {
        addTaskAndDoNextForAdmission(StartPipelineTask(qid, node->id, complete, injectQueryFailure(node, failure), node));
    }

    void emitPipelineStop(
        QueryId qid, std::unique_ptr<RunningQueryPlanNode> node, BaseTask::onComplete complete, BaseTask::onFailure failure) override
    {
        auto nodePtr = node.get();
        /// Calling the Unsafe version of injectQueryFailure is required here because the RunningQueryPlan is a unique ptr.
        /// However the StopPipelineTask takes ownership of the Node and thus guarantees that it is alive when the callback is invoked.
        addTaskAndDoNextForAdmission(StopPipelineTask(qid, std::move(node), complete, injectQueryFailureUnsafe(nodePtr, failure)));
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
        addTaskAndDoNextForAdmission(PendingPipelineStopTask{queryId, std::move(node), 0, std::move(complete), std::move(failure)});
    }

    ThreadPool(
        std::shared_ptr<AbstractQueryStatusListener> listener,
        std::shared_ptr<QueryEngineStatisticListener> stats,
        std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider,
        const size_t taskQueueSize)
        : listener(std::move(listener))
        , statistic(std::move(std::move(stats)))
        , bufferProvider(std::move(bufferProvider))
        , admissionQueue(taskQueueSize)
        , internalTaskQueue(taskQueueSize)
    {
    }

    /// Reserves the initial WorkerThreadId for the terminator thread, which is the thread which is calling shutdown.
    /// This allows the thread to access into the internalTaskQueue, which is prohibited for non-worker threads.
    /// The terminator thread does not count towards numberOfThreads
    constexpr static WorkerThreadId terminatorThreadId = INITIAL<WorkerThreadId>;
    [[nodiscard]] size_t numberOfThreads() const { return pool.size(); }

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

    void addTaskOrDoItInPlace(Task&& task)
    {
        PRECONDITION(ThreadPool::WorkerThread::id != INVALID<WorkerThreadId>, "This should only be called from a worker thread");
        if (!internalTaskQueue.write(std::move(task)))
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
    }


    void addTaskAndDoNextForAdmission(Task&& task)
    {
        if (not admissionQueue.tryWriteUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(10), std::move(task)))
        {
            /// The order below is important. We want to make sure that we pick up a next task before we write the current task into the queue.
            Task nextTask;
            const auto hasNextTask = admissionQueue.read(nextTask);
            admissionQueue.blockingWrite(std::move(task)); /// NOLINT no move will happen if tryWriteUntil has failed
            if (hasNextTask)
            {
                WorkerThread worker{*this, false};
                try
                {
                    if (std::visit(worker, nextTask))
                    {
                        completeTask(nextTask);
                    }
                }
                catch (const Exception& exception)
                {
                    failTask(nextTask, exception);
                }
            }
        }
    }

    void addTaskAndDoNext(Task&& task)
    {
        PRECONDITION(ThreadPool::WorkerThread::id != INVALID<WorkerThreadId>, "This should only be called from a worker thread");
        if (not internalTaskQueue.tryWriteUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(10), std::move(task)))
        {
            /// The order below is important. We want to make sure that we pick up a next task before we write the current task into the queue.
            Task nextTask;
            const auto hasNextTask = internalTaskQueue.read(nextTask);
            internalTaskQueue.blockingWrite(std::move(task)); /// NOLINT no move will happen if tryWriteUntil has failed
            if (hasNextTask)
            {
                WorkerThread worker{*this, false};
                try
                {
                    if (std::visit(worker, nextTask))
                    {
                        completeTask(nextTask);
                    }
                }
                catch (const Exception& exception)
                {
                    failTask(nextTask, exception);
                }
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

    std::vector<std::jthread> pool;
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
            pool.pool.size(),
            WorkerThread::id,
            pipeline->id,
            pool.bufferProvider,
            [&](const Memory::TupleBuffer& tupleBuffer, auto policy)
            {
                for (const auto& successor : pipeline->successors)
                {
                    pool.statistic->onEvent(
                        TaskEmit{WorkerThread::id, task.queryId, pipeline->id, successor->id, taskId, tupleBuffer.getNumberOfTuples()});
                    pool.emitWork(
                        task.queryId,
                        successor,
                        tupleBuffer,
                        {},
                        {},
                        policy == Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
                }
                return true;
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
    INVARIANT(pendingPipelineStop.pipeline->pendingTasks >= 0, "Pending Pipeline Stop must have pending tasks, but had 0 pending tasks.");

    if (!pendingPipelineStop.pipeline->requiresTermination)
    {
        /// The decision for a soft stop might have been overruled by a hardstop or system shutdown
        return false;
    }

    if (pendingPipelineStop.pipeline->pendingTasks > 0)
    {
        ENGINE_LOG_TRACE(
            "Pipeline {}-{} is still active: {}",
            pendingPipelineStop.queryId,
            pendingPipelineStop.pipeline->id,
            pendingPipelineStop.pipeline->pendingTasks);
        pool.addTaskAndDoNextForAdmission(std::move(pendingPipelineStop));
    }

    return true;
}

bool ThreadPool::WorkerThread::operator()(const StopPipelineTask& stopPipelineTask) const
{
    ENGINE_LOG_DEBUG("Stop Pipeline Task for {}-{}", stopPipelineTask.queryId, stopPipelineTask.pipeline->id);
    DefaultPEC pec(
        pool.pool.size(),
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
                pool.emitWork(
                    stopPipelineTask.queryId,
                    successor,
                    tupleBuffer,
                    [ref = successor] {},
                    {},
                    policy == Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
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
            pool.admissionQueue.blockingWrite(stopSource);
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
        [this, id = pool.size() + 1](const std::stop_token& stopToken)
        {
            WorkerThread::id = WorkerThreadId(WorkerThreadId::INITIAL + id);
            setThreadName(fmt::format("WorkerThread-{}", id));
            WorkerThread worker{*this, false};
            while (!stopToken.stop_requested())
            {
                Task task;
                /// This timeout controls how often a thread needs to wake up from polling on the TaskQueue to check the stopToken
                if (!internalTaskQueue.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(10), task))
                {
                    if (admissionQueue.read(task))
                    {
                        internalTaskQueue.blockingWrite(std::move(task));
                    }
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
    , threadPool(std::make_unique<ThreadPool>(statusListener, statisticListener, bufferManager, config.taskQueueSize.getValue()))
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
                locked->transition([](Starting&& starting) { return Running{std::move(starting.plan)}; });
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
    auto [runningQueryPlan, callback] = RunningQueryPlan::start(queryId, std::move(plan), controller, emitter, queryListener);

    auto state = std::make_shared<StateRef>(Starting{std::move(runningQueryPlan)});
    queryListener->state = state;
    this->queryStates.emplace(queryId, state);
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
