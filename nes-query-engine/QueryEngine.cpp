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
#include <Executable.hpp>
#include <InstantiatedQueryPlan.hpp>
#include <Interfaces.hpp>
#include <PipelineExecutionContext.hpp>
#include <QueryEngine.hpp>
#include <QueryEngineConfiguration.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <RunningQueryPlan.hpp>
#include <Task.hpp>

// #define DEBUG_THREAD_SWITCH(PROB) \
//     do \
//     { \
//         if (rand() < RAND_MAX / PROB) \
//         { \
//             ENGINE_LOG_INFO("Sleep"); \
//             std::this_thread::sleep_for(std::chrono::milliseconds(100)); \
//         } \
//     } while (0)
#define DEBUG_THREAD_SWITCH(PROB)

namespace NES::Runtime
{


/// The InstantiatedQueryPlan moved into a RunningQueryPlan.
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
        Failure,
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
        std::unique_ptr<InstantiatedQueryPlan> plan,
        const std::shared_ptr<AbstractQueryStatusListener>& listener,
        QueryLifetimeController& controller,
        WorkEmitter& emitter);
    QueryId registerQuery(std::unique_ptr<InstantiatedQueryPlan>);
    void stopQuery(QueryId queryId);
    void clear()
    {
        std::scoped_lock const lock(mutex);
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
    std::function<void(const Memory::TupleBuffer& tb, ContinuationPolicy)> handler;
    std::shared_ptr<Memory::AbstractBufferProvider> bm;
    size_t numberOfThreads;
    WorkerThreadId threadId;
    PipelineId pipelineId;

    DefaultPEC(
        size_t numberOfThreads,
        WorkerThreadId threadId,
        PipelineId pipelineId,
        std::shared_ptr<Memory::AbstractBufferProvider> bm,
        std::function<void(const Memory::TupleBuffer& tb, ContinuationPolicy)> handler)
        : handler(std::move(handler)), bm(std::move(bm)), numberOfThreads(numberOfThreads), threadId(threadId), pipelineId(pipelineId)
    {
    }

    [[nodiscard]] WorkerThreadId getId() const override { return threadId; }
    Memory::TupleBuffer allocateTupleBuffer() override { return bm->getBufferBlocking(); }
    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return numberOfThreads; }
    void emitBuffer(const Memory::TupleBuffer& buffer, ContinuationPolicy policy) override { handler(buffer, policy); }
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
/// - ThreadPool owns the Queue.
///     - As long as any thread is alive the Queue Needs to Exist
/// - ThreadPool has to outlive all Queries
class ThreadPool : public WorkEmitter, public QueryLifetimeController
{
public:
    void addThread();

    void emitWork(
        QueryId qid,
        const std::shared_ptr<RunningQueryPlanNode>& node,
        Memory::TupleBuffer buffer,
        onComplete complete,
        onFailure failure) override
    {
        auto updatedCount = node->pendingTasks.fetch_add(1) + 1;
        ENGINE_LOG_DEBUG("Increasing number of pending tasks on pipeline {}-{} to {}", qid, node->id, updatedCount);
        taskQueue.blockingWrite(WorkTask(
            qid,
            node->id,
            node,
            buffer,
            [qid, complete = std::move(complete), node = std::weak_ptr(node)]()
            {
                if (auto existingNode = node.lock())
                {
                    auto updatedCount = existingNode->pendingTasks.fetch_sub(1) - 1;
                    ((void)qid);
                    ENGINE_LOG_DEBUG("Decreasing number of pending tasks on pipeline {}-{} to {}", qid, existingNode->id, updatedCount);
                    INVARIANT(updatedCount >= 0, "ThreadPool returned a negative number of pending tasks.");
                }
                else
                {
                    ENGINE_LOG_WARNING("Node Expired and pendingTasks could not be reduced");
                }
                if (complete)
                {
                    complete();
                }
            },
            [qid, failure = std::move(failure), node = std::weak_ptr(node)](auto exception)
            {
                if (auto existingNode = node.lock())
                {
                    auto updatedCount = existingNode->pendingTasks.fetch_sub(1) - 1;
                    ENGINE_LOG_DEBUG("Decreasing number of pending tasks on pipeline {}-{} to {}", qid, existingNode->id, updatedCount);
                    INVARIANT(updatedCount >= 0, "ThreadPool returned a negative number of pending tasks.");
                    exception.what() += fmt::format(" In Pipeline: {}.", existingNode->id);
                    existingNode->fail(exception);
                }
                else
                {
                    ENGINE_LOG_WARNING("Node Expired and pendingTasks could not be reduced");
                }

                if (failure)
                {
                    failure(exception);
                }
            }));
    }

    void emitPipelineStart(QueryId qid, const std::shared_ptr<RunningQueryPlanNode>& node, onComplete complete, onFailure failure) override
    {
        taskQueue.blockingWrite(StartPipelineTask(qid, node->id, complete, failure, node));
    }

    void emitPipelineStop(QueryId qid, std::unique_ptr<RunningQueryPlanNode> node, onComplete complete, onFailure failure) override
    {
        taskQueue.blockingWrite(StopPipelineTask(qid, std::move(node), complete, failure));
    }

    void initializeSourceFailure(QueryId id, OriginId sourceId, std::weak_ptr<RunningSource> source, Exception exception) override
    {
        taskQueue.blockingWrite(FailSourceTask{
            id,
            std::move(source),
            std::move(exception),
            [id, sourceId, listener = listener] { listener->logSourceTermination(id, sourceId, QueryTerminationType::Failure); },
            {}});
    }

    void initializeSourceStop(QueryId id, OriginId sourceId, std::weak_ptr<RunningSource> source) override
    {
        taskQueue.blockingWrite(StopSourceTask{
            id,
            std::move(source),
            [id, sourceId, listener = listener] { listener->logSourceTermination(id, sourceId, QueryTerminationType::Graceful); },
            {}});
    }

    void emitPendingPipelineStop(
        QueryId queryId, const std::shared_ptr<RunningQueryPlanNode>& node, onComplete complete, onFailure failure) override
    {
        ENGINE_LOG_DEBUG("Inserting Pending Pipeline Stop for {}-{}", queryId, node->id);
        taskQueue.blockingWrite(PendingPipelineStop{queryId, std::move(node), 0, std::move(complete), std::move(failure)});
    }

    ThreadPool(
        std::shared_ptr<AbstractQueryStatusListener> listener,
        std::shared_ptr<QueryEngineStatisticListener> stats,
        std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider,
        const size_t taskQueueSize)
        : listener(std::move(listener))
        , statistic(std::move(std::move(stats)))
        , bufferProvider(std::move(bufferProvider))
        , taskQueue(taskQueueSize)
    {
    }


private:
    struct Worker
    {
        /// Handler for different Pipeline Tasks
        /// Boolean return value indicates if the onComplete should be called
        bool operator()(const WorkTask& task) const;
        bool operator()(const StopQueryTask& stopQuery) const;
        bool operator()(StartQueryTask& startQuery) const;
        bool operator()(const StartPipelineTask& startPipeline) const;
        bool operator()(PendingPipelineStop pendingPipelineStop) const;
        bool operator()(const StopPipelineTask& stopPipeline) const;
        bool operator()(const StopSourceTask& stopSource) const;
        bool operator()(const FailSourceTask& failSource) const;

        [[nodiscard]] Worker(WorkerThreadId threadId, ThreadPool& pool, bool terminating)
            : threadId(std::move(threadId)), pool(pool), terminating(terminating)
        {
        }

    private:
        WorkerThreadId threadId;
        ThreadPool& pool; ///NOLINT The ThreadPool will always outlive the worker and not move.
        bool terminating{};
    };

    /// Order of destruction matters: TaskQueue has to outlive the pool
    std::shared_ptr<AbstractQueryStatusListener> listener;
    std::shared_ptr<QueryEngineStatisticListener> statistic;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    std::atomic<TaskId::Underlying> taskIdCounter;
    detail::Queue taskQueue;
    std::vector<std::jthread> pool;
    friend class QueryEngine;
};

bool ThreadPool::Worker::operator()(const WorkTask& task) const
{
    if (terminating)
    {
        ENGINE_LOG_DEBUG("Skipped Task during termination");
        return false;
    }

    auto const taskId = TaskId(pool.taskIdCounter++);

    DEBUG_THREAD_SWITCH(50);

    if (auto pipeline = task.pipeline.lock())
    {
        ENGINE_LOG_DEBUG("Handle Task for {}-{}. Tuples: {}", task.queryId, pipeline->id, task.buf.getNumberOfTuples());
        DefaultPEC pec(
            pool.pool.size(),
            this->threadId,
            pipeline->id,
            pool.bufferProvider,
            [&](const Memory::TupleBuffer& tupleBuffer, auto)
            {
                ENGINE_LOG_DEBUG(
                    "Task emitted tuple buffer {}-{}. Tuples: {}", task.queryId, task.pipelineId, tupleBuffer.getNumberOfTuples());
                for (const auto& successor : pipeline->successors)
                {
                    pool.statistic->onEvent(TaskEmit{threadId, taskId, pipeline->id, successor->id, task.queryId});
                    pool.emitWork(task.queryId, successor, tupleBuffer, {}, {});
                }
            });
        pool.statistic->onEvent(TaskExecutionStart{
            threadId, taskId, task.buf.getNumberOfTuples(), pipeline->id, task.queryId, std::chrono::system_clock::now()});
        pipeline->stage->execute(task.buf, pec);
        pool.statistic->onEvent(TaskExecutionComplete{threadId, taskId, pipeline->id, task.queryId, std::chrono::system_clock::now()});
        return true;
    }

    ENGINE_LOG_WARNING(
        "Task {} for Query {}-{} is expired. Tuples: {}", taskId, task.queryId, task.pipelineId, task.buf.getNumberOfTuples());
    pool.statistic->onEvent(TaskExpired{threadId, taskId, task.pipelineId, task.queryId});
    return false;
}

bool ThreadPool::Worker::operator()(const StartPipelineTask& startPipeline) const
{
    if (terminating)
    {
        ENGINE_LOG_DEBUG("Pipeline Setup was skipped during Termination");
        return false;
    }

    if (auto pipeline = startPipeline.pipeline.lock())
    {
        ENGINE_LOG_DEBUG("Setup Pipeline Task for Query {}-{}", startPipeline.queryId, pipeline->id);
        DefaultPEC pec(
            pool.pool.size(),
            this->threadId,
            pipeline->id,
            pool.bufferProvider,
            [](const auto&, auto)
            {
                /// Catch Emits, that are currently not supported during pipeline stage initilialization.
                INVARIANT(
                    false,
                    "Currently we do not assume that a pipeline can emit data during setup. All pipeline initializations happen "
                    "concurrently and there is not guarantee that the successor pipeline has been initialized");
            });
        pipeline->stage->start(pec);
        pool.statistic->onEvent(PipelineStart{threadId, pipeline->id, startPipeline.queryId});
        return true;
    }

    ENGINE_LOG_DEBUG("Setup pipeline is expired for {}-{}", startPipeline.queryId, startPipeline.pipelineId);
    return false;
}

bool ThreadPool::Worker::operator()(PendingPipelineStop pendingPipelineStop) const
{
    INVARIANT(pendingPipelineStop.pipeline->requireTermination, "Pending Pipeline Stop should always require a non-terminated pipeline");
    INVARIANT(pendingPipelineStop.pipeline->pendingTasks >= 0);

    if (pendingPipelineStop.pipeline->pendingTasks != 0)
    {
        ENGINE_LOG_WARNING(
            "Pipeline {}-{} is still active: {}",
            pendingPipelineStop.queryId,
            pendingPipelineStop.pipeline->id,
            pendingPipelineStop.pipeline->pendingTasks);
        pool.taskQueue.blockingWrite(std::move(pendingPipelineStop));
    }

    return true;
}

bool ThreadPool::Worker::operator()(const StopPipelineTask& stopPipeline) const
{
    ENGINE_LOG_DEBUG("Stop Pipeline Task for Query {}", stopPipeline.queryId);
    DefaultPEC pec(
        pool.pool.size(),
        this->threadId,
        stopPipeline.pipeline->id,
        pool.bufferProvider,
        [&](const Memory::TupleBuffer& tupleBuffer, auto)
        {
            if (terminating)
            {
                ENGINE_LOG_WARNING("Dropping tuple buffer during query engine termination");
                return;
            }

            for (const auto& successor : stopPipeline.pipeline->successors)
            {
                /// The Termination Exceution Context appends a strong reference to the successer into the Task.
                /// This prevents the successor nodes to be destructed before they were able process tuplebuffer generated during
                /// pipeline termination.
                pool.emitWork(stopPipeline.queryId, successor, tupleBuffer, [ref = successor] {}, {});
            }
        });

    ENGINE_LOG_DEBUG("Stopping Pipeline {}-{}", stopPipeline.queryId, stopPipeline.pipeline->id);
    stopPipeline.pipeline->stage->stop(pec);
    pool.statistic->onEvent(PipelineStop{threadId, stopPipeline.pipeline->id, stopPipeline.queryId});
    return true;
}

bool ThreadPool::Worker::operator()(const StopQueryTask& stopQuery) const
{
    ENGINE_LOG_INFO("Terminate Query Task for Query {}", stopQuery.queryId);
    if (auto queryCatalog = stopQuery.catalog.lock())
    {
        queryCatalog->stopQuery(stopQuery.queryId);
        pool.statistic->onEvent(QueryStop{threadId, stopQuery.queryId});
        return true;
    }
    return false;
}

bool ThreadPool::Worker::operator()(StartQueryTask& startQuery) const
{
    ENGINE_LOG_INFO("Start Query Task for Query {}", startQuery.queryId);
    if (auto queryCatalog = startQuery.catalog.lock())
    {
        queryCatalog->start(startQuery.queryId, std::move(startQuery.queryPlan), pool.listener, pool, pool);
        pool.statistic->onEvent(QueryStart{threadId, startQuery.queryId});
        return true;
    }
    return false;
}

bool ThreadPool::Worker::operator()(const StopSourceTask& stopSource) const
{
    if (auto source = stopSource.target.lock())
    {
        ENGINE_LOG_DEBUG("Stop Source Task for Query {} Source {}", stopSource.queryId, source->getOriginId());
        source->stop();
        return true;
    }

    ENGINE_LOG_WARNING("Stop Source Task for Query {} and expired source", stopSource.queryId);
    return false;
}

bool ThreadPool::Worker::operator()(const FailSourceTask& failSource) const
{
    if (auto source = failSource.target.lock())
    {
        ENGINE_LOG_DEBUG("Fail Source Task for Query {} Source {}", failSource.queryId, source->getOriginId());
        source->fail(failSource.exception);
        return true;
    }
    return false;
}


/// This parameter controls how often a thread needs to wake up from polling on the TaskQueue to check the stopToken
constexpr std::chrono::milliseconds QUEUE_POLL_DURATION{10};

void ThreadPool::addThread()
{
    pool.emplace_back(
        [this, id = pool.size()](const std::stop_token& stopToken)
        {
            setThreadName(fmt::format("WorkerThread-{}", id));
            Worker worker{WorkerThreadId(id), *this, false};
            while (!stopToken.stop_requested())
            {
                Task task;
                if (!taskQueue.tryReadUntil(std::chrono::high_resolution_clock::now() + QUEUE_POLL_DURATION, task))
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

            ENGINE_LOG_INFO("Shutting down ThreadPool")
            /// Worker in termination mode will emit further work and eventually clear the task queue and terminate.
            Worker terminatingWorker{WorkerThreadId(id), *this, true};
            while (true)
            {
                Task task;
                if (!taskQueue.readIfNotEmpty(task))
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
    for (size_t i = 0; i < config.numberOfWorkerThreads.getValue(); i++)
    {
        threadPool->addThread();
    }
}

/// NOLINTNEXTLINE Intentionally non-const
void QueryEngine::stop(QueryId queryId)
{
    threadPool->taskQueue.blockingWrite(StopQueryTask{queryId, queryCatalog, {}, {}});
}

/// NOLINTNEXTLINE Intentionally non-const
void QueryEngine::start(std::unique_ptr<InstantiatedQueryPlan> qep)
{
    ENGINE_LOG_INFO("Starting Query: {}", fmt::streamed(*qep));
    threadPool->taskQueue.blockingWrite(StartQueryTask{qep->queryId, std::move(qep), queryCatalog, {}, {}});
}

QueryEngine::~QueryEngine()
{
    queryCatalog->clear();
}

void QueryCatalog::start(
    QueryId queryId,
    std::unique_ptr<InstantiatedQueryPlan> plan,
    const std::shared_ptr<AbstractQueryStatusListener>& listener,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    std::scoped_lock const lock(mutex);
    struct RealQueryLifeTimeListener : QueryLifetimeListener
    {
        RealQueryLifeTimeListener(QueryId queryId, std::shared_ptr<AbstractQueryStatusListener> listener)
            : listener(std::move(listener)), queryId(queryId)
        {
        }

        void onRunning() override
        {
            ENGINE_LOG_DEBUG("Query {} onRunning", queryId);
            listener->logQueryStatusChange(queryId, Execution::QueryStatus::Running);
            if (auto locked = state.lock())
            {
                locked->transition([](Starting&& starting) { return Running{std::move(starting.plan)}; });
            }
        }
        void onFailure(Exception exception) override
        {
            ENGINE_LOG_DEBUG("Query {} onFailure", queryId);
            if (auto locked = state.lock())
            {
                auto successfulTransition = locked->transition(
                    [](Starting&& starting)
                    {
                        RunningQueryPlan::dispose(std::move(starting.plan));
                        return Terminated{Terminated::Failure};
                    },
                    [](Running&& running)
                    {
                        RunningQueryPlan::dispose(std::move(running.plan));
                        return Terminated{Terminated::Failure};
                    },
                    [](Stopping&& stopping)
                    {
                        StoppingQueryPlan::dispose(std::move(stopping.plan));
                        return Terminated{Terminated::Failure};
                    });

                if (successfulTransition)
                {
                    exception.what() += fmt::format(" In Query {}.", queryId);
                    ENGINE_LOG_ERROR("Query Failed: {}", exception.what());
                    listener->logQueryFailure(queryId, std::move(exception));
                }
            }
        }
        /// OnDestruction is called when the entire query graph is terminated.
        void onDestruction() override
        {
            ENGINE_LOG_DEBUG("Query {} onDestruction", queryId);
            if (auto locked = state.lock())
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
                ENGINE_LOG_DEBUG("Query {} Stopped", queryId);
                listener->logQueryStatusChange(queryId, Execution::QueryStatus::Stopped);
            }
            else
            {
                ENGINE_LOG_WARNING("QueryState {} is expired", queryId);
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
    std::unique_ptr<RunningQueryPlan> const toBeDeleted;
    {
        CallbackRef ref;
        std::scoped_lock const lock(mutex);
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
    }
}
}
