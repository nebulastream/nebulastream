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

#include <functional>
#include <memory>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/AbstractQueryStatusListener.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/AtomicState.hpp>
#include <Util/ThreadNaming.hpp>
#include <ErrorHandling.hpp>
#include <Executable.hpp>
#include <InstantiatedQueryPlan.hpp>
#include <Interfaces.hpp>
#include <QueryEngine.hpp>
#include <RunningQueryPlan.hpp>
#include <Task.hpp>

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
        std::shared_ptr<AbstractQueryStatusListener> listener,
        QueryLifetimeController& controller,
        WorkEmitter& emitter);
    QueryId registerQuery(std::unique_ptr<InstantiatedQueryPlan>);
    void stopQuery(QueryId queryId);
    void clear()
    {
        std::scoped_lock lock(mutex);
        queryStates.clear();
    }

private:
    std::atomic<QueryId::Underlying> queryIdCounter = QueryId::INITIAL;
    std::recursive_mutex mutex;
    std::unordered_map<QueryId, State> queryStates;
};

namespace detail
{
using Queue = folly::MPMCQueue<Operation>;
}

struct DefaultPEC : NES::Runtime::Execution::PipelineExecutionContext
{
    std::vector<std::shared_ptr<RunningQueryPlanNode>>& successors;
    detail::Queue& queue;
    std::shared_ptr<Memory::AbstractBufferProvider> bm;
    std::vector<std::shared_ptr<NES::Runtime::Execution::OperatorHandler>>* operatorHandlers = nullptr;
    QueryId queryId;
    DefaultPEC(
        QueryId queryId,
        std::shared_ptr<Memory::AbstractBufferProvider> bm,
        detail::Queue& queue,
        std::vector<std::shared_ptr<RunningQueryPlanNode>>& successors)
        : successors(successors), queue(queue), bm(std::move(bm)), queryId(queryId)
    {
    }

    WorkerThreadId getId() const override { return WorkerThreadId(1); }
    Memory::TupleBuffer allocateTupleBuffer() override { return bm->getBufferBlocking(); }
    uint64_t getNumberOfWorkerThreads() override { return 1; }
    std::shared_ptr<Memory::AbstractBufferProvider> getBufferManager() override { return bm; }
    PipelineId getPipelineID() override { return INVALID<PipelineId>; }
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
        QueryId qid, std::weak_ptr<RunningQueryPlanNode> node, Memory::TupleBuffer buffer, onComplete complete, onFailure failure) override
    {
        taskQueue.blockingWrite(Task(qid, node, buffer, complete, failure));
    }

    void emitSetup(QueryId qid, std::weak_ptr<RunningQueryPlanNode> node, onComplete complete, onFailure failure) override
    {
        taskQueue.blockingWrite(SetupPipeline(qid, node, complete, failure));
    }

    void emitStop(QueryId qid, std::unique_ptr<RunningQueryPlanNode> node, onComplete complete, onFailure failure) override
    {
        taskQueue.blockingWrite(StopPipeline(qid, std::move(node), complete, failure));
    }

    void initializeSourceFailure(QueryId id, OriginId sourceId, std::weak_ptr<RunningSource> source, Exception exception) override
    {
        taskQueue.blockingWrite(FailSource{id, std::move(source), std::move(exception), [id, sourceId, listener = listener] {
                                               listener->logSourceTermination(id, sourceId, QueryTerminationType::Failure);
                                           }});
    }

    void initializeSourceStop(QueryId id, OriginId sourceId, std::weak_ptr<RunningSource> source) override
    {
        taskQueue.blockingWrite(StopSource{id, std::move(source), [id, sourceId, listener = listener] {
                                               listener->logSourceTermination(id, sourceId, QueryTerminationType::Graceful);
                                           }});
    }

    ThreadPool(std::shared_ptr<AbstractQueryStatusListener> listener, std::shared_ptr<Memory::AbstractBufferProvider> buffer_provider)
        : listener(std::move(listener)), bufferProvider(std::move(buffer_provider))
    {
    }

private:
    struct Worker
    {
        /// Handler for different Pipeline Tasks
        /// Boolean return value indicates if the onComplete should be called
        bool operator()(const Task& task);
        bool operator()(const Terminate& terminate);
        bool operator()(Start& start);
        bool operator()(const SetupPipeline& setup);
        bool operator()(const StopPipeline& stop);
        bool operator()(const StopSource& sourceStop);
        bool operator()(const FailSource& sourceStop);
        ThreadPool& pool;
        bool terminating;
    };

    /// Order of destruction matters: TaskQueue has to outlive the pool
    std::shared_ptr<AbstractQueryStatusListener> listener;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    detail::Queue taskQueue{1000};
    std::vector<std::jthread> pool;
    friend class QueryEngine;
};

bool ThreadPool::Worker::operator()(const Task& task)
{
    if (terminating)
    {
        NES_DEBUG("Skipped Task during termination");
        return false;
    }

    NES_DEBUG("Handle Task for Query {}", task.queryId);
    struct TaskPEC : DefaultPEC
    {
        TaskPEC(
            QueryId queryId,
            std::shared_ptr<Memory::AbstractBufferProvider> bm,
            detail::Queue& queue,
            std::vector<std::shared_ptr<RunningQueryPlanNode>>& successors)
            : DefaultPEC(queryId, std::move(bm), queue, successors)
        {
        }

        void emitBuffer(const Memory::TupleBuffer& tupleBuffer, ContinuationPolicy) override
        {
            for (const auto& successor : successors)
            {
                queue.blockingWrite(Task{queryId, successor, tupleBuffer});
            }
        }
    };

    if (auto pipeline = task.pipeline.lock())
    {
        TaskPEC pec(task.queryId, pool.bufferProvider, pool.taskQueue, pipeline->successors);
        pipeline->stage->execute(task.buf, pec);
        return true;
    }
    else
    {
        NES_DEBUG("Task pipeline is expired");
        return false;
    }
}

bool ThreadPool::Worker::operator()(const SetupPipeline& task)
{
    if (terminating)
    {
        NES_DEBUG("Pipeline Setup was skipped during Termination");
        return false;
    }

    NES_DEBUG("Setup Pipeline Task for Query {}", task.queryId);
    /// Catch Emits, that are currently not supported during pipeline stage initilialization.
    struct SetupPEC : DefaultPEC
    {
        SetupPEC(
            QueryId queryId,
            std::shared_ptr<Memory::AbstractBufferProvider> bm,
            detail::Queue& queue,
            std::vector<std::shared_ptr<RunningQueryPlanNode>>& successors)
            : DefaultPEC(queryId, std::move(bm), queue, successors)
        {
        }

        void emitBuffer(const Memory::TupleBuffer&, ContinuationPolicy) override
        {
            INVARIANT(
                false,
                "Currently we do not assume that a pipeline can emit data during setup. All pipeline initializations happen "
                "concurrently and there is not guarantee that the successor pipeline has been initialized");
        }
    };

    if (auto pipeline = task.pipeline.lock())
    {
        SetupPEC pec(task.queryId, pool.bufferProvider, pool.taskQueue, pipeline->successors);
        pipeline->stage->setup(pec);
        return true;
    }
    else
    {
        NES_DEBUG("Setup pipeline is expired");
        return false;
    }
}

bool ThreadPool::Worker::operator()(const StopPipeline& task)
{
    NES_DEBUG("Stop Pipeline Task for Query {}", task.queryId);
    struct TerminationPEC : DefaultPEC
    {
        TerminationPEC(
            QueryId queryId,
            std::shared_ptr<Memory::AbstractBufferProvider> bm,
            detail::Queue& queue,
            std::vector<std::shared_ptr<RunningQueryPlanNode>>& successors,
            bool is_terminating)
            : DefaultPEC(queryId, std::move(bm), queue, successors), isTerminating(is_terminating)
        {
        }

        bool isTerminating;

        void emitBuffer(const Memory::TupleBuffer& tupleBuffer, ContinuationPolicy) override
        {
            if (isTerminating)
            {
                NES_WARNING("Dropping tuple buffer during query engine termination");
                return;
            }

            for (const auto& successor : successors)
            {
                /// The Termination Exceution Context appends a strong reference to the successer into the Task.
                /// This prevents the successor nodes to be destructed before they were able process tuplebuffer generated during
                /// pipeline termination.
                queue.blockingWrite(Task{queryId, successor, tupleBuffer, [ref = successor]() { (void)ref; }});
            }
        }
    };

    TerminationPEC pec(task.queryId, pool.bufferProvider, pool.taskQueue, task.pipeline->successors, terminating);
    NES_DEBUG("Stopping Pipeline");
    task.pipeline->stage->stop(pec);
    return true;
}

bool ThreadPool::Worker::operator()(const Terminate& terminate)
{
    NES_INFO("Terminate Query Task for Query {}", terminate.queryId);
    if (auto qc = terminate.catalog.lock())
    {
        qc->stopQuery(terminate.queryId);
        return true;
    }
    return false;
}

bool ThreadPool::Worker::operator()(Start& start)
{
    NES_INFO("Start Query Task for Query {}", start.queryId);
    if (auto qc = start.catalog.lock())
    {
        qc->start(start.queryId, std::move(start.qep), pool.listener, pool, pool);
        return true;
    }
    return false;
}

bool ThreadPool::Worker::operator()(const StopSource& stopSource)
{
    if (auto source = stopSource.target.lock())
    {
        NES_DEBUG("Stop Source Task for Query {} Source {}", stopSource.queryId, source->getOriginId());
        source->stop();
        return true;
    }

    NES_WARNING("Stop Source Task for Query {} and expired source", stopSource.queryId);
    return false;
}

bool ThreadPool::Worker::operator()(const FailSource& failSource)
{
    if (auto source = failSource.target.lock())
    {
        NES_DEBUG("Fail Source Task for Query {} Source {}", failSource.queryId, source->getOriginId());
        source->fail(failSource.exception);
        return true;
    }
    return false;
}

void ThreadPool::addThread()
{
    pool.emplace_back(
        [this, id = pool.size()](const std::stop_token& stop_token)
        {
            setThreadName("WorkerThread-%d", id);
            while (!stop_token.stop_requested())
            {
                Worker w{*this, false};
                Operation op;
                if (!taskQueue.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(10), op))
                {
                    continue;
                }
                try
                {
                    if (std::visit(w, op))
                    {
                        std::visit([](const auto& op) { op.complete(); }, op);
                    }
                }
                catch (const Exception& e)
                {
                    std::visit([&e](const auto& op) { op.fail(e); }, op);
                }
            }
            NES_INFO("Shutting down ThreadPool")
            while (true)
            {
                Worker w{*this, true};
                Operation op;
                if (!taskQueue.readIfNotEmpty(op))
                {
                    break;
                }

                try
                {
                    if (std::visit(w, op))
                    {
                        std::visit([](const auto& op) { op.complete(); }, op);
                    }
                }
                catch (const Exception& e)
                {
                    std::visit([&e](const auto& op) { op.fail(e); }, op);
                }
            }
        });
}

QueryEngine::QueryEngine(
    size_t workerThreads, std::shared_ptr<AbstractQueryStatusListener> listener, std::shared_ptr<Memory::BufferManager> bm)
    : bufferManager(std::move(bm))
    , statusListener(std::move(listener))
    , qc(std::make_shared<QueryCatalog>())
    , threadPool(std::make_unique<ThreadPool>(statusListener, bufferManager))
{
    for (size_t i = 0; i < workerThreads; i++)
    {
        threadPool->addThread();
    }
}

void QueryEngine::stop(QueryId queryId)
{
    threadPool->taskQueue.blockingWrite(Terminate{queryId, qc});
}

void QueryEngine::start(QueryId queryId, std::unique_ptr<InstantiatedQueryPlan> qep)
{
    threadPool->taskQueue.blockingWrite(Start{queryId, std::move(qep), qc});
}

QueryEngine::~QueryEngine()
{
    qc->clear();
}

void QueryCatalog::start(
    QueryId queryId,
    std::unique_ptr<InstantiatedQueryPlan> plan,
    std::shared_ptr<AbstractQueryStatusListener> listener,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    std::scoped_lock lock(mutex);
    struct RealQueryLifeTimeListener : QueryLifetimeListener
    {
        RealQueryLifeTimeListener(QueryId queryId, std::shared_ptr<AbstractQueryStatusListener> listener)
            : listener(std::move(listener)), queryId(queryId)
        {
        }

        void onRunning() override
        {
            NES_DEBUG("Query {} onRunning", queryId);
            listener->logQueryStatusChange(queryId, Execution::QueryStatus::Running);
            if (auto locked = state.lock())
            {
                locked->transition([](Starting&& s) { return Running{std::move(s.plan)}; });
            }
        }
        void onFailure(Exception e) override
        {
            NES_DEBUG("Query {} onFailure", queryId);
            if (auto locked = state.lock())
            {
                locked->transition(
                    [](Starting&& s)
                    {
                        RunningQueryPlan::dispose(std::move(s.plan));
                        return Terminated{Terminated::Failure};
                    },
                    [](Running&& r)
                    {
                        RunningQueryPlan::dispose(std::move(r.plan));
                        return Terminated{Terminated::Failure};
                    },
                    [](Stopping&& s)
                    {
                        StoppingQueryPlan::dispose(std::move(s.plan));
                        return Terminated{Terminated::Failure};
                    });
                listener->logQueryFailure(queryId, std::move(e));
            }
        }
        /// OnDestruction is called when the entire query graph is terminated.
        void onDestruction() override
        {
            NES_DEBUG("Query {} onDestruction", queryId);
            if (auto locked = state.lock())
            {
                locked->transition(
                    [](Starting&& s)
                    {
                        RunningQueryPlan::dispose(std::move(s.plan));
                        return Terminated{Terminated::Stopped};
                    },
                    [](Running&& r)
                    {
                        RunningQueryPlan::dispose(std::move(r.plan));
                        return Terminated{Terminated::Stopped};
                    },
                    [](Stopping&& s)
                    {
                        StoppingQueryPlan::dispose(std::move(s.plan));
                        return Terminated{Terminated::Stopped};
                    });
                NES_DEBUG("Query {} Stopped", queryId);
                listener->logQueryStatusChange(queryId, Execution::QueryStatus::Stopped);
            }
            else
            {
                NES_WARNING("QueryState {} is expired", queryId);
            }
        }

        std::shared_ptr<AbstractQueryStatusListener> listener;
        QueryId queryId;
        WeakStateRef state;
    };

    auto queryListener = std::make_shared<RealQueryLifeTimeListener>(queryId, listener);
    auto [rp, callback] = RunningQueryPlan::start(queryId, std::move(plan), controller, emitter, queryListener);

    auto state = std::make_shared<StateRef>(Starting{std::move(rp)});
    queryListener->state = state;
    this->queryStates.emplace(queryId, state);
}

void QueryCatalog::stopQuery(QueryId id)
{
    std::unique_ptr<RunningQueryPlan> toBeDeleted;
    {
        CallbackRef ref;
        std::scoped_lock lock(mutex);
        if (auto it = queryStates.find(id); it != queryStates.end())
        {
            auto& state = *it->second;
            state.transition(
                [&ref](Starting&& s)
                {
                    auto [q, callback] = RunningQueryPlan::stop(std::move(s.plan));
                    ref = callback;
                    return Stopping{std::move(q)};
                },
                [&ref](Running&& r)
                {
                    auto [q, callback] = RunningQueryPlan::stop(std::move(r.plan));
                    ref = callback;
                    return Stopping{std::move(q)};
                });
        }
    }
}
}
