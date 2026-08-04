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
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
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
#include <Runtime/BufferManager.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/AtomicState.hpp>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <BufferExhaustionPolicy.hpp>
#include <DelayedTaskSubmitter.hpp>
#include <EngineLogger.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <ExecutableQueryPlan.hpp>
#include <Interfaces.hpp>
#include <PipelineExecutionContext.hpp>
#include <QueryEngineConfiguration.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
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
    ENGINE_IF_LOG_DEBUG(QueryId qid, ) std::weak_ptr<RunningQueryPlanNode> node, TaskCallback::onComplete innerFunction)
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
    enum TerminationReason : uint8_t
    {
        Failed,
        Stopped
    };

    TerminationReason reason;
};

class QueryPlanReaper;

class QueryCatalog
{
public:
    using State = std::shared_ptr<AtomicState<Reserved, Starting, Running, Stopping, Terminated>>;
    using WeakStateRef = State::weak_type;
    using StateRef = State::element_type;

    QueryCatalog(std::shared_ptr<AbstractQueryStatusListener> listener, std::shared_ptr<QueryEngineStatisticListener> statistic)
        : listener(std::move(listener)), statistic(std::move(statistic))
    {
    }

    void start(
        QueryId queryId,
        std::unique_ptr<ExecutableQueryPlan> plan,
        const std::shared_ptr<AbstractQueryStatusListener>& listener,
        const std::shared_ptr<QueryEngineStatisticListener>& statistic,
        QueryLifetimeController& controller,
        WorkEmitter& emitter);
    void stopQuery(QueryId queryId);

    /// Terminates a query by id with an error: transitions it to Terminated::Failed, reports the failure exactly once
    /// (guarded by the transition), and hands the blocking disposal of its plan to the QueryPlanReaper. Used by the
    /// buffer-exhaustion arbiter to shed a victim query. Thread-safe.
    void failQuery(QueryId queryId, Exception exception);

    /// Chooses a victim query to terminate when the buffer pool is exhausted, per the given policy, or nullopt if there
    /// is no eligible victim. currentQuery is the query whose worker hit the exhaustion (used by TERMINATE_SELF and as
    /// a fallback). `buffersHeld` maps each query to the number of pooled buffers it currently holds (maintained by the
    /// per-query buffer providers); queries without an entry hold none. Thread-safe.
    std::optional<QueryId>
    selectVictim(BufferExhaustionPolicy policy, QueryId currentQuery, const std::unordered_map<QueryId, uint64_t>& buffersHeld);

    /// The reaper performs the blocking disposal of plans terminated via failQuery. Must be attached before any query
    /// is started.
    void attachReaper(QueryPlanReaper* planReaper) { this->reaper = planReaper; }

    void clear()
    {
        const std::scoped_lock lock(mutex);
        queryStates.clear();
    }

private:
    /// Queries eligible to be terminated on buffer-pool exhaustion. Running, Starting and Stopping queries all hold
    /// buffers: under tiny-pool startup contention the offender is often still Starting, and a Stopping query can hold
    /// many buffers while flushing (e.g. a large window) -- StopPipelineTasks allocate through the arbiter as well.
    /// failQuery supports the Stopping -> Terminated::Failed transition, so terminating a Stopping victim is safe.
    /// The state may change between this check and the kill; failQuery re-checks atomically and is a no-op on
    /// Terminated queries. Must be called while holding `mutex`.
    std::vector<QueryId> gatherVictimCandidates();

    /// The candidate holding the most pooled buffers (ties broken by iteration order). `candidates` must be non-empty.
    static QueryId selectLargest(const std::vector<QueryId>& candidates, const std::unordered_map<QueryId, uint64_t>& buffersHeld);

    std::recursive_mutex mutex;
    std::unordered_map<QueryId, State> queryStates;
    std::shared_ptr<AbstractQueryStatusListener> listener;
    std::shared_ptr<QueryEngineStatisticListener> statistic;
    QueryPlanReaper* reaper = nullptr; ///NOLINT owned by the QueryEngine; attached right after construction
};

/// Disposes terminated query plans on a dedicated thread. Disposal is blocking work: it joins the victim's source
/// threads, which can themselves be blocked allocating from the exhausted pool or in the bounded admission queue. If
/// the worker that detected the exhaustion disposed the victim inline, every worker of a small pool could end up in
/// allocate -> failQuery -> join with nobody left to release a buffer. The initiating worker therefore only
/// *initiates* the kill (state transition + enqueue here); this thread performs the blocking disposal.
class QueryPlanReaper
{
public:
    using Disposable = std::variant<std::unique_ptr<RunningQueryPlan>, std::unique_ptr<StoppingQueryPlan>>;

    explicit QueryPlanReaper(const Host& host) : thread("PlanReaper", host, [this](const std::stop_token&) { run(); }) { }

    ~QueryPlanReaper() { shutdown(); }

    QueryPlanReaper(const QueryPlanReaper&) = delete;
    QueryPlanReaper(QueryPlanReaper&&) = delete;
    QueryPlanReaper& operator=(const QueryPlanReaper&) = delete;
    QueryPlanReaper& operator=(QueryPlanReaper&&) = delete;

    /// Disposes `plan` (destroying it and releasing its buffers) on the reaper thread. After shutdown() the caller
    /// disposes inline instead; at that point failQuery is only reachable from worker threads, which are allowed to
    /// emit the pipeline stops that disposal produces.
    void dispose(Disposable plan)
    {
        {
            std::unique_lock lock(mutex);
            if (!stopped)
            {
                queue.push_back(std::move(plan));
                lock.unlock();
                pending.notify_one();
                return;
            }
        }
        disposeNow(std::move(plan));
    }

    /// Drains outstanding disposals and joins the reaper thread. Idempotent. Must be called while the ThreadPool is
    /// still alive, because disposing a plan emits pipeline-stop tasks into the task queue.
    void shutdown()
    {
        {
            const std::scoped_lock lock(mutex);
            stopped = true;
        }
        pending.notify_all();
        thread = {}; /// joins; run() drains the queue before returning
    }

private:
    /// Defined below ThreadPool: the reaper thread registers itself as an engine thread (terminatorThreadId).
    void run();

    static void disposeNow(Disposable plan)
    {
        std::visit([]<typename T>(T&& owned) { T::element_type::dispose(std::forward<T>(owned)); }, std::move(plan));
    }

    std::mutex mutex;
    std::condition_variable pending;
    std::deque<Disposable> queue;
    bool stopped = false;
    /// Must be the last member: the thread starts immediately and accesses the members above.
    Thread thread;
};

namespace detail
{
using Queue = folly::MPMCQueue<Task>;
}

class BufferExhaustionArbiter;

/// Per-query view onto the global buffer pool, handed to pipelines through the PipelineExecutionContext.
/// 1. Every allocation that would otherwise block indefinitely (getBufferBlocking) routes through the
///    BufferExhaustionArbiter, so *all* pipeline-side allocation sites (Arena, PagedVector, output formatters,
///    SequenceShredder, ...) participate in exhaustion handling, not only DefaultPEC::allocateTupleBuffer.
/// 2. It counts the pooled buffers the query currently holds: incremented on hand-out, decremented on recycle.
///    Buffers are prepared with this provider as their recycler, so the recycle callback flows through
///    recyclePooledBuffer on its way back to the global pool. The arbiter ranks victim candidates on this count.
class QueryBufferProvider final : public AbstractBufferProvider,
                                  public BufferRecycler,
                                  public std::enable_shared_from_this<QueryBufferProvider>
{
public:
    QueryBufferProvider(std::shared_ptr<BufferManager> globalPool, BufferExhaustionArbiter* arbiter, QueryId queryId)
        : globalPool(std::move(globalPool)), arbiter(arbiter), queryId(std::move(queryId))
    {
    }

    [[nodiscard]] QueryId getQueryId() const { return queryId; }
    [[nodiscard]] uint64_t buffersHeld() const { return held.load(std::memory_order_relaxed); }

    /// Counted, non-blocking take from the global pool; the buffer is prepared with this provider as its recycler.
    std::optional<TupleBuffer> takeCounted()
    {
        if (auto buffer = globalPool->getBufferNoBlockingFor(shared_from_this()))
        {
            held.fetch_add(1, std::memory_order_relaxed);
            return buffer;
        }
        return std::nullopt;
    }

    /// Routes through the arbiter; defined after BufferExhaustionArbiter.
    TupleBuffer getBufferBlocking() override;

    std::optional<TupleBuffer> getBufferNoBlocking() override { return takeCounted(); }

    std::optional<TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds timeoutMs) override
    {
        /// Bounded wait: the caller handles the empty optional, so this needs no arbitration.
        if (auto buffer = globalPool->getBufferWithTimeoutFor(timeoutMs, shared_from_this()))
        {
            held.fetch_add(1, std::memory_order_relaxed);
            return buffer;
        }
        return std::nullopt;
    }

    std::optional<TupleBuffer> getUnpooledBuffer(const size_t bufferSize) override { return globalPool->getUnpooledBuffer(bufferSize); }

    BufferManagerType getBufferManagerType() const override { return BufferManagerType::LOCAL; }
    size_t getBufferSize() const override { return globalPool->getBufferSize(); }
    size_t getNumOfPooledBuffers() const override { return globalPool->getNumOfPooledBuffers(); }
    size_t getNumOfUnpooledBuffers() const override { return globalPool->getNumOfUnpooledBuffers(); }
    [[nodiscard]] size_t getNumberOfAvailableBuffers() const override { return globalPool->getNumberOfAvailableBuffers(); }

    void recyclePooledBuffer(detail::MemorySegment* segment) override
    {
        held.fetch_sub(1, std::memory_order_relaxed);
        globalPool->recyclePooledBuffer(segment);
    }

    void recycleUnpooledBuffer(detail::MemorySegment* segment, const AllocationThreadInfo& threadInfo) override
    {
        globalPool->recycleUnpooledBuffer(segment, threadInfo);
    }

private:
    std::shared_ptr<BufferManager> globalPool;
    BufferExhaustionArbiter* arbiter; ///NOLINT owned by the QueryEngine, which outlives the ThreadPool that uses providers
    QueryId queryId;
    std::atomic<uint64_t> held{0};
};

/// Relieves global buffer-pool exhaustion by terminating a victim query instead of deadlocking. All pipeline
/// allocations go through per-query QueryBufferProviders (see providerFor); their blocking path lands in allocate().
///
/// allocate() hands out a buffer while more than `recoveryMargin` buffers remain free. On exhaustion it first waits a
/// bounded grace period for a recycled buffer, so transient pressure (the backpressure the removed getBufferBlocking
/// timeout used to provide) resolves without any kill. Only then does it select and terminate the query holding the
/// most pooled buffers. Victim selection and termination are serialized under `victimMutex`: the killing worker holds
/// it from selection until the victim's buffers drained (or the per-victim recovery window lapsed), and every other
/// exhausted worker re-checks pool availability after acquiring the mutex before it may select another victim -- so
/// concurrent workers cannot shed two queries where one sufficed. If the caller's own query is the chosen victim,
/// allocate() throws QueryBufferExhausted to abort the current task.
///
/// LOCK ORDER: victimMutex -> QueryCatalog::mutex (via selectVictim/failQuery) and victimMutex -> providersMutex
/// (via snapshotBuffersHeld). Neither the catalog nor the providers call back into the arbiter's locks, so the
/// reverse order cannot occur. providersMutex is a leaf lock.
class BufferExhaustionArbiter
{
public:
    BufferExhaustionArbiter(
        std::shared_ptr<BufferManager> bufferProvider, QueryCatalog* catalog, BufferExhaustionPolicy policy, size_t recoveryMargin)
        : bufferProvider(std::move(bufferProvider)), catalog(catalog), policy(policy), recoveryMargin(recoveryMargin)
    {
    }

    /// The per-query buffer provider handed to pipelines of `queryId` (created on first use). Entries are only
    /// reclaimed on engine shutdown, mirroring queryStates in the QueryCatalog; an idle entry is a few bytes.
    std::shared_ptr<QueryBufferProvider> providerFor(QueryId queryId)
    {
        const std::scoped_lock lock(providersMutex);
        auto& provider = providers[queryId];
        if (!provider)
        {
            provider = std::make_shared<QueryBufferProvider>(bufferProvider, this, queryId);
        }
        return provider;
    }

    /// Acquire a buffer for the requesting query, terminating a victim query if the pool stays exhausted. Throws
    /// QueryBufferExhausted if the caller's own query is selected as the victim.
    TupleBuffer allocate(QueryBufferProvider& requester)
    {
        /// Fast path: the pool has slack beyond the recovery margin (which stays free for the teardown path).
        if (hasSlack())
        {
            if (auto buffer = requester.takeCounted())
            {
                return std::move(*buffer);
            }
        }

        /// Exhausted: wait once (bounded) for a recycled buffer before considering any kill.
        waitForSlack(std::chrono::steady_clock::now() + backpressureGrace);

        /// Defensive backstop only: terminating queries frees buffers monotonically (bounded by the number of
        /// queries), so this loop makes progress without it; the deadline just guards against unforeseen wedges.
        const auto deadline = std::chrono::steady_clock::now() + safetyDeadline;
        while (true)
        {
            if (hasSlack())
            {
                if (auto buffer = requester.takeCounted())
                {
                    return std::move(*buffer);
                }
                /// Slack was reported but the take raced with other workers; re-evaluate.
                continue;
            }

            const std::scoped_lock killLock(victimMutex);
            if (hasSlack())
            {
                /// Another worker shed a victim while we waited for the mutex; re-check availability and retry the
                /// normal path before considering another victim.
                continue;
            }

            const auto victim = catalog->selectVictim(policy, requester.getQueryId(), snapshotBuffersHeld());
            if (!victim.has_value() || *victim == requester.getQueryId() || std::chrono::steady_clock::now() >= deadline)
            {
                /// We are the victim (or no other victim exists, or the backstop fired): terminate this query.
                throw QueryBufferExhausted("query {} terminated to relieve buffer-pool exhaustion", requester.getQueryId());
            }
            /// Initiate the kill: failQuery transitions the victim and hands the blocking disposal to the reaper.
            /// Keep holding victimMutex while the victim's (asynchronous) teardown returns buffers, so no other
            /// worker concurrently selects a second victim where this one sufficed.
            catalog->failQuery(
                *victim, QueryBufferExhausted("query {} terminated to relieve buffer-pool exhaustion (selected as victim)", *victim));
            waitForSlack(std::chrono::steady_clock::now() + perVictimRecovery);
        }
    }

private:
    /// Mirrors the global pool's former getBufferBlocking timeout (GET_BUFFER_TIMEOUT), so transiently exhausted
    /// callers self-resolve exactly as before instead of escalating to a kill on first observation.
    static constexpr auto backpressureGrace = std::chrono::milliseconds(1000);
    /// After initiating a kill, wait up to this long for the victim's teardown to return buffers before escalating to
    /// another victim, so the minimum number of queries is shed.
    static constexpr auto perVictimRecovery = std::chrono::milliseconds(1000);
    static constexpr auto safetyDeadline = std::chrono::seconds(10);

    [[nodiscard]] bool hasSlack() const { return bufferProvider->getNumberOfAvailableBuffers() > recoveryMargin; }

    void waitForSlack(const std::chrono::steady_clock::time_point until) const
    {
        while (std::chrono::steady_clock::now() < until && !hasSlack())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    }

    /// Snapshot of the pooled buffers held per query, for victim ranking.
    [[nodiscard]] std::unordered_map<QueryId, uint64_t> snapshotBuffersHeld() const
    {
        const std::scoped_lock lock(providersMutex);
        std::unordered_map<QueryId, uint64_t> held;
        held.reserve(providers.size());
        for (const auto& [queryId, provider] : providers)
        {
            held.emplace(queryId, provider->buffersHeld());
        }
        return held;
    }

    std::shared_ptr<BufferManager> bufferProvider;
    QueryCatalog* catalog;
    BufferExhaustionPolicy policy;
    size_t recoveryMargin;

    /// Serializes victim selection+termination across exhausted workers (see the class comment for the lock order).
    std::mutex victimMutex;
    /// Guards `providers`. Leaf lock: taken from providerFor (per task) and snapshotBuffersHeld (under victimMutex).
    mutable std::mutex providersMutex;
    std::unordered_map<QueryId, std::shared_ptr<QueryBufferProvider>> providers;
};

TupleBuffer QueryBufferProvider::getBufferBlocking()
{
    return arbiter->allocate(*this);
}

struct DefaultPEC final : PipelineExecutionContext
{
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* operatorHandlers = nullptr;
    std::function<bool(const TupleBuffer& tb, ContinuationPolicy)> handler;
    std::function<void(const TupleBuffer& tb, std::chrono::milliseconds duration)> repeatHandler;
    /// The per-query QueryBufferProvider: every allocation (direct or via getBufferManager()) is counted against the
    /// owning query and routes through the buffer-exhaustion arbiter when it would otherwise block indefinitely.
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

/// Lifetime of the ThreadPool:
/// - ThreadPool is owned by the QueryEngine
/// - ThreadPool owns the TaskQueue.
///     - As long as any thread is alive the TaskQueue Needs to Exist
/// - ThreadPool has to outlive all Queries
class ThreadPool : public WorkEmitter, public QueryLifetimeController
{
public:
    void addThread(const Host& host);

    bool emitWork(
        QueryId qid,
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

    void emitPipelineStart(QueryId qid, const std::shared_ptr<RunningQueryPlanNode>& node, TaskCallback callback) override
    {
        auto [complete, failure, success] = std::move(callback).take();
        auto wrappedCallback = TaskCallback{
            std::move(complete),
            std::move(success),
            TaskCallback::OnFailure(injectQueryFailure(node, std::move(failure.callback))),
        };
        addInternalTask(StartPipelineTask(qid, node->id, std::move(wrappedCallback), node));
    }

    void emitPipelineStop(QueryId qid, std::unique_ptr<RunningQueryPlanNode> node, TaskCallback callback) override
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

    void initializeSourceFailure(QueryId id, OriginId sourceId, std::weak_ptr<RunningSource> source, Exception exception) override
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

    void initializeSourceStop(QueryId id, OriginId sourceId, std::weak_ptr<RunningSource> source) override
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

    void emitPendingPipelineStop(QueryId queryId, std::shared_ptr<RunningQueryPlanNode> node, TaskCallback callback) override
    {
        ENGINE_LOG_DEBUG("Inserting Pending Pipeline Stop for {}-{}", queryId, node->id);
        addInternalTask(PendingPipelineStopTask{queryId, std::move(node), 0, std::move(callback)});
    }

    ThreadPool(
        std::shared_ptr<AbstractQueryStatusListener> listener,
        std::shared_ptr<QueryEngineStatisticListener> stats,
        BufferExhaustionArbiter* arbiter,
        const size_t admissionQueueSize)
        : listener(std::move(listener))
        , statistic(std::move(stats))
        , arbiter(arbiter)
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
    BufferExhaustionArbiter* arbiter; ///NOLINT owned by the QueryEngine, which outlives the pool
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

void QueryPlanReaper::run()
{
    /// Disposing a plan destroys its pipeline nodes, whose deleter emits StopPipelineTasks into the internal task
    /// queue -- an operation reserved for engine threads. Register like the terminator thread (which disposes plans
    /// the same way during shutdown); the id is only used as an access gate and in statistics events.
    ThreadPool::WorkerThread::id = ThreadPool::terminatorThreadId;
    std::unique_lock lock(mutex);
    while (true)
    {
        pending.wait(lock, [this] { return stopped || !queue.empty(); });
        if (queue.empty())
        {
            return; /// only reachable when stopped: the queue is drained before shutdown completes
        }
        auto plan = std::move(queue.front());
        queue.pop_front();
        lock.unlock();
        disposeNow(std::move(plan));
        lock.lock();
    }
}

bool ThreadPool::WorkerThread::operator()(WorkTask& task) const
{
    LogContext logContext("Task", fmt::format("{}-{}", task.queryId, task.pipelineId));
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
            pool.arbiter->providerFor(task.queryId),
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
    LogContext logContext("Task", fmt::format("{}-{}", startPipeline.queryId, startPipeline.pipelineId));
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
            pool.arbiter->providerFor(startPipeline.queryId),
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
    LogContext logContext("Task", fmt::format("{}-{}", pendingPipelineStop.queryId, pendingPipelineStop.pipeline->id));
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
    LogContext logContext("Task", fmt::format("{}-{}", stopPipelineTask.queryId, stopPipelineTask.pipeline->id));
    ENGINE_LOG_DEBUG("Stop Pipeline Task for {}-{}", stopPipelineTask.queryId, stopPipelineTask.pipeline->id);
    DefaultPEC pec(
        pool.numberOfThreads(),
        WorkerThread::id,
        stopPipelineTask.pipeline->id,
        pool.arbiter->providerFor(stopPipelineTask.queryId),
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
    LogContext logContext("Task", fmt::format("{}", stopQuery.queryId));
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
    LogContext logContext("Task", fmt::format("{}", startQuery.queryId));
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
    LogContext logContext("Task", fmt::format("{}", stopSource.queryId));
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
    LogContext logContext("Task", fmt::format("{}", failSource.queryId));
    if (auto source = failSource.target.lock())
    {
        ENGINE_LOG_DEBUG("Fail Source Task for Query {} Source {}", failSource.queryId, source->getOriginId());
        source->fail(std::move(*failSource.exception));
        return true;
    }
    return false;
}

void ThreadPool::addThread(const Host& host)
{
    pool.emplace_back(
        fmt::format("WorkerThread-{}", numberOfThreads_),
        host,
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
    const Host& host)
    : bufferManager(std::move(bm))
    , statusListener(std::move(listener))
    , statisticListener(std::move(statListener))
    , queryCatalog(std::make_shared<QueryCatalog>(statusListener, statisticListener))
    , queryPlanReaper(std::make_unique<QueryPlanReaper>(host))
    , bufferExhaustionArbiter(std::make_unique<BufferExhaustionArbiter>(
          bufferManager,
          queryCatalog.get(),
          config.bufferExhaustionPolicy.getValue(),
          config.bufferRecoveryMargin.getValue() != 0 ? config.bufferRecoveryMargin.getValue() : config.numberOfWorkerThreads.getValue()))
    , threadPool(std::make_unique<ThreadPool>(
          statusListener, statisticListener, bufferExhaustionArbiter.get(), config.admissionQueueSize.getValue()))
    , host(host)
{
    queryCatalog->attachReaper(queryPlanReaper.get());
    for (size_t i = 0; i < config.numberOfWorkerThreads.getValue(); ++i)
    {
        threadPool->addThread(host);
    }
}

/// NOLINTNEXTLINE Intentionally non-const
void QueryEngine::stop(QueryId queryId)
{
    ENGINE_LOG_INFO("Stopping Query: {}", queryId);
    threadPool->taskQueue.addAdmissionTaskBlocking({}, StopQueryTask{queryId, queryCatalog, TaskCallback{}});
}

/// NOLINTNEXTLINE Intentionally non-const
void QueryEngine::start(std::unique_ptr<ExecutableQueryPlan> executableQueryPlan)
{
    threadPool->taskQueue.addAdmissionTaskBlocking(
        {}, StartQueryTask{executableQueryPlan->queryId, std::move(executableQueryPlan), queryCatalog, TaskCallback{}});
}

QueryEngine::~QueryEngine()
{
    ThreadPool::WorkerThread::id = ThreadPool::terminatorThreadId;
    queryCatalog->clear();
    /// Drain and stop the reaper while the ThreadPool still accepts and processes the pipeline-stop tasks that
    /// disposal emits. Any failQuery after this point disposes inline on the calling worker thread.
    queryPlanReaper->shutdown();
}

void QueryCatalog::start(
    QueryId queryId,
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
            QueryId queryId, std::shared_ptr<AbstractQueryStatusListener> listener, std::shared_ptr<QueryEngineStatisticListener> statistic)
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
                listener->logQueryStatusChange(queryId, QueryStatus::Running, timestamp);
            }
        }

        void onFailure(Exception exception) override
        {
            ENGINE_LOG_DEBUG("Query {} onFailure", queryId);
            const auto timestamp = std::chrono::system_clock::now();
            if (const auto locked = state.lock())
            {
                /// We want to avoid running destructors and callbacks while holding the atomic transition lock.
                /// So we move the queryplan out of the lock and dispose (if there exists one)
                std::optional<std::variant<std::unique_ptr<RunningQueryPlan>, std::unique_ptr<StoppingQueryPlan>>> toDispose{};
                /// Move the query into the Terminated::Failed state from any non-terminated state. An already
                /// terminated query (e.g. shed by the buffer-exhaustion arbiter via failQuery) does not transition:
                /// its failure was already reported, and in-flight tasks failing afterwards must not report it again.
                const auto didTransition = locked->transition(
                    [](Reserved&&)
                    {
                        ENGINE_LOG_DEBUG("Query was stopped before all pipeline starts were submitted");
                        return Terminated{Terminated::Failed};
                    },
                    [&toDispose](Starting&& starting)
                    {
                        toDispose = std::move(starting.plan);
                        return Terminated{Terminated::Failed};
                    },
                    [&toDispose](Running&& running)
                    {
                        toDispose = std::move(running.plan);
                        return Terminated{Terminated::Failed};
                    },
                    [&toDispose](Stopping&& stopping)
                    {
                        toDispose = std::move(stopping.plan);
                        return Terminated{Terminated::Failed};
                    });

                /// Dispose after the transition (lock released) to avoid deadlock
                if (toDispose)
                {
                    std::visit([]<typename T>(T&& plan) { T::element_type::dispose(std::forward<T>(plan)); }, std::move(toDispose).value());
                }

                if (didTransition)
                {
                    exception.what() += fmt::format(" in Query {}.", queryId);
                    ENGINE_LOG_ERROR("Query Failed: {}", exception.what());
                    listener->logQueryFailure(queryId, std::move(exception), timestamp);
                    statistic->onEvent(QueryFail(ThreadPool::WorkerThread::id, queryId));
                }
            }
        }

        /// OnDestruction is called when the entire query graph is terminated.
        void onDestruction() override
        {
            ENGINE_LOG_DEBUG("Query {} onDestruction", queryId);
            const auto timestamp = std::chrono::system_clock::now();
            if (const auto locked = state.lock())
            {
                /// We want to avoid running destructors and callbacks while holding the atomic transition lock.
                /// So we move the queryplan out of the lock and dispose (if there exists one)
                std::optional<std::variant<std::unique_ptr<RunningQueryPlan>, std::unique_ptr<StoppingQueryPlan>>> toDispose{};

                const auto didTransition = locked->transition(
                    [&toDispose](Starting&& starting)
                    {
                        toDispose = std::move(starting.plan);
                        return Terminated{Terminated::Stopped};
                    },
                    [&toDispose](Running&& running)
                    {
                        toDispose = std::move(running.plan);
                        return Terminated{Terminated::Stopped};
                    },
                    [&toDispose](Stopping&& stopping)
                    {
                        toDispose = std::move(stopping.plan);
                        return Terminated{Terminated::Stopped};
                    });

                /// Dispose after the transition (lock released) to avoid deadlock
                if (toDispose)
                {
                    std::visit([]<typename T>(T&& plan) { T::element_type::dispose(std::forward<T>(plan)); }, std::move(toDispose).value());
                }

                if (didTransition)
                {
                    listener->logQueryStatusChange(queryId, QueryStatus::Stopped, timestamp);
                    statistic->onEvent(QueryStop(ThreadPool::WorkerThread::id, queryId));
                }
            }
        }

        std::shared_ptr<AbstractQueryStatusListener> listener;
        std::shared_ptr<QueryEngineStatisticListener> statistic;
        QueryId queryId;
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
        listener->logQueryStatusChange(queryId, QueryStatus::Started, startTimestamp);
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

void QueryCatalog::stopQuery(QueryId id)
{
    const std::unique_ptr<RunningQueryPlan> toBeDeleted;
    {
        const std::scoped_lock lock(mutex);
        if (auto it = queryStates.find(id); it != queryStates.end())
        {
            auto& state = *it->second;
            absl::AnyInvocable<void()> cleanup;
            bool didTransition = state.transition(
                [&cleanup](Starting&& starting) /// NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
                {
                    auto [stoppingQueryPlan, cb] = RunningQueryPlan::stop(std::move(starting.plan));
                    cleanup = std::move(cb);
                    return Stopping{std::move(stoppingQueryPlan)};
                },
                [&cleanup](Running&& running) /// NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
                {
                    auto [stoppingQueryPlan, cb] = RunningQueryPlan::stop(std::move(running.plan));
                    cleanup = std::move(cb);
                    return Stopping{std::move(stoppingQueryPlan)};
                });
            if (didTransition)
            {
                cleanup();
            }
        }
        else
        {
            ENGINE_LOG_WARNING("Attempting to stop query {} failed. Query was not submitted to the engine.", id);
        }
    }
}

void QueryCatalog::failQuery(QueryId id, Exception exception)
{
    std::optional<QueryPlanReaper::Disposable> toDispose{};
    bool didTransition = false;
    {
        const std::scoped_lock lock(mutex);
        if (auto it = queryStates.find(id); it != queryStates.end())
        {
            /// Move the query into Terminated::Failed regardless of its current (non-terminated) state, mirroring
            /// RealQueryLifeTimeListener::onFailure. The plan is moved out and disposed AFTER releasing the lock.
            didTransition = it->second->transition(
                [&toDispose](Starting&& starting)
                {
                    auto terminating = std::move(starting);
                    toDispose = std::move(terminating.plan);
                    return Terminated{Terminated::Failed};
                },
                [&toDispose](Running&& running)
                {
                    auto terminating = std::move(running);
                    toDispose = std::move(terminating.plan);
                    return Terminated{Terminated::Failed};
                },
                [&toDispose](Stopping&& stopping)
                {
                    auto terminating = std::move(stopping);
                    toDispose = std::move(terminating.plan);
                    return Terminated{Terminated::Failed};
                });
        }
    }

    if (!didTransition)
    {
        return;
    }

    /// Report the failure exactly once (guarded by the transition). In-flight tasks of the victim that fail later hit
    /// the Terminated state in RealQueryLifeTimeListener::onFailure and do not report again.
    const auto timestamp = std::chrono::system_clock::now();
    exception.what() += fmt::format(" in Query {}.", id);
    ENGINE_LOG_ERROR("Query Failed: {}", exception.what());
    listener->logQueryFailure(id, std::move(exception), timestamp);
    statistic->onEvent(QueryFail(ThreadPool::WorkerThread::id, id));

    /// Hand the blocking disposal (joins the victim's source threads, releases its buffers) to the reaper thread, so
    /// the initiating worker returns to its allocation loop instead of blocking inside the exhausted pool.
    if (toDispose)
    {
        PRECONDITION(reaper != nullptr, "QueryPlanReaper must be attached before failing queries");
        reaper->dispose(std::move(*toDispose));
    }
}

std::vector<QueryId> QueryCatalog::gatherVictimCandidates()
{
    std::vector<QueryId> candidates;
    candidates.reserve(queryStates.size());
    for (const auto& [queryId, state] : queryStates)
    {
        if (state->is<Running>() || state->is<Starting>() || state->is<Stopping>())
        {
            candidates.push_back(queryId);
        }
    }
    return candidates;
}

QueryId QueryCatalog::selectLargest(const std::vector<QueryId>& candidates, const std::unordered_map<QueryId, uint64_t>& buffersHeld)
{
    QueryId bestQuery = INVALID_QUERY_ID;
    uint64_t bestHeld = 0;
    for (const auto& candidate : candidates)
    {
        const auto it = buffersHeld.find(candidate);
        const uint64_t held = it != buffersHeld.end() ? it->second : 0;
        if (bestQuery == INVALID_QUERY_ID || held > bestHeld)
        {
            bestQuery = candidate;
            bestHeld = held;
        }
    }
    return bestQuery;
}

std::optional<QueryId> QueryCatalog::selectVictim(
    const BufferExhaustionPolicy policy, QueryId currentQuery, const std::unordered_map<QueryId, uint64_t>& buffersHeld)
{
    if (policy == BufferExhaustionPolicy::TERMINATE_SELF)
    {
        return currentQuery;
    }

    const std::scoped_lock lock(mutex);

    const std::vector<QueryId> candidates = gatherVictimCandidates();
    if (candidates.empty())
    {
        return std::nullopt;
    }

    switch (policy)
    {
        case BufferExhaustionPolicy::TERMINATE_LARGEST:
            return selectLargest(candidates, buffersHeld);
        case BufferExhaustionPolicy::TERMINATE_SELF:
            return currentQuery; /// handled above
    }
    return std::nullopt;
}
}
