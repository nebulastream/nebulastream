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

#pragma once
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <absl/functional/any_invocable.h>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <ExecutableQueryPlan.hpp>
#include <Interfaces.hpp>
#include <RunningSource.hpp>

namespace NES::Runtime
{

/// The Implementation of the RunningQueryPlan is based on the DesignDocument covering the redesign of the QueryEngine
/// https://github.com/nebulastream/nebulastream-public/pull/464/files
struct CallbackRef;
struct CallbackOwner;

struct Callback
{
    static std::pair<CallbackOwner, CallbackRef> create(std::string context);

private:
    friend CallbackOwner;
    std::mutex mutex;
    std::vector<absl::AnyInvocable<void()>> callbacks;
};

struct CallbackRef
{
    CallbackRef() = default;

    explicit CallbackRef(std::shared_ptr<Callback> ref) : ref(std::move(ref)) { }

private:
    friend CallbackOwner;
    std::shared_ptr<Callback> ref = nullptr;
};

/// Dropping the callback owner will clear all pending callbacks
struct CallbackOwner
{
    CallbackOwner() = default;

    explicit CallbackOwner(std::string context, std::weak_ptr<Callback> owner) : context(std::move(context)), owner(std::move(owner)) { }

    CallbackOwner(const CallbackOwner& other) = delete;
    CallbackOwner& operator=(const CallbackOwner& other) = delete;

    CallbackOwner(CallbackOwner&& other) = default;

    /// Destructor and move assignment will erase all pending callbacks
    ~CallbackOwner();
    CallbackOwner& operator=(CallbackOwner&& other) noexcept;

    explicit operator bool() const { return !owner.expired(); }

    void addCallback(absl::AnyInvocable<void()> callbackFunction) const;

    /// Only call this function if you can guarantee that:
    /// 1. no other thread can add a callback concurrently (e.g. there should only be a single owner)
    /// 2. you have at least one reference which prevents the callback from triggering
    void addCallbackUnsafe(const CallbackRef& ref, absl::AnyInvocable<void()> callbackFunction) const;

    CallbackRef addCallbackAssumeNonShared(CallbackRef&&, absl::AnyInvocable<void()> callbackFunction);
    [[nodiscard]] std::optional<CallbackRef> getRef() const;

private:
    std::string context;
    std::weak_ptr<Callback> owner;
};

/// Running Query Plan represents a ExecutableQueryPlan which is currently running.
/// The lifetime of the RunningQueryPlan is tied to the lifetime of the query. As long as the RunningQueryPlan object
/// is alive the query is conceptional (at least partially) running.
/// If the lifetime of the RunningQueryPlan object ends, all sources, pipelines are terminated.

struct RunningQueryPlan;

/// As long as the Node is alive the corresponding Pipeline is running. Once the lifetime of a Node ends the pipeline
/// is terminated. Nodes in the running query plan contain forward reference counts, if the lifetime of a node ends
/// the reference count of the successor nodes is decremented, potentially causing them to be destroyed as well.
/// This also guarantees, that a node will always be alive if any of its predecessors is alive.
struct RunningQueryPlanNode
{
    struct RunningQueryPlanNodeDeleter
    {
        WorkEmitter& emitter; ///NOLINT The WorkEmitter (a.k.a. ThreadPool) always outlives the RunningQueryPlan and its nodes
        QueryId queryId = INVALID_QUERY_ID;

        void operator()(RunningQueryPlanNode* ptr);
    };

    static std::shared_ptr<RunningQueryPlanNode> create(
        QueryId queryId,
        PipelineId pipelineId,
        WorkEmitter& emitter,
        std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
        std::unique_ptr<Execution::ExecutablePipelineStage> stage,
        std::function<void(Exception)> unregisterWithError,
        CallbackRef planRef,
        CallbackRef setupCallback);


    ~RunningQueryPlanNode();

    RunningQueryPlanNode(
        PipelineId id,
        std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
        std::unique_ptr<Execution::ExecutablePipelineStage> stage,
        std::function<void(Exception)> unregisterWithError,
        CallbackRef planRef)
        : id(id)
        , successors(std::move(successors))
        , stage(std::move(stage))
        , unregisterWithError(std::move(unregisterWithError))
        , planRef(std::move(planRef))
    {
    }

    void fail(Exception exception) const;

    PipelineId id;

    std::atomic_bool requiresTermination = false;
    std::atomic<ssize_t> pendingTasks = 0;
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors;
    std::unique_ptr<Execution::ExecutablePipelineStage> stage;

    std::function<void(Exception)> unregisterWithError;
    CallbackRef planRef;
};

struct QueryLifetimeListener
{
    virtual ~QueryLifetimeListener() = default;
    virtual void onRunning() = 0;
    virtual void onFailure(Exception) = 0;
    virtual void onDestruction() = 0;
};

struct StoppingQueryPlan
{
    static std::unique_ptr<ExecutableQueryPlan> dispose(std::unique_ptr<StoppingQueryPlan> stoppingQueryPlan);

    std::unique_ptr<ExecutableQueryPlan> plan;
    std::vector<std::shared_ptr<QueryLifetimeListener>> listeners;
    CallbackOwner allPipelinesExpired;
};

/// It is possible to attach callbacks to the RunningQueryPlan
struct RunningQueryPlan
{
    /// Returns a RunningQueryPlan alongside a CallbackRef.
    /// The CallbackRef prevents the RunningQueryPlan from completing its setup:
    /// AS LONG AS THE CallbackRef IS ALIVE onRunning WILL NOT BE CALLED.
    /// The main purpose is to allow the owner of the RQP to release locks before the listeners can be called.
    static std::pair<std::unique_ptr<RunningQueryPlan>, CallbackRef> start(
        QueryId queryId,
        std::unique_ptr<ExecutableQueryPlan> plan,
        QueryLifetimeController&,
        WorkEmitter&,
        std::shared_ptr<QueryLifetimeListener>);

    /// Stopping a RunningQueryPlan will:
    /// 1. Keep all callbacks alive. Eventually onDestruction will be called.
    /// 2. Initialize pipeline termination asynchronously.
    /// 3. Block until all sources are terminated.
    static std::pair<std::unique_ptr<StoppingQueryPlan>, CallbackRef> stop(std::unique_ptr<RunningQueryPlan> runningQueryPlan);

    /// Disposing a RunningQueryPlan will:
    /// 1. Not notify any listeners. `onDestruction` will not be called.
    /// 2. Pipelines are not terminated, just destroyed.
    static std::unique_ptr<ExecutableQueryPlan> dispose(std::unique_ptr<RunningQueryPlan> runningQueryPlan);

    /// Destroying a RunningQueryPlan will:
    /// 1. Will invoke listeners!
    /// 2. Pipelines are not terminated, just destroyed.
    /// 3. Block until all sources are terminated.
    ~RunningQueryPlan();

private:
    struct Internal
    {
        Internal(
            std::vector<std::shared_ptr<QueryLifetimeListener>> listeners,
            std::unordered_map<OriginId, std::shared_ptr<RunningSource>> sources,
            std::vector<std::weak_ptr<RunningQueryPlanNode>> pipelines,
            std::unique_ptr<ExecutableQueryPlan> qep,
            CallbackOwner all_pipelines_expired,
            CallbackOwner pipeline_setup_done)
            : listeners(std::move(listeners))
            , sources(std::move(sources))
            , pipelines(std::move(pipelines))
            , qep(std::move(qep))
            , allPipelinesExpired(std::move(all_pipelines_expired))
            , allPipelinesStarted(std::move(pipeline_setup_done))
        {
        }

        /// ORDER OF MEMBER IS IMPORTANT!
        /// The destructor of the Callbacks could potentially cause
        /// the `onDestruct` callback of the listeners to be called
        /// Thus it is important that when the callbacks are destroyed
        /// all potentially used members are still alive.
        /// The order of destruction is bottom up.
        std::vector<std::shared_ptr<QueryLifetimeListener>> listeners;
        std::unordered_map<OriginId, std::shared_ptr<RunningSource>> sources;
        std::vector<std::weak_ptr<RunningQueryPlanNode>> pipelines;
        std::unique_ptr<ExecutableQueryPlan> qep;

        /// The entire graph of the query has been destroyed.
        CallbackOwner allPipelinesExpired;
        /// All pipelines have been initialized
        CallbackOwner allPipelinesStarted;
    };

    folly::Synchronized<Internal, std::recursive_mutex> internal;

    explicit RunningQueryPlan(CallbackOwner allPipelinesExpired, CallbackOwner pipelineSetupDone)
        : internal(Internal(
              std::vector<std::shared_ptr<QueryLifetimeListener>>{},
              std::unordered_map<OriginId, std::shared_ptr<RunningSource>>{},
              std::vector<std::weak_ptr<RunningQueryPlanNode>>{},
              nullptr,
              std::move(allPipelinesExpired),
              std::move(pipelineSetupDone)))
    {
    }
};

}
