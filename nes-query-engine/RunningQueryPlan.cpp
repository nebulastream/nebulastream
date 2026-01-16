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

#include <RunningQueryPlan.hpp>

#include <algorithm>
#include <cassert>
#include <functional>
#include <iterator>
#include <memory>
#include <ranges>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <absl/functional/any_invocable.h>
#include <CompiledQueryPlan.hpp>
#include <EngineLogger.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <ExecutableQueryPlan.hpp>
#include <Interfaces.hpp>
#include <RunningSource.hpp>

namespace NES
{

/// The RunningQueryPlanNodeDeleter ensures that a RunningQueryPlan Node properly stopped.
/// If a node has been started the requiresTermination flag will be set. In a graceful stop this requires
/// the engine to stop the pipeline. Since a pipeline stop is potentially an expensive operation this is moved into
/// the task queue.
/// Because we use shared ptr to implement the reference counting, we can create a custom deleter, that instead of deleting the
/// resource of the shared_ptr rescues it in a unique_ptr. At this point there cannot be references onto the pipeline only the PipelineStop
/// task keeps the Node alive. The node however keeps all of its successors alive. If the pipeline stop has been performed the onSuccess
/// callback emits PendingPipelineStops for all successors and is destroyed.
void RunningQueryPlanNode::RunningQueryPlanNodeDeleter::operator()(RunningQueryPlanNode* ptr)
{
    std::unique_ptr<RunningQueryPlanNode> node(ptr);
    ENGINE_LOG_DEBUG("Node {} will be deleted", node->id);
    if (ptr->requiresTermination)
    {
        emitter.emitPipelineStop(
            queryId,
            std::move(node),
            TaskCallback{
                TaskCallback::OnComplete(
                    [ptr, &emitter = this->emitter, queryId = this->queryId]() mutable
                    {
                        ENGINE_LOG_DEBUG("Pipeline {}-{} was stopped", queryId, ptr->id);
                        ptr->requiresTermination = false;
                        for (auto& successor : ptr->successors)
                        {
                            emitter.emitPendingPipelineStop(queryId, std::move(successor), TaskCallback{});
                        }
                    }),
                TaskCallback::OnFailure(
                    [ENGINE_IF_LOG_DEBUG(queryId = queryId, ) ptr](Exception)
                    {
                        ENGINE_LOG_DEBUG("Failed to stop {}-{}", queryId, ptr->id);
                        ptr->requiresTermination = false;
                    })});
    }
    else
    {
        ENGINE_LOG_TRACE("Skipping {}-{} stop", queryId, ptr->id);
    }
}

std::shared_ptr<RunningQueryPlanNode> RunningQueryPlanNode::create(
    LocalQueryId queryId,
    PipelineId pipelineId,
    WorkEmitter& emitter,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    std::unique_ptr<ExecutablePipelineStage> stage,
    std::function<void(Exception)> unregisterWithError,
    CallbackRef planRef,
    CallbackRef setupCallback)
{
    auto node = std::shared_ptr<RunningQueryPlanNode>(
        new RunningQueryPlanNode(pipelineId, std::move(successors), std::move(stage), std::move(unregisterWithError), std::move(planRef)),
        RunningQueryPlanNodeDeleter{.emitter = emitter, .queryId = queryId});
    emitter.emitPipelineStart(
        queryId,
        node,
        TaskCallback{TaskCallback::OnComplete(
            [ENGINE_IF_LOG_TRACE(queryId, pipelineId, ) setupCallback = std::move(setupCallback), weakRef = std::weak_ptr(node)]
            {
                if (const auto nodeLocked = weakRef.lock())
                {
                    ENGINE_LOG_TRACE("Pipeline {}-{} was initialized", queryId, pipelineId);
                    nodeLocked->requiresTermination = true;
                }
            })});

    return node;
}

RunningQueryPlanNode::~RunningQueryPlanNode()
{
    assert(!requiresTermination && "Node was destroyed without termination. This should not happen");
}

void RunningQueryPlanNode::fail(Exception exception) const
{
    unregisterWithError(std::move(exception));
}

std::
    pair<std::vector<std::pair<std::unique_ptr<SourceHandle>, std::vector<std::shared_ptr<RunningQueryPlanNode>>>>, std::vector<std::weak_ptr<RunningQueryPlanNode>>> static createRunningNodes(
        LocalQueryId queryId,
        ExecutableQueryPlan& queryPlan,
        std::function<void(Exception)> unregisterWithError,
        const CallbackRef& terminationCallbackRef,
        const CallbackRef& pipelineSetupCallbackRef,
        WorkEmitter& emitter)
{
    std::vector<std::pair<std::unique_ptr<SourceHandle>, std::vector<std::shared_ptr<RunningQueryPlanNode>>>> sources;
    std::vector<std::weak_ptr<RunningQueryPlanNode>> pipelines;
    std::unordered_map<ExecutablePipeline*, std::shared_ptr<RunningQueryPlanNode>> cache;
    std::function<std::shared_ptr<RunningQueryPlanNode>(ExecutablePipeline*)> getOrCreate = [&](ExecutablePipeline* pipeline)
    {
        INVARIANT(pipeline, "Pipeline should not be nullptr");
        if (auto it = cache.find(pipeline); it != cache.end())
        {
            return it->second;
        }
        std::vector<std::shared_ptr<RunningQueryPlanNode>> successors;
        std::ranges::transform(
            pipeline->successors,
            std::back_inserter(successors),
            [&](const auto& successor) { return getOrCreate(successor.lock().get()); });
        auto node = RunningQueryPlanNode::create(
            queryId,
            pipeline->id,
            emitter,
            std::move(successors),
            std::move(pipeline->stage),
            unregisterWithError,
            terminationCallbackRef,
            pipelineSetupCallbackRef);
        pipelines.emplace_back(node);
        cache[pipeline] = std::move(node);
        return cache[pipeline];
    };

    for (auto& [source, successors] : queryPlan.sources)
    {
        std::vector<std::shared_ptr<RunningQueryPlanNode>> successorNodes;
        for (const auto& successor : successors)
        {
            successorNodes.push_back(getOrCreate(successor.lock().get()));
        }
        sources.emplace_back(std::move(source), std::move(successorNodes));
    }

    return {std::move(sources), pipelines};
}

std::pair<std::unique_ptr<RunningQueryPlan>, CallbackRef> RunningQueryPlan::start(
    LocalQueryId queryId,
    std::unique_ptr<ExecutableQueryPlan> plan,
    QueryLifetimeController& controller,
    WorkEmitter& emitter,
    std::shared_ptr<QueryLifetimeListener> listener)
{
    PRECONDITION(not plan->pipelines.empty(), "Cannot start an empty query plan");
    PRECONDITION(not plan->sources.empty(), "Cannot start a query plan without sources");

    /// Callback to track lifetime of all pipelines.
    auto [terminationCallbackOwner, terminationCallbackRef] = Callback::create();
    terminationCallbackOwner.setCallback([listener] { listener->onDestruction(); });

    /// Callback to track the lifetime of all setup tasks.
    auto [pipelineSetupCallbackOwner, pipelineSetupCallbackRef] = Callback::create();

    /// The RunningQueryPlan object is the owner of both callbacks. If the RunningQueryPlan object is destroyed (i.e., stopped from a user,
    /// or because of a failure). The Callbacks are cancelled, as they have become meaningless.
    auto runningPlan = std::unique_ptr<RunningQueryPlan>(
        new RunningQueryPlan(std::move(terminationCallbackOwner), std::move(pipelineSetupCallbackOwner)));

    auto lock = runningPlan->internal.lock();

    auto& internal = *lock;
    internal.qep = std::move(plan);
    auto [sources, pipelines] = createRunningNodes(
        queryId,
        *internal.qep,
        [ENGINE_IF_LOG_DEBUG(queryId, ) listener](Exception exception)
        {
            ENGINE_LOG_DEBUG("Fail PipelineNode called for QueryId: {}", queryId)
            listener->onFailure(std::move(exception));
        },
        terminationCallbackRef,
        pipelineSetupCallbackRef,
        emitter);
    internal.pipelines = std::move(pipelines);

    /// The QueryEngine uses the setup callback to start the sources once all pipelines have been set up, effectively starting the query.
    /// The setup callback tracks the lifetimes of all pipeline setup tasks. Either a task will fail and terminate the query, which will cancel the setup callback.
    /// Or the setup task completes and destroys the callback reference.
    /// When the last setup task is destroyed, it sets the guard counter of the callback reference to 0, triggering the execution of setup callback.
    internal.allPipelinesStarted.setCallback(
        [
                /// This reference is guaranteed to be alive. As the running callback holds the Callbacks owner object. The callback
                /// guarantees, that its owner is either alive during the callback execution or that the callback will never execute
                & runningPlan = *runningPlan,
                queryId,
                listener = std::move(listener),
                &controller,
                &emitter,
                sources = std::move(sources)]() mutable
        {
            {
                auto lock = runningPlan.internal.lock();
                auto& internal = *lock;

                ENGINE_LOG_DEBUG("Pipeline Setup Completed");
                for (auto& [source, successors] : sources)
                {
                    auto sourceId = source->getSourceId();
                    internal.sources.emplace(
                        sourceId,
                        RunningSource::create(
                            queryId,
                            std::move(source),
                            std::move(successors),
                            [&emitter, queryId](std::vector<std::shared_ptr<RunningQueryPlanNode>>&& successors)
                            {
                                /// On source unregistration all successor pipelines are soft stopped.
                                ENGINE_LOG_INFO("Source unregistered, emitting pending pipeline stops");
                                for (auto& successor : successors)
                                {
                                    emitter.emitPendingPipelineStop(queryId, std::move(successor), TaskCallback{});
                                }
                                return true;
                            },
                            [listener](const Exception& exception) { listener->onFailure(exception); },
                            controller,
                            emitter));
                }
                /// release lock
            }

            listener->onRunning();
        });

    return {std::move(runningPlan), pipelineSetupCallbackRef};
}

std::pair<std::unique_ptr<StoppingQueryPlan>, absl::AnyInvocable<void()>>
RunningQueryPlan::stop(std::unique_ptr<RunningQueryPlan> runningQueryPlan)
{
    ENGINE_LOG_DEBUG("Soft Stopping Query Plan");

    auto lock = runningQueryPlan->internal.lock();
    auto& internal = *lock;

    return {
        std::make_unique<StoppingQueryPlan>(
            std::move(internal.qep), std::move(internal.listeners), std::move(internal.allPipelinesExpired)),
        [callbackOwner = std::move(internal.allPipelinesStarted),
         pipelines = std::move(internal.pipelines),
         sources = std::move(internal.sources)]() mutable
        {
            /// Destroying the sources needs to ensure that there is no concurrency on the sources vector.
            /// The setup callback may run concurrently so it is essential that the callback owner is destroyed before
            /// however the callback owner will block until the callback is either cancelled or completed. Completing the callback requires
            /// a lock on the AtomicState. Therefore, this callback needs to be called from outside the atomic state.
            callbackOwner = {};
            pipelines.clear();
            sources.clear();
        }

    };
}

std::unique_ptr<ExecutableQueryPlan> StoppingQueryPlan::dispose(std::unique_ptr<StoppingQueryPlan> stoppingQueryPlan)
{
    ENGINE_LOG_DEBUG("Disposing Stopping Query Plan");
    return std::move(stoppingQueryPlan->plan);
}

std::unique_ptr<ExecutableQueryPlan> RunningQueryPlan::dispose(std::unique_ptr<RunningQueryPlan> runningQueryPlan)
{
    ENGINE_LOG_DEBUG("Disposing Running Query Plan");

    auto lock = runningQueryPlan->internal.lock();
    auto& internal = *lock;

    internal.listeners.clear();
    internal.allPipelinesExpired = {};
    return std::move(internal.qep);
}

RunningQueryPlan::~RunningQueryPlan()
{
    auto lock = this->internal.lock();
    auto& internal = *lock;


    /// CRITICAL: Disable pipeline setup callback during destruction to prevent race condition.
    ///
    /// This prevents any pending pipeline setup callbacks from executing after the
    /// RunningQueryPlan starts being destroyed. Without this, callbacks could access
    /// partially destroyed object state, leading to use-after-free errors.
    ///
    /// The callback may have captured a raw pointer to this RunningQueryPlan during
    /// the start() method, and this ensures it cannot execute after destruction begins.
    internal.allPipelinesStarted = {};

    for (const auto& weakRef : internal.pipelines)
    {
        if (auto strongRef = weakRef.lock())
        {
            strongRef->requiresTermination = false;
        }
    }
    internal.sources.clear();
}
}
