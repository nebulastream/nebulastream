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
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <ranges>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceHandle.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <InstantiatedQueryPlan.hpp>
#include <QueryEngine.hpp>
#include <RunningQueryPlan.hpp>
#include <RunningSource.hpp>

namespace NES::Runtime
{
std::pair<CallbackOwner, CallbackRef> Callback::create(std::string context)
{
    auto ref = std::shared_ptr<Callback>{
        std::make_unique<Callback>().release(),
        [=](Callback* ptr)
        {
            std::unique_ptr<Callback> cb(ptr);
            std::scoped_lock lock(ptr->mutex);
            try
            {
                if (!cb->callbacks.empty())
                {
                    NES_DEBUG("Triggering {} callbacks", context);
                }
                for (auto& fn : cb->callbacks)
                {
                    fn();
                }
            }
            catch (const Exception& e)
            {
                NES_ERROR("A Callback has thrown an exception. The callback chain has been aborted.\nException: {}", e);
            }
        }};
    return std::make_pair(CallbackOwner{std::move(context), ref}, CallbackRef{ref});
}

CallbackOwner& CallbackOwner::operator=(CallbackOwner&& other)
{
    if (auto ptr = owner.lock())
    {
        std::scoped_lock lock(ptr->mutex);
        if (!ptr->callbacks.empty())
        {
            NES_DEBUG("Overwrite {} Callbacks", context);
        }
        ptr->callbacks.clear();
    }
    owner = std::move(other.owner);
    context = std::move(other.context);
    other.owner.reset();
    return *this;
}

CallbackOwner::~CallbackOwner()
{
    if (auto ptr = owner.lock())
    {
        NES_DEBUG("Disabling {} Callbacks", context);
        std::scoped_lock lock(ptr->mutex);
        ptr->callbacks.clear();
    }
}

void CallbackOwner::addCallback(std::move_only_function<void()> fn) const
{
    if (auto ptr = owner.lock())
    {
        std::scoped_lock lock(ptr->mutex);
        ptr->callbacks.emplace_back(std::move(fn));
    }
}

CallbackRef CallbackOwner::addCallbackAssumeNonShared(CallbackRef&& ref, absl::AnyInvocable<void()> fn) const
{
    PRECONDITION(ref.ref.use_count() == 1, "This function can only be called if there is no other user of the Callback.")
    ref.ref->callbacks.emplace_back(std::move(fn));
    return ref;
}

void CallbackOwner::addCallbackUnsafe(CallbackRef ref, absl::AnyInvocable<void()> fn) const
{
    PRECONDITION(!owner.expired(), "Weak Ptr to Callback has expired. Callbacks have already been triggered");
    PRECONDITION(ref.ref.get() == owner.lock().get(), "Callback Ref and Owner do not match");
    ref.ref->callbacks.emplace_back(std::move(fn));
}

std::optional<CallbackRef> CallbackOwner::getRef() const
{
    if (auto locked = owner.lock())
    {
        return CallbackRef{locked};
    }
    return {};
}

void RunningQueryPlanNode::RunningQueryPlanNodeDeleter::operator()(RunningQueryPlanNode* ptr)
{
    std::unique_ptr<RunningQueryPlanNode> node(ptr);
    if (ptr->requireTermination)
    {
        emitter.emitStop(
            queryId,
            std::move(node),
            [ptr, queryId = this->queryId]()
            {
                NES_TRACE("Pipeline {}-{} was terminated", queryId, ptr->id);
                ptr->requireTermination = false;
            });
    }
    else
    {
        NES_TRACE("Skipping {}-{} Termination", queryId, ptr->id);
    }
}

std::shared_ptr<RunningQueryPlanNode> RunningQueryPlanNode::create(
    QueryId queryId,
    PipelineId pipelineId,
    WorkEmitter& emitter,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    std::unique_ptr<Execution::ExecutablePipelineStage> stage,
    CallbackRef planRef,
    CallbackRef setupCallback)
{
    auto node = std::shared_ptr<RunningQueryPlanNode>(
        new RunningQueryPlanNode(pipelineId, std::move(successors), std::move(stage), std::move(planRef)),
        RunningQueryPlanNodeDeleter{emitter, queryId});
    emitter.emitSetup(
        queryId,
        node,
        [queryId, pipelineId, cb = std::move(setupCallback), weak_ref = std::weak_ptr(node)]
        {
            if (auto node = weak_ref.lock())
            {
                NES_TRACE("Pipeline {}-{} was initialized", queryId, pipelineId);
                node->requireTermination = true;
            }
        });

    return node;
}

RunningQueryPlanNode::~RunningQueryPlanNode()
{
    assert(!requireTermination && "Node was destroyed without termination. This should not happen");
}

std::
    pair<std::vector<std::pair<std::unique_ptr<Sources::SourceHandle>, std::vector<std::shared_ptr<RunningQueryPlanNode>>>>, std::vector<std::weak_ptr<RunningQueryPlanNode>>> static createRunningNodes(
        QueryId queryId,
        InstantiatedQueryPlan& qep,
        const CallbackRef& terminationCallbackRef,
        const CallbackRef& pipelineSetupCallbackRef,
        WorkEmitter& emitter)
{
    std::vector<std::pair<std::unique_ptr<Sources::SourceHandle>, std::vector<std::shared_ptr<RunningQueryPlanNode>>>> sources;
    std::vector<std::weak_ptr<RunningQueryPlanNode>> pipelines;
    std::unordered_map<Execution::ExecutablePipeline*, std::shared_ptr<RunningQueryPlanNode>> cache;
    std::function<std::shared_ptr<RunningQueryPlanNode>(Execution::ExecutablePipeline*, PipelineId::Underlying&)> get_or_create
        = [&](Execution::ExecutablePipeline* pipeline, PipelineId::Underlying& pipelineIdCounter)
    {
        if (auto it = cache.find(pipeline); it != cache.end())
        {
            return it->second;
        }
        PipelineId pipelineId(pipelineIdCounter++);
        std::vector<std::shared_ptr<RunningQueryPlanNode>> successors;
        std::ranges::transform(
            pipeline->successors,
            std::back_inserter(successors),
            [&](const auto& s) { return get_or_create(s.lock().get(), pipelineIdCounter); });
        auto node = RunningQueryPlanNode::create(
            queryId,
            pipelineId,
            emitter,
            std::move(successors),
            std::move(pipeline->stage),
            terminationCallbackRef,
            pipelineSetupCallbackRef);
        pipelines.emplace_back(node);
        cache[pipeline] = std::move(node);
        return cache[pipeline];
    };

    auto pipelineIdCounter = PipelineId::INITIAL;
    for (auto& [source, successors] : qep.sources)
    {
        std::vector<std::shared_ptr<RunningQueryPlanNode>> successorNodes;
        std::ranges::transform(
            successors,
            std::back_inserter(successorNodes),
            [get_or_create, &pipelineIdCounter](auto& t) { return get_or_create(t.lock().get(), pipelineIdCounter); });
        sources.emplace_back(std::move(source), std::move(successorNodes));
    }

    return {std::move(sources), pipelines};
}

std::pair<std::unique_ptr<RunningQueryPlan>, CallbackRef> RunningQueryPlan::start(
    QueryId queryId,
    std::unique_ptr<InstantiatedQueryPlan> plan,
    QueryLifetimeController& controller,
    WorkEmitter& emitter,
    std::shared_ptr<QueryLifetimeListener> listener)
{
    PRECONDITION(!plan->pipelines.empty(), "Cannot start an empty Query Plan");
    PRECONDITION(!plan->sources.empty(), "Cannot start a Query Plan without sources");

    auto [terminationCallbackOwner, terminationCallbackRef] = Callback::create("Termination");
    auto [pipelineSetupCallbackOwner, pipelineSetupCallbackRef] = Callback::create("Pipeline Setup");

    auto runningPlan = std::unique_ptr<RunningQueryPlan>(
        new RunningQueryPlan(std::move(terminationCallbackOwner), std::move(pipelineSetupCallbackOwner)));
    runningPlan->qep = std::move(plan);

    terminationCallbackRef = runningPlan->allPipelinesExpired.addCallbackAssumeNonShared(
        std::move(terminationCallbackRef), [listener] { listener->onDestruction(); });


    auto [sources, pipelines] = createRunningNodes(queryId, *runningPlan->qep, terminationCallbackRef, pipelineSetupCallbackRef, emitter);
    runningPlan->pipelines = std::move(pipelines);


    /// We can call the unsafe version of addCallback (which does not take a lock), because we hold a reference to one pipelineSetupCallbackRef,
    /// thus it is impossible for a different thread to trigger the registered callbacks. We are the only owner of the CallbackOwner, so
    /// it is impossible for any other thread to add addtional callbacks concurrently.
    runningPlan->pipelineSetupDone.addCallbackUnsafe(
        pipelineSetupCallbackRef,
        [runningPlan = runningPlan.get(),
         queryId,
         listener = std::move(listener),
         &controller,
         &emitter,
         sources = std::move(sources)]() mutable
        {
            /// The RunningQueryPlan is guaranteed to be alive at this point. Otherwise the the callback would have been cleared.
            NES_DEBUG("Pipeline Setup Completed");
            for (auto& [source, successors] : sources)
            {
                auto sourceId = source->getSourceId();
                runningPlan->sources.lock()->emplace(
                    sourceId,
                    RunningSource::create(
                        queryId,
                        std::move(source),
                        std::move(successors),
                        [runningPlan, id = sourceId] { runningPlan->sources.lock()->erase(id); },
                        [listener](Exception e) { listener->onFailure(e); },
                        controller,
                        emitter));
            }
            listener->onRunning();
        });

    return {std::move(runningPlan), pipelineSetupCallbackRef};
}
void RunningQueryPlan::stopSource(OriginId oid)
{
    sources.lock()->erase(oid);
}

std::pair<std::unique_ptr<StoppingQueryPlan>, CallbackRef> RunningQueryPlan::stop(std::unique_ptr<RunningQueryPlan> rp)
{
    NES_DEBUG("Soft Stopping Query Plan");
    /// By default the RunningQueryPlan dtor will disable pipeline termination. The Softstop should invoke all pipeline terminations.
    /// So we prevent the disabling the terminations by clearing all weak references to the pipelines before destroying the rqp.
    rp->pipelines.clear();
    auto callback = rp->allPipelinesExpired.getRef();
    return {
        std::make_unique<StoppingQueryPlan>(std::move(rp->qep), std::move(rp->listeners), std::move(rp->allPipelinesExpired)), *callback};
}


std::unique_ptr<InstantiatedQueryPlan> StoppingQueryPlan::dispose(std::unique_ptr<StoppingQueryPlan> rp)
{
    NES_DEBUG("Disposing Stopping Query Plan");
    return std::move(rp->plan);
}

std::unique_ptr<InstantiatedQueryPlan> RunningQueryPlan::dispose(std::unique_ptr<RunningQueryPlan> rp)
{
    NES_DEBUG("Disposing Running Query Plan");
    rp->listeners.clear();
    rp->allPipelinesExpired = {};
    return std::move(rp->qep);
}

RunningQueryPlan::~RunningQueryPlan()
{
    for (auto weakRef : pipelines)
    {
        if (auto strongRef = weakRef.lock())
        {
            strongRef->requireTermination = false;
        }
    }
    sources.lock()->clear();
}
}
