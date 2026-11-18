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

#include <Placement/QueryPlacementAmendmentPhase.hpp>

#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Placement/BottomUpPlacement.hpp>
#include <Placement/IncrementalPlacementUtils.hpp>
#include <Placement/PlacementAdditionStrategy.hpp>
#include <Placement/PlacementRemovalStrategy.hpp>
#include <Placement/QueryDecomposition.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <ChangeLog.hpp>
#include <DeploymentContext.hpp>
#include <ErrorHandling.hpp>
#include <GlobalExecutionPlan.hpp>
#include <OperatorGraph.hpp>
#include <QueryOptimizerNetworkConfiguration.hpp>
#include <SharedQueryPlan.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

QueryPlacementAmendmentPhase::QueryPlacementAmendmentPhase(
    GlobalExecutionPlan& globalExecutionPlan,
    SharedPtr<WorkerCatalog> workerCatalog,
    const QueryDecomposer& decomposer,
    QueryOptimizerNetworkConfiguration networkConfiguration,
    const bool enableIncrementalPlacement)
    : globalExecutionPlan(globalExecutionPlan)
    , workerCatalog(std::move(workerCatalog))
    , decomposer(decomposer)
    , networkConfiguration(std::move(networkConfiguration))
    , enableIncrementalPlacement(enableIncrementalPlacement)
{
}

DeploymentUnit QueryPlacementAmendmentPhase::execute(SharedQueryPlan& sharedQueryPlan)
{
    return enableIncrementalPlacement ? executeIncrementally(sharedQueryPlan) : executeHolistically(sharedQueryPlan);
}

void QueryPlacementAmendmentPhase::computeReconfigurationMarkerDeploymentUnit(
    const SharedQueryPlan& sharedQueryPlan, const ChangeLogEntry& entry, std::set<ReconfigurationMarkerUnit>& units) const
{
    const auto& queryId = sharedQueryPlan.getId();
    const auto& graph = sharedQueryPlan.getGraph();
    const auto topology = workerCatalog->getTopology();
    for (const auto& upstreamOperator : entry.upstreamOperators)
    {
        const auto state = sharedQueryPlan.getOperatorState(upstreamOperator);
        if (state != OperatorState::Placed and state != OperatorState::ToBeRemoved)
        {
            continue;
        }
        /// Cond-1: the plan hosting the pinned upstream operator.
        const auto upstreamPlan = globalExecutionPlan.findDecomposedQueryPlanHosting(queryId, upstreamOperator);
        INVARIANT(upstreamPlan.has_value(), "Placed operator {} of query {} is not hosted by any plan", upstreamOperator, queryId);
        units.insert({.worker = upstreamPlan->worker, .plan = upstreamPlan->id});

        /// Cond-2: the plan after a removed link on the way to a downstream operator on another worker.
        for (const auto& downstreamOperator : graph.getDownstreamOperators(upstreamOperator))
        {
            const auto downstreamState = sharedQueryPlan.getOperatorState(downstreamOperator);
            if (downstreamState != OperatorState::Placed and downstreamState != OperatorState::ToBeRemoved
                and downstreamState != OperatorState::ToBeReplaced)
            {
                continue;
            }
            if (sharedQueryPlan.getPinnedWorker(downstreamOperator) == upstreamPlan->worker)
            {
                continue;
            }
            for (const auto& hop : globalExecutionPlan.getChannelChain(queryId, upstreamOperator, downstreamOperator))
            {
                if (not topology.hasLink(hop.channel.upstreamWorker, hop.channel.downstreamWorker))
                {
                    units.insert({.worker = hop.channel.downstreamWorker, .plan = hop.downstreamPlan});
                }
            }
        }
    }
}

namespace
{
/// Deviation: the original removes the re-placed operators from a plan and redeploys the rest of the plan. The current query engine cannot
/// change a running plan, so if a plan hosts operators to be re-placed and operators that stay, the operators that stay are re-placed as
/// well, so the plan is drained as a whole. The pinned operators are adjusted to the extended set: the operators adjacent to re-placed
/// operators are pinned.
ChangeLogEntry
expandToWholePlans(SharedQueryPlan& sharedQueryPlan, const GlobalExecutionPlan& globalExecutionPlan, const ChangeLogEntry& entry)
{
    const auto& queryId = sharedQueryPlan.getId();
    const auto& graph = sharedQueryPlan.getGraph();
    auto isReplaced = [&](const OperatorId op)
    {
        const auto state = sharedQueryPlan.getOperatorState(op);
        return state == OperatorState::ToBeReplaced or state == OperatorState::ToBeRemoved;
    };

    std::set<OperatorId> replaced;
    std::ranges::copy_if(entry.poSetOfSubQueryPlan, std::inserter(replaced, replaced.end()), isReplaced);
    bool expanded = false;
    for (bool changed = true; changed;)
    {
        changed = false;
        for (const auto& operatorId : std::set(replaced))
        {
            const auto plan = globalExecutionPlan.findDecomposedQueryPlanHosting(queryId, operatorId);
            if (not plan.has_value())
            {
                continue;
            }
            for (const auto& hosted : plan->hostedOperators)
            {
                if (replaced.insert(hosted).second)
                {
                    NES_DEBUG("Operator {} of query {} is re-placed, as plan {} hosts re-placed operators", hosted, queryId, plan->id);
                    sharedQueryPlan.setOperatorState(hosted, OperatorState::ToBeReplaced);
                    changed = true;
                    expanded = true;
                }
            }
        }
    }
    if (not expanded)
    {
        return entry;
    }

    std::set<OperatorId> upstreamOperators;
    std::set<OperatorId> downstreamOperators;
    std::ranges::copy_if(
        entry.upstreamOperators,
        std::inserter(upstreamOperators, upstreamOperators.end()),
        [&](auto op) { return not replaced.contains(op); });
    std::ranges::copy_if(
        entry.downstreamOperators,
        std::inserter(downstreamOperators, downstreamOperators.end()),
        [&](auto op) { return not replaced.contains(op); });
    for (const auto& operatorId : replaced)
    {
        for (const auto& child : graph.getUpstreamOperators(operatorId))
        {
            if (not replaced.contains(child))
            {
                upstreamOperators.insert(child);
            }
        }
        for (const auto& parent : graph.getDownstreamOperators(operatorId))
        {
            if (not replaced.contains(parent))
            {
                downstreamOperators.insert(parent);
            }
        }
    }
    return ChangeLogEntry::create(std::move(upstreamOperators), std::move(downstreamOperators), graph);
}
}

DeploymentUnit QueryPlacementAmendmentPhase::executeIncrementally(SharedQueryPlan& sharedQueryPlan)
{
    const auto& queryId = sharedQueryPlan.getId();
    const auto now = sharedQueryPlan.getCurrentChangeLogTimestamp();
    const auto version = globalExecutionPlan.getNextVersion();

    DeploymentUnit deploymentUnit;
    std::map<DecomposedQueryId, DeploymentContext> deploymentContexts;
    std::vector<ChangeLogEntry> failedChangeLogEntries;
    for (const auto& compactedEntry : sharedQueryPlan.getChangeLogEntries(now) | std::views::values)
    {
        try
        {
            const auto entry = expandToWholePlans(sharedQueryPlan, globalExecutionPlan, compactedEntry);

            /// P0: the plans at which the reconfiguration marker is injected.
            computeReconfigurationMarkerDeploymentUnit(sharedQueryPlan, entry, deploymentUnit.reconfigurationMarkerUnits);

            /// Stateful operators that possibly have to be migrated, with their current worker and plan.
            /// Shortcut (same as the original): only joins are considered stateful.
            std::map<OperatorId, DecomposedQueryPlan> statefulOperators;
            for (const auto& operatorId : entry.poSetOfSubQueryPlan)
            {
                if (sharedQueryPlan.getGraph().getOperator(operatorId).tryGetAs<JoinLogicalOperator>().has_value())
                {
                    if (const auto plan = globalExecutionPlan.findDecomposedQueryPlanHosting(queryId, operatorId))
                    {
                        statefulOperators.emplace(operatorId, *plan);
                    }
                }
            }

            std::set<OperatorId> upstreamOperators;
            std::set<OperatorId> downstreamOperators;
            std::ranges::copy_if(
                entry.upstreamOperators,
                std::inserter(upstreamOperators, upstreamOperators.end()),
                [&](auto op) { return sharedQueryPlan.getOperatorState(op) != OperatorState::Removed; });
            std::ranges::copy_if(
                entry.downstreamOperators,
                std::inserter(downstreamOperators, downstreamOperators.end()),
                [&](auto op) { return sharedQueryPlan.getOperatorState(op) != OperatorState::Removed; });

            /// P1: placement removal, P2: placement addition.
            const auto removal = PlacementRemovalStrategy(globalExecutionPlan, *workerCatalog)
                                     .updateGlobalExecutionPlan(sharedQueryPlan, upstreamOperators, downstreamOperators, version);
            for (const auto& [planId, context] : removal.deploymentContexts)
            {
                deploymentContexts.insert_or_assign(planId, context);
            }
            const auto addition = PlacementAdditionStrategy(globalExecutionPlan, copyPtr(workerCatalog), decomposer, networkConfiguration)
                                      .updateGlobalExecutionPlan(sharedQueryPlan, upstreamOperators, downstreamOperators, version, removal);
            for (const auto& [planId, context] : addition.deploymentContexts)
            {
                deploymentContexts.insert_or_assign(planId, context);
            }

            /// Stateful operators that moved hand their state over: their old plan is updated and drained.
            for (const auto& [operatorId, oldPlan] : statefulOperators)
            {
                const auto newWorker = sharedQueryPlan.getPinnedWorker(operatorId);
                if (not newWorker.has_value() or *newWorker == oldPlan.worker)
                {
                    continue;
                }
                const auto newPlan = globalExecutionPlan.findDecomposedQueryPlanHosting(queryId, operatorId);
                INVARIANT(newPlan.has_value(), "Re-placed operator {} of query {} is not hosted by any plan", operatorId, queryId);
                deploymentUnit.stateMigrations.emplace_back(
                    StateMigration{
                        .operatorId = operatorId,
                        .oldWorker = oldPlan.worker,
                        .oldPlan = oldPlan.id,
                        .newWorker = *newWorker,
                        .newPlan = newPlan->id});
                auto& context = deploymentContexts.at(oldPlan.id);
                context.plan.state = DecomposedQueryPlanState::MarkedForUpdateAndDrain;
                globalExecutionPlan.setState(queryId, oldPlan.id, DecomposedQueryPlanState::MarkedForUpdateAndDrain);
                NES_DEBUG("Stateful operator {} of query {} moves from {} to {}", operatorId, queryId, oldPlan.worker, *newWorker);
            }
        }
        catch (const Exception& exception)
        {
            NES_ERROR(
                "Failed to process a change log entry of query {}. The entry is recorded again for further processing: {}",
                queryId,
                exception.what());
            sharedQueryPlan.setStatus(SharedQueryPlanStatus::PartiallyProcessed);
            failedChangeLogEntries.emplace_back(compactedEntry);
        }
    }
    if (not failedChangeLogEntries.empty())
    {
        sharedQueryPlan.recordFailedChangeLogEntries(failedChangeLogEntries);
    }
    sharedQueryPlan.updateProcessedChangeLogTimestamp(now);
    if (sharedQueryPlan.getStatus() != SharedQueryPlanStatus::PartiallyProcessed
        and sharedQueryPlan.getStatus() != SharedQueryPlanStatus::Stopped)
    {
        sharedQueryPlan.setStatus(SharedQueryPlanStatus::Processed);
    }
    for (auto& context : deploymentContexts | std::views::values)
    {
        deploymentUnit.deploymentAdditionContexts.emplace_back(std::move(context));
    }
    return deploymentUnit;
}

namespace
{
LogicalOperator removePlacementTraits(const LogicalOperator& op)
{
    auto traitSet = op.getTraitSet() | std::views::filter([](const auto& trait) { return trait.getTypeInfo() != typeid(PlacementTrait); })
        | std::ranges::to<TraitSet>();
    auto children = op.getChildren() | std::views::transform(removePlacementTraits) | std::ranges::to<std::vector>();
    return op.withTraitSet(traitSet).withChildren(std::move(children));
}
}

DeploymentUnit QueryPlacementAmendmentPhase::executeHolistically(SharedQueryPlan& sharedQueryPlan)
{
    const auto queryId = sharedQueryPlan.getId();
    DeploymentUnit deploymentUnit;

    /// 1. Remove all plans of the query and release the slots of its operators.
    for (auto plan : globalExecutionPlan.getDecomposedQueryPlans(queryId))
    {
        const auto oldVersion = plan.version;
        plan.state = DecomposedQueryPlanState::MarkedForMigration;
        deploymentUnit.deploymentRemovalContexts.emplace_back(
            DeploymentContext{
                .plan = std::move(plan), .oldVersion = oldVersion, .forMigration = false, .sinkUpdates = {}, .detachedChannels = {}});
    }
    const auto& graph = sharedQueryPlan.getGraph();
    for (const auto& operatorId : graph.getAllOperators())
    {
        if (const auto worker = sharedQueryPlan.getPinnedWorker(operatorId))
        {
            [[maybe_unused]] const auto released
                = workerCatalog->releaseSlots(*worker, getOperatorCapacityDemand(graph.getOperator(operatorId)));
        }
    }
    globalExecutionPlan.removeQuery(queryId);

    /// 2. Place and decompose the query again.
    auto plan = sharedQueryPlan.getPlan();
    plan = plan.withRootOperators(plan.getRootOperators() | std::views::transform(removePlacementTraits) | std::ranges::to<std::vector>());
    BottomUpOperatorPlacer(std::shared_ptr<const WorkerCatalog>(copyPtr(workerCatalog))).apply(plan);
    const auto distributedPlan = decomposer.decompose(plan, networkConfiguration);
    const auto placement = getPlacement(distributedPlan.getGlobalPlan());
    const OperatorGraph newGraph(distributedPlan.getGlobalPlan());
    for (const auto& [operatorId, worker] : placement)
    {
        workerCatalog->occupySlots(worker, getOperatorCapacityDemand(newGraph.getOperator(operatorId)));
    }
    globalExecutionPlan.addQuery(queryId, distributedPlan);
    for (auto newPlan : globalExecutionPlan.getDecomposedQueryPlans(queryId))
    {
        newPlan.state = DecomposedQueryPlanState::MarkedForDeployment;
        globalExecutionPlan.addOrUpdateDecomposedQueryPlan(queryId, newPlan);
        deploymentUnit.deploymentAdditionContexts.emplace_back(
            DeploymentContext{
                .plan = std::move(newPlan), .oldVersion = std::nullopt, .forMigration = false, .sinkUpdates = {}, .detachedChannels = {}});
    }
    sharedQueryPlan = SharedQueryPlan(queryId, distributedPlan.getGlobalPlan(), placement);
    sharedQueryPlan.setStatus(SharedQueryPlanStatus::Processed);
    return deploymentUnit;
}

}
