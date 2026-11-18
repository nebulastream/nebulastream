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

#include <Placement/PlacementRemovalStrategy.hpp>

#include <algorithm>
#include <cstddef>
#include <deque>
#include <map>
#include <ranges>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Placement/BottomUpPlacement.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>
#include <DeploymentContext.hpp>
#include <ErrorHandling.hpp>
#include <GlobalExecutionPlan.hpp>
#include <SharedQueryPlan.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

std::set<OperatorId> getOperatorsToProcess(
    const SharedQueryPlan& sharedQueryPlan,
    const std::set<OperatorId>& pinnedUpstreamOperators,
    const std::set<OperatorId>& pinnedDownstreamOperators)
{
    std::set<OperatorId> operatorsToProcess;
    std::deque<OperatorId> toVisit(pinnedUpstreamOperators.begin(), pinnedUpstreamOperators.end());
    while (not toVisit.empty())
    {
        const auto current = toVisit.front();
        toVisit.pop_front();
        if (not operatorsToProcess.insert(current).second or pinnedDownstreamOperators.contains(current))
        {
            continue;
        }
        for (const auto& parent : sharedQueryPlan.getGraph().getDownstreamOperators(current))
        {
            toVisit.push_back(parent);
        }
    }
    return operatorsToProcess;
}

PlacementRemovalStrategy::PlacementRemovalStrategy(GlobalExecutionPlan& globalExecutionPlan, WorkerCatalog& workerCatalog)
    : globalExecutionPlan(globalExecutionPlan), workerCatalog(workerCatalog)
{
}

PlacementRemovalResult PlacementRemovalStrategy::updateGlobalExecutionPlan(
    SharedQueryPlan& sharedQueryPlan,
    const std::set<OperatorId>& pinnedUpstreamOperators,
    const std::set<OperatorId>& pinnedDownstreamOperators,
    const DecomposedQueryPlanVersion version)
{
    const auto& queryId = sharedQueryPlan.getId();
    const auto& graph = sharedQueryPlan.getGraph();
    const auto topology = workerCatalog.getTopology();
    auto pinnedWorkerOf = [&](const OperatorId operatorId)
    {
        const auto worker = sharedQueryPlan.getPinnedWorker(operatorId);
        INVARIANT(worker.has_value(), "Operator {} of query {} has no pinned worker", operatorId, queryId);
        return *worker;
    };

    /// 1. Collect the operators between the pinned operators and the ones to be re-placed or removed.
    const auto operatorsToProcess = getOperatorsToProcess(sharedQueryPlan, pinnedUpstreamOperators, pinnedDownstreamOperators);
    std::set<OperatorId> removedOperators;
    for (const auto& operatorId : operatorsToProcess)
    {
        const auto state = sharedQueryPlan.getOperatorState(operatorId);
        if (state == OperatorState::Removed)
        {
            throw PlacementFailure("Operator {} of query {} was already removed", operatorId, queryId);
        }
        if (state == OperatorState::ToBeReplaced or state == OperatorState::ToBeRemoved)
        {
            removedOperators.insert(operatorId);
        }
    }

    /// 2. Find the channel chains that are no longer valid: chains from or to an operator to be re-placed, and chains using a removed link.
    std::map<LogicalEdge, std::vector<PlacedNetworkChannel>> removedChains;
    for (const auto& upstream : operatorsToProcess)
    {
        if (pinnedDownstreamOperators.contains(upstream))
        {
            continue;
        }
        for (const auto& downstream : graph.getDownstreamOperators(upstream))
        {
            if (not operatorsToProcess.contains(downstream) or pinnedWorkerOf(upstream) == pinnedWorkerOf(downstream))
            {
                continue;
            }
            auto chain = globalExecutionPlan.getChannelChain(queryId, upstream, downstream);
            const bool usesRemovedLink = std::ranges::any_of(
                chain, [&](const auto& hop) { return not topology.hasLink(hop.channel.upstreamWorker, hop.channel.downstreamWorker); });
            if (removedOperators.contains(upstream) or removedOperators.contains(downstream) or usesRemovedLink)
            {
                removedChains.emplace(LogicalEdge{upstream, downstream}, std::move(chain));
            }
        }
    }

    /// 3. Find the plans to drain: the plans hosting operators to be re-placed and the forwarding plans of removed chains.
    std::set<DecomposedQueryId> drainedPlans;
    for (const auto& operatorId : removedOperators)
    {
        const auto plan = globalExecutionPlan.findDecomposedQueryPlanHosting(queryId, operatorId);
        INVARIANT(plan.has_value(), "Placed operator {} of query {} is not hosted by any decomposed query plan", operatorId, queryId);
        if (not std::ranges::includes(removedOperators, plan->hostedOperators))
        {
            throw PlacementFailure(
                "Decomposed query plan {} of query {} hosts operators to be re-placed and operators that stay ({}). The plan cannot be "
                "reconfigured incrementally.",
                plan->id,
                queryId,
                fmt::join(plan->hostedOperators, ", "));
        }
        drainedPlans.insert(plan->id);
    }
    for (const auto& chain : removedChains | std::views::values)
    {
        for (const auto& hop : chain | std::views::take(chain.size() - 1))
        {
            drainedPlans.insert(hop.downstreamPlan);
        }
    }

    PlacementRemovalResult result;
    for (const auto& [edge, chain] : removedChains)
    {
        if (not drainedPlans.contains(chain.front().upstreamPlan))
        {
            result.removedFirstHops.emplace(edge, chain.front());
        }
        if (not drainedPlans.contains(chain.back().downstreamPlan))
        {
            result.removedLastHops.emplace(edge, chain.back());
        }
    }

    /// 4. Release the slots of the operators to be re-placed or removed.
    std::unordered_map<Host, size_t> releasedSlots;
    for (const auto& operatorId : removedOperators)
    {
        releasedSlots[pinnedWorkerOf(operatorId)] += getOperatorCapacityDemand(graph.getOperator(operatorId));
    }
    for (const auto& [worker, slots] : releasedSlots)
    {
        if (not workerCatalog.releaseSlots(worker, slots))
        {
            throw PlacementFailure("Unable to release {} slots on worker {} for query {}", slots, worker, queryId);
        }
    }

    /// 5. Mark the plans to drain and compute their deployment contexts. A drained plan detaches from receivers that keep running.
    const auto channels = globalExecutionPlan.getChannels(queryId);
    for (const auto& planId : drainedPlans)
    {
        auto plan = globalExecutionPlan.getDecomposedQueryPlan(queryId, planId);
        INVARIANT(plan.has_value(), "Decomposed query plan {} of query {} does not exist", planId, queryId);
        DeploymentContext context{
            .plan = *plan, .oldVersion = plan->version, .forMigration = false, .sinkUpdates = {}, .detachedChannels = {}};
        for (const auto& placed : channels)
        {
            if (placed.upstreamPlan == planId and not drainedPlans.contains(placed.downstreamPlan))
            {
                context.detachedChannels.emplace_back(placed.channel.id);
            }
        }
        context.plan.version = version;
        context.plan.state = DecomposedQueryPlanState::MarkedForMigration;
        globalExecutionPlan.addOrUpdateDecomposedQueryPlan(queryId, context.plan);
        result.deploymentContexts.emplace(planId, std::move(context));
    }
    for (const auto& chain : removedChains | std::views::values)
    {
        for (const auto& hop : chain)
        {
            globalExecutionPlan.removeChannel(queryId, hop.channel.id);
        }
    }

    /// 6. Update the operator states. Operators to be re-placed lose their worker, unless they are pinned (sources, sinks and the pinned
    /// upstream operators).
    for (const auto& operatorId : removedOperators)
    {
        if (sharedQueryPlan.getOperatorState(operatorId) == OperatorState::ToBeRemoved)
        {
            sharedQueryPlan.setOperatorState(operatorId, OperatorState::Removed);
            continue;
        }
        sharedQueryPlan.setOperatorState(operatorId, OperatorState::ToBePlaced);
        const auto& op = graph.getOperator(operatorId);
        const bool isSourceOrSink
            = op.tryGetAs<SourceDescriptorLogicalOperator>().has_value() or op.tryGetAs<SinkLogicalOperator>().has_value();
        if (not isSourceOrSink and not pinnedUpstreamOperators.contains(operatorId))
        {
            sharedQueryPlan.removePinnedWorker(operatorId);
        }
    }
    NES_DEBUG(
        "Placement removal of query {}: drained plans [{}], removed chains {}",
        queryId,
        fmt::join(drainedPlans, ", "),
        removedChains.size());
    return result;
}

}
