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

#include <Placement/PlacementAdditionStrategy.hpp>

#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Placement/BottomUpPlacement.hpp>
#include <Placement/PlacementRemovalStrategy.hpp>
#include <Placement/QueryDecomposition.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <fmt/ranges.h>
#include <DeploymentContext.hpp>
#include <ErrorHandling.hpp>
#include <GlobalExecutionPlan.hpp>
#include <NetworkTopology.hpp>
#include <QueryId.hpp>
#include <QueryOptimizerNetworkConfiguration.hpp>
#include <SharedQueryPlan.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

PlacementAdditionStrategy::PlacementAdditionStrategy(
    GlobalExecutionPlan& globalExecutionPlan,
    SharedPtr<WorkerCatalog> workerCatalog,
    const QueryDecomposer& decomposer,
    QueryOptimizerNetworkConfiguration networkConfiguration)
    : globalExecutionPlan(globalExecutionPlan)
    , workerCatalog(std::move(workerCatalog))
    , decomposer(decomposer)
    , networkConfiguration(std::move(networkConfiguration))
{
}

namespace
{
/// The network operators of a new chain of channels realizing one logical edge.
struct Chain
{
    std::vector<Host> path;
    std::vector<std::string> channelIds;
    /// Network sources and sinks per hop. The sink of the first hop is not used if the upstream operator stays.
    std::vector<LogicalOperator> sources;
    std::vector<LogicalOperator> sinks;
    /// Plans holding the network sink of each hop.
    std::vector<DecomposedQueryId> upstreamPlans;
    /// Plans holding the network source of each hop.
    std::vector<DecomposedQueryId> downstreamPlans;
};

LogicalOperator withPlacement(const LogicalOperator& op, const Host& worker)
{
    auto traitSet = op.getTraitSet() | std::views::filter([](const auto& trait) { return trait.getTypeInfo() != typeid(PlacementTrait); })
        | std::ranges::to<TraitSet>();
    traitSet.insert(PlacementTrait{worker});
    return op.withTraitSet(traitSet);
}

/// Computes the workers on which operators between the pinned operators may be placed: all workers on paths from the workers of the pinned
/// upstream operators to the workers of the pinned downstream operators.
///
/// Deviation: the original selects one path per pinned upstream worker before the placement. The bottom-up placement of the current code
/// base is an ILP, so the port gives it all workers on the paths and lets it choose.
std::unordered_set<Host>
selectCandidateWorkers(const NetworkTopology& topology, const std::set<Host>& upstreamWorkers, const std::set<Host>& downstreamWorkers)
{
    std::unordered_set<Host> candidates;
    for (const auto& upstreamWorker : upstreamWorkers)
    {
        candidates.insert(upstreamWorker);
        for (const auto& downstreamWorker : downstreamWorkers)
        {
            candidates.insert(downstreamWorker);
            for (const auto& path : topology.findPaths(upstreamWorker, downstreamWorker, NetworkTopology::Downstream))
            {
                candidates.insert(path.begin(), path.end());
            }
        }
    }
    return candidates;
}
}

PlacementAdditionResult PlacementAdditionStrategy::updateGlobalExecutionPlan(
    SharedQueryPlan& sharedQueryPlan,
    const std::set<OperatorId>& pinnedUpstreamOperators,
    const std::set<OperatorId>& pinnedDownstreamOperators,
    const DecomposedQueryPlanVersion version,
    const PlacementRemovalResult& removal)
{
    const auto& queryId = sharedQueryPlan.getId();
    const auto& graph = sharedQueryPlan.getGraph();
    const auto topology = workerCatalog->getTopology();
    auto workerOf = [&](const OperatorId operatorId)
    {
        const auto worker = sharedQueryPlan.getPinnedWorker(operatorId);
        INVARIANT(worker.has_value(), "Operator {} of query {} has no worker", operatorId, queryId);
        return *worker;
    };

    const auto operatorsToProcess = getOperatorsToProcess(sharedQueryPlan, pinnedUpstreamOperators, pinnedDownstreamOperators);
    std::set<OperatorId> operatorsToPlace;
    std::ranges::copy_if(
        operatorsToProcess,
        std::inserter(operatorsToPlace, operatorsToPlace.end()),
        [&](const auto operatorId) { return sharedQueryPlan.getOperatorState(operatorId) == OperatorState::ToBePlaced; });

    /// 1. Path selection and placement of the operators to be placed.
    if (not operatorsToPlace.empty())
    {
        std::set<Host> upstreamWorkers;
        std::set<Host> downstreamWorkers;
        std::ranges::transform(pinnedUpstreamOperators, std::inserter(upstreamWorkers, upstreamWorkers.end()), workerOf);
        std::ranges::transform(pinnedDownstreamOperators, std::inserter(downstreamWorkers, downstreamWorkers.end()), workerOf);
        const auto candidates = selectCandidateWorkers(topology, upstreamWorkers, downstreamWorkers);

        std::unordered_map<OperatorId, Host> fixedPlacement;
        for (const auto& operatorId : graph.getAllOperators())
        {
            if (const auto worker = sharedQueryPlan.getPinnedWorker(operatorId);
                worker.has_value() and not operatorsToPlace.contains(operatorId))
            {
                fixedPlacement.emplace(operatorId, *worker);
            }
        }
        std::unordered_map<Host, Capacity> availableSlots;
        for (const auto& candidate : candidates)
        {
            availableSlots.emplace(candidate, workerCatalog->getAvailableSlots(candidate));
        }
        const auto placement = BottomUpOperatorPlacer(std::shared_ptr<const WorkerCatalog>(copyPtr(workerCatalog)))
                                   .placeIncrementally(sharedQueryPlan.getPlan(), fixedPlacement, candidates, availableSlots);

        /// Optimistic validation: occupy the slots the placement assumed. Fails if a concurrent amendment occupied them in the meantime.
        std::unordered_map<Host, size_t> occupiedSlots;
        for (const auto& operatorId : operatorsToPlace)
        {
            occupiedSlots[placement.at(operatorId)] += getOperatorCapacityDemand(graph.getOperator(operatorId));
        }
        std::vector<std::pair<Host, size_t>> occupied;
        for (const auto& [worker, slots] : occupiedSlots)
        {
            if (not workerCatalog->tryOccupySlots(worker, slots))
            {
                for (const auto& [occupiedWorker, occupiedCount] : occupied)
                {
                    [[maybe_unused]] const auto released = workerCatalog->releaseSlots(occupiedWorker, occupiedCount);
                }
                throw PlacementFailure("Unable to occupy {} slots on worker {} for query {}", slots, worker, queryId);
            }
            occupied.emplace_back(worker, slots);
        }
        for (const auto& operatorId : operatorsToPlace)
        {
            sharedQueryPlan.setPinnedWorker(operatorId, placement.at(operatorId));
        }
    }

    /// 2. Connected operators to be placed on the same worker form a new plan.
    std::unordered_map<OperatorId, DecomposedQueryId> componentOf;
    std::map<DecomposedQueryId, std::vector<OperatorId>> components;
    for (const auto& operatorId : operatorsToPlace)
    {
        if (componentOf.contains(operatorId))
        {
            continue;
        }
        const auto planId = globalExecutionPlan.getNextDecomposedQueryId();
        std::vector toVisit{operatorId};
        while (not toVisit.empty())
        {
            const auto current = toVisit.back();
            toVisit.pop_back();
            if (not componentOf.try_emplace(current, planId).second)
            {
                continue;
            }
            components[planId].emplace_back(current);
            for (const auto& neighbor : graph.getUpstreamOperators(current))
            {
                if (operatorsToPlace.contains(neighbor) and workerOf(neighbor) == workerOf(current))
                {
                    toVisit.emplace_back(neighbor);
                }
            }
            for (const auto& neighbor : graph.getDownstreamOperators(current))
            {
                if (operatorsToPlace.contains(neighbor) and workerOf(neighbor) == workerOf(current))
                {
                    toVisit.emplace_back(neighbor);
                }
            }
        }
    }

    /// 3. Find the edges to connect: edges from or to a placed operator between different plans, and edges whose chain was removed.
    std::set<LogicalEdge> edgesToConnect;
    for (const auto& upstream : operatorsToProcess)
    {
        if (pinnedDownstreamOperators.contains(upstream))
        {
            continue;
        }
        for (const auto& downstream : graph.getDownstreamOperators(upstream))
        {
            if (not operatorsToProcess.contains(downstream))
            {
                continue;
            }
            const LogicalEdge edge{upstream, downstream};
            const bool placeUpstream = operatorsToPlace.contains(upstream);
            const bool placeDownstream = operatorsToPlace.contains(downstream);
            if (placeUpstream and placeDownstream and componentOf.at(upstream) == componentOf.at(downstream))
            {
                continue;
            }
            if (placeUpstream or placeDownstream or removal.removedFirstHops.contains(edge) or removal.removedLastHops.contains(edge))
            {
                edgesToConnect.insert(edge);
            }
        }
    }

    /// 4. Build the chains and the operator trees of the new plans.
    std::map<LogicalEdge, Chain> chains;
    std::unordered_map<OperatorId, LogicalOperator> builtOperators;
    std::map<DecomposedQueryId, DeploymentContext> contexts;
    std::vector<std::pair<Host, DecomposedQueryPlan>> forwardingPlans;

    std::function<LogicalOperator(OperatorId)> build;
    std::function<const Chain&(const LogicalEdge&)> createChain;

    build = [&](const OperatorId operatorId) -> LogicalOperator
    {
        if (const auto it = builtOperators.find(operatorId); it != builtOperators.end())
        {
            return it->second;
        }
        std::vector<LogicalOperator> children;
        for (const auto& child : graph.getUpstreamOperators(operatorId))
        {
            if (operatorsToPlace.contains(child) and componentOf.at(child) == componentOf.at(operatorId))
            {
                children.emplace_back(build(child));
            }
            else
            {
                children.emplace_back(createChain({child, operatorId}).sources.back());
            }
        }
        auto built = withPlacement(graph.getOperator(operatorId), workerOf(operatorId)).withChildren(std::move(children));
        builtOperators.emplace(operatorId, built);
        return built;
    };

    createChain = [&](const LogicalEdge& edge) -> const Chain&
    {
        if (const auto it = chains.find(edge); it != chains.end())
        {
            return it->second;
        }
        const auto& [upstream, downstream] = edge;
        const bool placeUpstream = operatorsToPlace.contains(upstream);
        const bool placeDownstream = operatorsToPlace.contains(downstream);
        const auto upstreamWorker = workerOf(upstream);
        const auto downstreamWorker = workerOf(downstream);

        Chain chain;
        if (upstreamWorker == downstreamWorker)
        {
            /// A pinned and a placed operator on the same worker are in different plans, so they are connected by a channel on the worker.
            chain.path = {upstreamWorker, downstreamWorker};
        }
        else
        {
            const auto paths = topology.findPaths(upstreamWorker, downstreamWorker, NetworkTopology::Downstream);
            if (paths.empty())
            {
                throw PlacementFailure(
                    "No path from {} to {} to connect operator {} with {}", upstreamWorker, downstreamWorker, upstream, downstream);
            }
            chain.path = paths.front().path;
        }
        const auto numberOfHops = chain.path.size() - 1;

        /// The last hop to a pinned downstream operator reuses the channel of the removed chain.
        std::optional<PlacedNetworkChannel> lastHopToAttach;
        if (not placeDownstream)
        {
            const auto it = removal.removedLastHops.find(edge);
            if (it == removal.removedLastHops.end())
            {
                throw PlacementFailure(
                    "Operator {} of query {} stays on its worker but has no channel to attach to for its upstream operator {}",
                    downstream,
                    queryId,
                    upstream);
            }
            lastHopToAttach = it->second;
        }
        std::optional<PlacedNetworkChannel> firstHopToUpdate;
        if (not placeUpstream)
        {
            const auto it = removal.removedFirstHops.find(edge);
            if (it == removal.removedFirstHops.end())
            {
                throw PlacementFailure(
                    "Operator {} of query {} stays on its worker but has no network sink to update for its downstream operator {}",
                    upstream,
                    queryId,
                    downstream);
            }
            firstHopToUpdate = it->second;
        }

        LogicalOperator upstreamOperator = placeUpstream ? build(upstream) : graph.getOperator(upstream);
        for (size_t hop = 0; hop < numberOfHops; ++hop)
        {
            const bool isLastHop = hop + 1 == numberOfHops;
            const auto channelId = isLastHop and lastHopToAttach.has_value() ? lastHopToAttach->channel.id : UUIDToString(generateUUID());
            auto [source, sink] = decomposer.createNetworkOperators(
                channelId, upstreamOperator, chain.path.at(hop), chain.path.at(hop + 1), networkConfiguration);
            chain.channelIds.emplace_back(channelId);
            chain.sources.emplace_back(source);
            chain.sinks.emplace_back(sink);
            if (hop == 0)
            {
                chain.upstreamPlans.emplace_back(placeUpstream ? componentOf.at(upstream) : firstHopToUpdate->upstreamPlan);
            }
            else
            {
                /// A forwarding plan on an intermediate worker.
                const auto planId = globalExecutionPlan.getNextDecomposedQueryId();
                chain.upstreamPlans.emplace_back(planId);
                chain.downstreamPlans.emplace_back(planId);
                forwardingPlans.emplace_back(
                    chain.path.at(hop),
                    DecomposedQueryPlan{
                        .id = planId,
                        .version = version,
                        .worker = chain.path.at(hop),
                        .plan = LogicalPlan(INVALID_QUERY_ID, {sink}),
                        .hostedOperators = {},
                        .state = DecomposedQueryPlanState::MarkedForDeployment,
                        .localQueryId = std::nullopt});
            }
            upstreamOperator = source;
        }
        chain.downstreamPlans.emplace_back(placeDownstream ? componentOf.at(downstream) : lastHopToAttach->downstreamPlan);

        /// A pinned upstream operator keeps running in its plan, whose network sink connects to the first hop.
        if (firstHopToUpdate.has_value())
        {
            const auto upstreamPlanId = firstHopToUpdate->upstreamPlan;
            auto it = contexts.find(upstreamPlanId);
            if (it == contexts.end())
            {
                auto plan = globalExecutionPlan.getDecomposedQueryPlan(queryId, upstreamPlanId);
                INVARIANT(plan.has_value(), "Decomposed query plan {} of query {} does not exist", upstreamPlanId, queryId);
                DeploymentContext context{
                    .plan = *plan, .oldVersion = plan->version, .forMigration = false, .sinkUpdates = {}, .detachedChannels = {}};
                context.plan.version = version;
                context.plan.state = DecomposedQueryPlanState::MarkedForRedeployment;
                it = contexts.emplace(upstreamPlanId, std::move(context)).first;
            }
            const auto receiver = workerCatalog->getWorker(chain.path.at(1));
            INVARIANT(receiver.has_value(), "Worker {} is not part of the worker catalog", chain.path.at(1));
            it->second.sinkUpdates.emplace_back(
                NetworkSinkUpdate{
                    .oldChannel = firstHopToUpdate->channel.id,
                    .newChannel = chain.channelIds.front(),
                    .newDataEndpoint = receiver->dataAddress});
        }
        return chains.emplace(edge, std::move(chain)).first->second;
    };

    for (const auto& edge : edgesToConnect)
    {
        createChain(edge);
    }

    /// 5. Register the new plans, the updated plans and the new channels.
    for (const auto& [planId, operators] : components)
    {
        std::vector<LogicalOperator> roots;
        for (const auto& [edge, chain] : chains)
        {
            if (operatorsToPlace.contains(edge.first) and componentOf.at(edge.first) == planId)
            {
                roots.emplace_back(chain.sinks.front());
            }
        }
        INVARIANT(not roots.empty(), "The new plan {} of query {} does not send its results anywhere", planId, queryId);
        if (roots.size() > 1)
        {
            /// Shortcut: the query engine runs exactly one sink per local plan. Operators on the same worker with several outgoing edges
            /// (shared sub-plans) are not split into several plans.
            throw PlacementFailure(
                "The new plan {} of query {} would have {} sinks, but a plan can only have one", planId, queryId, roots.size());
        }
        DecomposedQueryPlan plan{
            .id = planId,
            .version = version,
            .worker = workerOf(operators.front()),
            .plan = LogicalPlan(INVALID_QUERY_ID, std::move(roots)),
            .hostedOperators = {operators.begin(), operators.end()},
            .state = DecomposedQueryPlanState::MarkedForDeployment,
            .localQueryId = std::nullopt};
        globalExecutionPlan.addOrUpdateDecomposedQueryPlan(queryId, plan);
        contexts.emplace(
            planId,
            DeploymentContext{
                .plan = std::move(plan), .oldVersion = std::nullopt, .forMigration = false, .sinkUpdates = {}, .detachedChannels = {}});
    }
    for (auto& [worker, plan] : forwardingPlans)
    {
        globalExecutionPlan.addOrUpdateDecomposedQueryPlan(queryId, plan);
        const auto planId = plan.id;
        contexts.emplace(
            planId,
            DeploymentContext{
                .plan = std::move(plan), .oldVersion = std::nullopt, .forMigration = false, .sinkUpdates = {}, .detachedChannels = {}});
    }
    for (const auto& context : contexts | std::views::values)
    {
        if (context.plan.state == DecomposedQueryPlanState::MarkedForRedeployment)
        {
            globalExecutionPlan.addOrUpdateDecomposedQueryPlan(queryId, context.plan);
        }
    }
    for (const auto& [edge, chain] : chains)
    {
        for (size_t hop = 0; hop + 1 < chain.path.size(); ++hop)
        {
            globalExecutionPlan.addChannel(
                queryId,
                PlacedNetworkChannel{
                    .channel
                    = NetworkChannel{.id = chain.channelIds.at(hop), .upstreamWorker = chain.path.at(hop), .downstreamWorker = chain.path.at(hop + 1), .upstreamOperatorId = edge.first, .downstreamOperatorId = edge.second, .upstreamPlanIndex = 0, .downstreamPlanIndex = 0},
                    .upstreamPlan = chain.upstreamPlans.at(hop),
                    .downstreamPlan = chain.downstreamPlans.at(hop)});
        }
    }
    for (const auto& operatorId : operatorsToPlace)
    {
        sharedQueryPlan.setOperatorState(operatorId, OperatorState::Placed);
    }
    NES_DEBUG(
        "Placement addition of query {}: placed operators [{}], {} new plans, {} connected edges",
        queryId,
        fmt::join(operatorsToPlace, ", "),
        components.size() + forwardingPlans.size(),
        chains.size());
    return {.deploymentContexts = std::move(contexts)};
}

}
