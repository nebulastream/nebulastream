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

#include <LegacyOptimizer/QueryDecomposition.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Util/UUID.hpp>


#include <Iterators/BFSIterator.hpp>
#include <LegacyOptimizer/OperatorPlacement.hpp>
#include <LegacyOptimizer/QueryPlanning.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <ErrorHandling.hpp>
#include <NetworkTopology.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

QueryDecomposer::QueryDecomposer(QueryPlanningContext& context) : context{context}
{
}

QueryDecomposer QueryDecomposer::with(QueryPlanningContext& context)
{
    return QueryDecomposer{context};
}

PlanStage::DecomposedLogicalPlan<HostAddr> QueryDecomposer::decompose(PlanStage::PlacedLogicalPlan&& placedPlan) &&
{
    PRECONDITION(placedPlan.plan.getRootOperators().size() == 1, "BUG: query decomposition requires a single root operator");
    PRECONDITION(
        std::ranges::all_of(
            BFSRange(placedPlan.plan.getRootOperators().front()),
            [](const auto& op) { return hasTrait<PlacementTrait>(op.getTraitSet()); }),
        "BUG: query decomposition requires placement of all operators");

    auto root = decomposePlanRecursive(placedPlan.plan.getRootOperators().front());
    addPlanToNode(std::move(root), getPlacementFor(root));

    for (const auto& [node, plans] : plansByNode)
    {
        for (const auto& plan : plans)
        {
            NES_DEBUG("Plan fragment on node [{}]: {}", node, plan);
        }
    }

    return PlanStage::DecomposedLogicalPlan{plansByNode};
}

LogicalOperator QueryDecomposer::decomposePlanRecursive(const LogicalOperator& op)
{
    std::vector<LogicalOperator> assignedChildren;
    assignedChildren.reserve(op.getChildren().size());

    for (const auto& child : op.getChildren())
    {
        assignedChildren.emplace_back(assignOperator(op, child));
    }

    return op.withChildren({std::move(assignedChildren)});
}

LogicalOperator QueryDecomposer::assignOperator(const LogicalOperator& op, const LogicalOperator& child)
{
    auto assignedChild = decomposePlanRecursive(child);

    const auto opPlacement = getPlacementFor(op);
    const auto childPlacement = getPlacementFor(child);

    if (opPlacement == childPlacement)
    {
        return assignedChild;
    }
    return createNetworkChannel(assignedChild, childPlacement, opPlacement);
}

LogicalOperator
QueryDecomposer::createNetworkChannel(const LogicalOperator& op, const Topology::NodeId& startNode, const Topology::NodeId& endNode)
{
    /// Ask the topology for a path of nodes that connects upstream and downstream, currently we use any of them
    const auto paths = context.workerCatalog->getTopology().findPaths(startNode, endNode, Topology::Direction::Downstream);
    if (paths.empty())
    {
        throw PlacementFailure("No path from {} to {} found", startNode, endNode);
    }
    const auto [path] = paths.front();
    INVARIANT(path.size() >= 2, "Path from {} to {} must contain at least 2 nodes", startNode, endNode);

    LogicalOperator currentOp = op;
    for (size_t i = 0; i < path.size() - 1; ++i)
    {
        const auto& upstreamNode = path.at(i);
        const auto& downstreamNode = path.at(i + 1);

        auto [networkSource, networkSink] = connect(NetworkChannel{
            .id = ChannelId(generateUUID()), .upstreamOp = op, .upstreamNode = upstreamNode, .downstreamNode = downstreamNode});
        addPlanToNode(networkSink.withChildren({std::move(currentOp)}), upstreamNode);
        currentOp = networkSource;
    }

    return currentOp;
}

QueryDecomposer::Bridge QueryDecomposer::connect(const NetworkChannel& channel)
{
    const auto logicalSourceOpt = context.sourceCatalog->addLogicalSource(channel.id.getRawValue(), channel.upstreamOp.getOutputSchema());
    INVARIANT(logicalSourceOpt.has_value(), "Failed to add logical source for network channel");
    const auto& logicalSource = logicalSourceOpt.value();

    auto networkSourceDescriptorOpt = context.sourceCatalog->addPhysicalSource(
        logicalSource,
        "Network",
        channel.downstreamNode.getRawValue(),
        {{"channel", channel.id.getRawValue()}, {"bind", channel.downstreamNode.getRawValue()}},
        {{"type", "Native"}});

    INVARIANT(networkSourceDescriptorOpt.has_value(), "Failed to add physical source for network channel");
    const auto& networkSourceDescriptor = networkSourceDescriptorOpt.value();

    auto networkSinkDescriptor = context.sinkCatalog->addSinkDescriptor(
        channel.id.getRawValue(),
        channel.upstreamOp.getOutputSchema(),
        "Network",
        channel.upstreamNode.getRawValue(),
        {{"channel", channel.id.getRawValue()},
         {"bind", channel.upstreamNode.getRawValue()},
         {"connection", channel.downstreamNode.getRawValue()}});

    INVARIANT(networkSinkDescriptor.has_value(), "Invalid sink descriptor config for network sink");

    auto outputOriginIds = channel.upstreamOp.getTraitSet().get<OutputOriginIdsTrait>();
    return Bridge{
        SourceDescriptorLogicalOperator{networkSourceDescriptor}.withTraitSet(TraitSet{outputOriginIds}),
        SinkLogicalOperator{networkSinkDescriptor.value()}
            .withTraitSet(TraitSet{outputOriginIds})
            .withInferredSchema({channel.upstreamOp.getOutputSchema()})};
}

void QueryDecomposer::addPlanToNode(LogicalOperator&& op, const Topology::NodeId& nodeId)
{
    plansByNode[nodeId].emplace_back(LogicalPlan{{std::move(op)}, context.sqlString});
}

}
