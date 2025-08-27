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

#include <GlobalOptimizer/QueryDecomposition.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <uuid/uuid.h>

#include <fmt/format.h>

#include <GlobalOptimizer/OperatorPlacement.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <ErrorHandling.hpp>
#include <NetworkTopology.hpp>

namespace NES
{

static std::string generateUUID()
{
    uuid_t bytes;
    std::array<char, 36 + 1> parsed{};
    uuid_generate(bytes);
    uuid_unparse(bytes, parsed.data());
    return {parsed.data(), 36};
}

QueryDecomposer::QueryDecomposer(QueryPlanningContext& context) : context{context}
{
}

QueryDecomposer QueryDecomposer::with(QueryPlanningContext& context)
{
    return QueryDecomposer{context};
}

PlanStage::DecomposedLogicalPlan QueryDecomposer::decompose(PlanStage::PlacedLogicalPlan&& placedPlan) &&
{
    PRECONDITION(placedPlan.plan.getRootOperators().size() == 1, "BUG: query decomposition requires a single root operator");
    PRECONDITION(
        std::ranges::all_of(
            BFSRange(placedPlan.plan.getRootOperators().front()),
            [](const auto& op) { return hasTrait<PlacementTrait>(op.getTraitSet()); }),
        "BUG: query decomposition requires placement of all operators");

    auto root = decomposePlanRecursive(placedPlan.plan.getRootOperators().front());
    addPlanToNode(std::move(root), *getPlacementFor(root));

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

    const auto opPlacement = *getPlacementFor(op);
    const auto childPlacement = *getPlacementFor(child);

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
    const auto [path] = context.workerCatalog->getTopology().findPaths(startNode, endNode, Topology::Direction::Downstream).front();
    INVARIANT(path.size() >= 2, "Path from {} to {} must contain at least 2 nodes", startNode, endNode);

    LogicalOperator currentOp = op;
    for (size_t i = 0; i < path.size() - 1; ++i)
    {
        const auto& upstreamNode = path.at(i);
        const auto& downstreamNode = path.at(i + 1);

        auto [networkSource, networkSink] = connect(
            NetworkChannel{.id = generateUUID(), .upstreamOp = op, .upstreamNode = upstreamNode, .downstreamNode = downstreamNode});
        addPlanToNode(networkSink.withChildren({std::move(currentOp)}), upstreamNode);
        currentOp = networkSource;
    }

    return currentOp;
}

QueryDecomposer::Bridge QueryDecomposer::connect(NetworkChannel channel)
{
    const auto logicalSource
        = context.sourceCatalog
              ->addLogicalSource(channel.id, channel.upstreamOp.getOutputSchema(), SourceCatalog::SourceIngestionType::Forward)
              .value();

    auto networkSourceDescriptor = context.sourceCatalog
                                       ->addPhysicalSource(
                                           logicalSource,
                                           "Network",
                                           channel.downstreamNode,
                                           {{"channel", channel.id}, {"bind", channel.downstreamNode}},
                                           ParserConfig{.parserType = "Native", .tupleDelimiter = "", .fieldDelimiter = ""})
                                       .value();

    auto networkSinkDescriptor = context.sinkCatalog->addSinkDescriptor(
        channel.id,
        channel.upstreamOp.getOutputSchema(),
        "Network",
        channel.upstreamNode,
        {{"channel", channel.id}, {"bind", channel.upstreamNode}, {"connection", channel.downstreamNode}});

    INVARIANT(networkSinkDescriptor.has_value(), "Invalid sink descriptor config for network sink");
    return Bridge{
        SourceDescriptorLogicalOperator{std::move(networkSourceDescriptor)}.withInputOriginIds({channel.upstreamOp.getOutputOriginIds()}),
        SinkLogicalOperator{std::move(*networkSinkDescriptor)}
            .withInputOriginIds({channel.upstreamOp.getOutputOriginIds()})
            .withOutputOriginIds(channel.upstreamOp.getOutputOriginIds())
            .withInferredSchema({channel.upstreamOp.getOutputSchema()})};
}

void QueryDecomposer::addPlanToNode(LogicalOperator&& op, const Topology::NodeId& nodeId)
{
    plansByNode[nodeId].emplace_back(LogicalPlan{context.id, {std::move(op)}, context.sqlString});
}

}
