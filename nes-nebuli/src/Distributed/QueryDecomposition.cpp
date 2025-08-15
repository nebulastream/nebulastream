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

#include <Distributed/QueryDecomposition.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <optional>
#include <queue>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <uuid/uuid.h>

#include <fmt/format.h>

#include <Distributed/BridgeLogicalOperator.hpp>
#include <Distributed/NetworkTopology.hpp>
#include <Distributed/OperatorPlacement.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <LegacyOptimizer/Phases/TypeInferencePhase.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <ErrorHandling.hpp>

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

QueryDecomposer::QueryDecomposer(
    OperatorPlacer::PlacedLogicalPlan&& placedPlan,
    const TopologyGraph& topology,
    SharedPtr<SourceCatalog> sourceCatalog,
    SharedPtr<SinkCatalog> sinkCatalog)
    : inputPlan{std::move(placedPlan.plan)}, topology{topology}, sourceCatalog{copyPtr(sourceCatalog)}, sinkCatalog{copyPtr(sinkCatalog)}
{
}

QueryDecomposer QueryDecomposer::from(
    OperatorPlacer::PlacedLogicalPlan&& placedPlan,
    const TopologyGraph& topology,
    SharedPtr<SourceCatalog> sourceCatalog,
    SharedPtr<SinkCatalog> sinkCatalog)
{
    return {std::move(placedPlan), topology, std::move(sourceCatalog), std::move(sinkCatalog)};
}

QueryDecomposer::DecomposedLogicalPlan QueryDecomposer::decompose() &&
{
    PRECONDITION(inputPlan.getRootOperators().size() == 1, "BUG: query decomposition requires a single root operator");
    PRECONDITION(
        std::ranges::all_of(
            BFSRange(inputPlan.getRootOperators().front()), [](const auto& op) { return hasTrait<PlacementTrait>(op.getTraitSet()); }),
        "BUG: query decomposition requires placement of all operators");

    auto root = decomposePlanRecursive(inputPlan.getRootOperators().front());
    const auto rootPlacement = *getPlacementFor(root);
    addPlanToNode(std::move(root), rootPlacement);

    for (const auto& [node, plans] : plansByNode)
    {
        for (const auto& plan : plans)
        {
            NES_DEBUG("Plan fragment on node [{}]: {}", node, plan);
        }
    }
    return plansByNode;
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

LogicalOperator QueryDecomposer::createNetworkChannel(
    const LogicalOperator& op,
    const TopologyGraph::NodeId& startNode,
    const TopologyGraph::NodeId& endNode)
{
    /// Ask the topology for a path of nodes that connects upstream and downstream, currently we use any of them
    const auto [path] = topology.findPaths(startNode, endNode, TopologyGraph::Direction::Downstream).front();
    INVARIANT(path.size() >= 2, "Path from {} to {} must contain at least 2 nodes", startNode, endNode);

    LogicalOperator currentOp = op;
    for (size_t i = 0; i < path.size() - 1; ++i)
    {
        const auto& upstreamNode = path.at(i);
        const auto& downstreamNode = path.at(i + 1);

        auto [networkSource, networkSink] = connect(
            NetworkChannel{
                .id = generateUUID(),
                .upstreamOp = op,
                .upstreamNode = upstreamNode,
                .downstreamNode = downstreamNode});
        addPlanToNode(networkSink.withChildren({std::move(currentOp)}), upstreamNode);
        currentOp = networkSource;
    }

    return currentOp;
}

QueryDecomposer::Bridge QueryDecomposer::connect(NetworkChannel channel)
{
    const auto logicalSource
        = sourceCatalog->addLogicalSource(channel.id, channel.upstreamOp.getOutputSchema(), SourceCatalog::SourceIngestionType::Forward)
              .value();

    auto networkSourceDescriptor = sourceCatalog
                                       ->addPhysicalSource(
                                           logicalSource,
                                           "Network",
                                           channel.downstreamNode,
                                           {
                                               {"channel", channel.id},
                                           },
                                           ParserConfig{.parserType = "Native", .tupleDelimiter = "", .fieldDelimiter = ""})
                                       .value();

    auto networkSinkDescriptor = sinkCatalog->addSinkDescriptor(
        channel.id,
        channel.upstreamOp.getOutputSchema(),
        "Network",
        channel.upstreamNode,
        {{"channel", channel.id}, {"connection", channel.downstreamNode}});

    INVARIANT(networkSinkDescriptor.has_value(), "Invalid sink descriptor config for network sink");
    return Bridge{
        SourceDescriptorLogicalOperator{std::move(networkSourceDescriptor)}.withInputOriginIds({channel.upstreamOp.getOutputOriginIds()}),
        SinkLogicalOperator{std::move(*networkSinkDescriptor)}
            .withInputOriginIds({channel.upstreamOp.getOutputOriginIds()})
            .withOutputOriginIds(channel.upstreamOp.getOutputOriginIds())
            .withInferredSchema({channel.upstreamOp.getOutputSchema()})};
}

void QueryDecomposer::addPlanToNode(LogicalOperator&& op, const TopologyGraph::NodeId& nodeId)
{
    plansByNode[nodeId].emplace_back(LogicalPlan{inputPlan.getQueryId(), {std::move(op)}, inputPlan.getOriginalSql()});
}

}
