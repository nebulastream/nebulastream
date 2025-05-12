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

#include <Distributed/OperatorPlacement.hpp>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <ranges>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Distributed/NetworkTopology.hpp>
#include <Distributed/NodeCatalog.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/Trait.hpp>

#include <LegacyOptimizer/LegacyOptimizer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
std::optional<TopologyGraph::NodeId> getPlacementFor(const LogicalOperator& op)
{
    return getTrait<PlacementTrait>(op.getTraitSet()).transform([](const PlacementTrait& placement) { return placement.onNode; });
}

static LogicalOperator placeOperatorOnNode(const LogicalOperator& op, const TopologyGraph::NodeId& nodeId)
{
    TraitSet traitSetWithPlacement = op.getTraitSet();
    traitSetWithPlacement.emplace_back(PlacementTrait{nodeId});
    return op.withTraitSet(traitSetWithPlacement);
}

std::optional<TopologyGraph::NodeId> OperatorPlacer::findNodeWithCapacity(
    const std::vector<std::tuple<std::optional<TopologyGraph::NodeId>, TopologyGraph::Path, TopologyGraph::Path>>& candidates) const
{
    for (const auto& [lca, path1, path2] : candidates)
    {
        if (!lca)
        {
            continue;
        }

        const auto lcaIt = std::ranges::find(path1.path, lca.value());

        // Walk from LCA to dest along the common suffix
        for (auto it = lcaIt; it != path1.path.end(); ++it)
        {
            if (nodeCatalog.getCapacityFor(*it) > 0)
            {
                return *it; // First node with capacity
            }
        }
    }
    return std::nullopt;
}

OperatorPlacer::OperatorPlacer(
    LegacyOptimizer::OptimizedLogicalPlan&& inputPlan, TopologyGraph& topology, NodeCatalog& nodeCatalog, SinkCatalog& sinkCatalog)
    : inputPlan{std::move(inputPlan.plan)}, topology{topology}, nodeCatalog{nodeCatalog}, sinkCatalog{sinkCatalog}
{
}

OperatorPlacer OperatorPlacer::from(
    LegacyOptimizer::OptimizedLogicalPlan&& inputPlan, TopologyGraph& topology, NodeCatalog& nodeCatalog, SinkCatalog& sinkCatalog)
{
    return OperatorPlacer{std::move(inputPlan), topology, nodeCatalog, sinkCatalog};
}

OperatorPlacer::PlacedLogicalPlan OperatorPlacer::place() &&
{
    PRECONDITION(inputPlan.getRootOperators().size() == 1, "BottomUpPlacement requires queries to have a single sink");
    const auto rootOp = inputPlan.getRootOperators().front();
    PRECONDITION(rootOp.tryGet<SinkLogicalOperator>().has_value(), "The root of the plan needs to be the sink");
    placeSources();
    placeSinks();
    PRECONDITION(hasTrait<PlacementTrait>(rootOp.getTraitSet()), "The sink needs to be pinned before placement");
    const auto& sources = getLeafOperators(inputPlan);
    PRECONDITION(
        std::ranges::all_of(
            sources.cbegin(), sources.cend(), [](const auto& source) { return hasTrait<PlacementTrait>(source.getTraitSet()); }),
        "The sources need to be pinned before placement");

    return PlacedLogicalPlan{
        .plan = LogicalPlan{inputPlan.getQueryId(), {this->placeOperatorRecursive(rootOp)}, inputPlan.getOriginalSql()}};
}

LogicalOperator OperatorPlacer::placeOperatorRecursive(const LogicalOperator& op)
{
    /// Place all children operators before placing the operator itself
    switch (const auto children = op.getChildren(); children.size())
    {
        case 0: /// Operator has no children --> source, already placed and can not have children
            return op;
        case 1: /// Operator has a single child (aggregation, selection, map, projection, etc.), place child recursively
            return this->placeUnaryOperator(op, children.front());
        case 2: /// Operator with two children (join, union, etc.)
            return this->placeBinaryOperator(op, children[0], children[1]);
        default: /// More than two children are not allowed
            throw InvalidQueryPlan("BottomUpPlacement only supports operators with up to two children");
    }
}

LogicalOperator
OperatorPlacer::placeBinaryOperator(const LogicalOperator& op, const LogicalOperator& leftChild, const LogicalOperator& rightChild)
{
    LogicalOperator placedLeftChild = leftChild;
    auto leftChildPlacement = getPlacementFor(leftChild);
    if (not leftChildPlacement.has_value())
    {
        placedLeftChild = placeOperatorRecursive(leftChild);
        leftChildPlacement.emplace(getTrait<PlacementTrait>(placedLeftChild.getTraitSet()).value().onNode);
    }

    LogicalOperator placedRightChild = rightChild;
    auto rightChildPlacement = getPlacementFor(rightChild);
    if (not rightChildPlacement.has_value())
    {
        placedRightChild = this->placeOperatorRecursive(rightChild);
        rightChildPlacement.emplace(getTrait<PlacementTrait>(placedRightChild.getTraitSet()).value().onNode);
    }

    /// Place operator after both children have been placed
    /// Do this by looking for the first common node in the topology
    const auto candidates = topology.findLCA(
        leftChildPlacement.value(),
        rightChildPlacement.value(),
        getPlacementFor(inputPlan.getRootOperators().front()).value(),
        TopologyGraph::Downstream);

    const auto selectedNode = findNodeWithCapacity(candidates);
    INVARIANT(selectedNode.has_value(), "Could not find a path from child operators to root operator");

    nodeCatalog.consumeCapacity(*selectedNode, 1);
    return placeOperatorOnNode(op, selectedNode.value()).withChildren({placedLeftChild, placedRightChild});
}

LogicalOperator OperatorPlacer::placeUnaryOperator(const LogicalOperator& op, const LogicalOperator& child)
{
    auto childPlacement = getPlacementFor(child);
    LogicalOperator placedChild = child;
    /// If the child has no placement yet, recurse and do placement
    if (not childPlacement.has_value())
    {
        placedChild = this->placeOperatorRecursive(child);
        /// INVARIANT: after this call, the subtree beginning with child has the placement trait attached
        /// Retrieve the placement of the single child of the operator
        childPlacement.emplace(getTrait<PlacementTrait>(placedChild.getTraitSet()).value().onNode);
    }
    /// We are ready to place the operator itself

    /// Operator is already placed, return and attach children
    if (getPlacementFor(op).has_value())
    {
        return op.withChildren({placedChild});
    }

    /// Find node on the path between the childPlacement and the sink (root op) placement in the plan
    if (const auto& operatorTargetNode
        = findFirstNodeWithCapacity(childPlacement.value(), getPlacementFor(inputPlan.getRootOperators().front()).value());
        operatorTargetNode.has_value())
    {
        /// Reduce capacity on the selected node and place the operator
        nodeCatalog.consumeCapacity(*operatorTargetNode, 1);
        return placeOperatorOnNode(op, operatorTargetNode.value()).withChildren({placedChild});
    }
    throw PlacementFailure("Not enough capacities");
}

std::optional<TopologyGraph::NodeId>
OperatorPlacer::findFirstNodeWithCapacity(const TopologyGraph::NodeId& start, const TopologyGraph::NodeId& nextPlacedOperatorNode) const
{
    const auto paths = topology.findPaths(start, nextPlacedOperatorNode, TopologyGraph::Downstream);
    if (paths.empty())
    {
        throw NoPathFound();
    }

    for (const auto& nodeId : paths.front())
    {
        if (nodeCatalog.hasCapacity(nodeId))
        {
            return {nodeId};
        }
    }
    return std::nullopt;
}

void OperatorPlacer::placeSources()
{
    LogicalPlan withPlacedSources = inputPlan;
    for (const auto& source : getOperatorByType<SourceDescriptorLogicalOperator>(inputPlan))
    {
        const auto& placement = source.getSourceDescriptor().getWorkerId();
        /// TODO(yschroeder97): this addition of a trait works only when there are no other traits that should be forwarded, fix
        withPlacedSources = *replaceOperator(withPlacedSources, source.id, source.withTraitSet({PlacementTrait{placement}}));
    }
    inputPlan = std::move(withPlacedSources);
}

void OperatorPlacer::placeSinks()
{
    const auto sink = getOperatorByType<SinkLogicalOperator>(inputPlan).front();
    const auto& placement = sinkCatalog.at(sink.sinkName);
    inputPlan = *replaceOperator(inputPlan, sink.id, sink.withTraitSet({PlacementTrait{placement}}));
}
}
