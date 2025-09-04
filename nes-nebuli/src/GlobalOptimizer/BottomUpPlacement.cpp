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

#include <GlobalOptimizer/BottomUpPlacement.hpp>

#include <algorithm>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <GlobalOptimizer/OperatorPlacement.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/Trait.hpp>
#include <ErrorHandling.hpp>
#include <NetworkTopology.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{

Topology::NodeId
BottomUpOperatorPlacer::findFirstNodeWithCapacityFromLCAs(const std::vector<Topology::LowestCommonAncestor>& candidates) const
{
    for (const auto& [lca, paths] : candidates)
    {
        const auto lcaIt = std::ranges::find(paths.first.path, lca);

        /// Walk from LCA to dest along the common suffix
        for (auto it = lcaIt; it != paths.first.path.end(); ++it)
        {
            if (context.workerCatalog->tryConsumeCapacity(*it, 1) > 0)
            {
                return *it; /// First node with capacity
            }
        }
    }
    throw PlacementFailure("No capacity left on all possible paths from the first common node to the sink node");
}

BottomUpOperatorPlacer::BottomUpOperatorPlacer(QueryPlanningContext& context) : OperatorPlacer{}, context{context}
{
}

BottomUpOperatorPlacer BottomUpOperatorPlacer::with(QueryPlanningContext& context)
{
    return BottomUpOperatorPlacer{context};
}

PlanStage::PlacedLogicalPlan BottomUpOperatorPlacer::place(PlanStage::OptimizedLogicalPlan&& inputPlan) &&
{
    const LogicalPlan plan = std::move(inputPlan.plan);
    PRECONDITION(plan.getRootOperators().size() == 1, "BUG: placement requires queries to have a single sink");
    const LogicalOperator root = plan.getRootOperators().at(0);
    PRECONDITION(root.tryGet<SinkLogicalOperator>().has_value(), "BUG: root operator is required to be a sink");
    sinkPlacement.emplace(root.get<SinkLogicalOperator>().getSinkDescriptor()->getWorkerId());

    return PlanStage::PlacedLogicalPlan{LogicalPlan{plan.getQueryId(), {this->placeOperatorRecursive(root)}, plan.getOriginalSql()}};
}

LogicalOperator BottomUpOperatorPlacer::placeOperatorRecursive(const LogicalOperator& op)
{
    LogicalOperator placedOp;

    /// Place all children operators before placing the operator itself
    switch (const auto& children = op.getChildren(); children.size())
    {
        case 0: { /// Operator has no children --> source
            placedOp = placeOperatorOnNode(op, op.get<SourceDescriptorLogicalOperator>().getSourceDescriptor().getWorkerId());
            break;
        }
        case 1: /// Operator has a single child (aggregation, selection, map, projection, etc.), place child recursively
            placedOp = this->placeUnaryOperator(op, children.front());
            break;
        case 2: /// Operator with two children (join, union, etc.)
            placedOp = this->placeBinaryOperator(op, children[0], children[1]);
            break;
        default: /// More than two children are not allowed
            throw PlacementFailure("Operators with up to two children supported, but got {}", children.size());
    }

    return placedOp;
}

LogicalOperator
BottomUpOperatorPlacer::placeBinaryOperator(const LogicalOperator& op, const LogicalOperator& leftChild, const LogicalOperator& rightChild)
{
    /// Place left subtree
    LogicalOperator placedLeftChild = leftChild;
    std::optional<Topology::NodeId> leftChildPlacement = getPlacementFor(leftChild);
    if (not leftChildPlacement.has_value())
    {
        placedLeftChild = this->placeOperatorRecursive(leftChild);
        INVARIANT(
            hasTrait<PlacementTrait>(placedLeftChild.getTraitSet()),
            "BUG: left child {} has to be placed prior to placing operator {}",
            leftChild,
            op);
        leftChildPlacement.emplace(getTrait<PlacementTrait>(placedLeftChild.getTraitSet()).value().onNode);
    }

    /// Place right subtree
    LogicalOperator placedRightChild = rightChild;
    std::optional<Topology::NodeId> rightChildPlacement = getPlacementFor(rightChild);
    if (not rightChildPlacement.has_value())
    {
        placedRightChild = this->placeOperatorRecursive(rightChild);
        INVARIANT(
            hasTrait<PlacementTrait>(placedRightChild.getTraitSet()),
            "BUG: right child {} has to be placed prior to placing operator {}",
            rightChild,
            op);
        rightChildPlacement.emplace(getTrait<PlacementTrait>(placedRightChild.getTraitSet()).value().onNode);
    }

    /// Place operator after both children have been placed
    /// Do this by looking for the first common node (LCA) in the topology
    const auto candidates = context.workerCatalog->getTopology().findLCA(
        *leftChildPlacement, *rightChildPlacement, *sinkPlacement, Topology::Direction::Downstream);

    if (candidates.empty())
    {
        throw PlacementFailure(
            "No common node found between start nodes [{}] and [{}] to destination node [{}]",
            *leftChildPlacement,
            *rightChildPlacement,
            sinkPlacement);
    }
    return placeOperatorOnNode(op, findFirstNodeWithCapacityFromLCAs(candidates)).withChildren({placedLeftChild, placedRightChild});
}

LogicalOperator BottomUpOperatorPlacer::placeUnaryOperator(const LogicalOperator& op, const LogicalOperator& child)
{
    auto childPlacement = getPlacementFor(child);
    LogicalOperator placedChild = child;
    /// If the child has no placement yet, recurse and do placement
    if (not childPlacement.has_value())
    {
        placedChild = this->placeOperatorRecursive(child);
        INVARIANT(
            hasTrait<PlacementTrait>(placedChild.getTraitSet()), "BUG: child {} has to be placed prior to placing operator {}", child, op);

        /// Retrieve the placement of the single child of the operator
        childPlacement.emplace(getTrait<PlacementTrait>(placedChild.getTraitSet()).value().onNode);
    }

    /// We are ready to place the operator itself
    /// In op is the sink of the plan, simply retrieve the placement from its descriptor
    if (op.tryGet<SinkLogicalOperator>().has_value())
    {
        return placeOperatorOnNode(op, *sinkPlacement).withChildren({placedChild});
    }
    /// Otherwise, find node on the path between the childPlacement and the sink (root op) placement in the plan
    const auto& opPlacement = findFirstNodeWithCapacityOnPath(*childPlacement, *sinkPlacement);
    /// Reduce capacity on the selected node and place the operator
    return placeOperatorOnNode(op, opPlacement).withChildren({placedChild});
}

Topology::NodeId BottomUpOperatorPlacer::findFirstNodeWithCapacityOnPath(const Topology::NodeId& src, const Topology::NodeId& dest) const
{
    const auto paths = context.workerCatalog->getTopology().findPaths(src, dest, Topology::Direction::Downstream);
    if (paths.empty())
    {
        throw PlacementFailure("No path found between [{}] and [{}]", src, dest);
    }

    for (const auto& nodeId : paths.front())
    {
        if (context.workerCatalog->tryConsumeCapacity(nodeId, 1))
        {
            return nodeId;
        }
    }
    throw PlacementFailure("Not enough capacities");
}

}
