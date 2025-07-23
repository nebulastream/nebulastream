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
#pragma once

#include <optional>
#include <vector>

#include <Distributed/NetworkTopology.hpp>
#include <Distributed/NodeCatalog.hpp>
#include <LegacyOptimizer/LegacyOptimizer.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>

namespace NES
{

/// Obtain the placement for op, return empty optional if placement trait is not present in op
std::optional<TopologyGraph::NodeId> getPlacementFor(const LogicalOperator& op);
/// Place operator op on node nodeId by adding the placement trait
LogicalOperator placeOperatorOnNode(const LogicalOperator& op, const TopologyGraph::NodeId& nodeId);

/// Placement that traverses the topology from bottom-to-top based on the given capacities of the nodes.
/// This does not change the structure of the plan in any way, it only assigns placement traits with node placements to the given
/// logical operators.
class OperatorPlacer
{
    /// Since this object is only usable as an rvalue that is consumed by the place method, storing refs here is safe
    TopologyGraph& topology;
    NodeCatalog& nodeCatalog;
    std::optional<TopologyGraph::NodeId> sinkPlacement;

    OperatorPlacer(TopologyGraph& topology, NodeCatalog& nodeCatalog);

public:
    static OperatorPlacer from(TopologyGraph& topology, NodeCatalog& nodeCatalog);

    OperatorPlacer() = delete;
    ~OperatorPlacer() = default;

    OperatorPlacer(const OperatorPlacer&) = delete;
    OperatorPlacer(OperatorPlacer&&) = delete;
    OperatorPlacer& operator=(const OperatorPlacer&) = delete;
    OperatorPlacer& operator=(OperatorPlacer&&) = delete;

    struct PlacedLogicalPlan
    {
        LogicalPlan plan;
    };
    PlacedLogicalPlan place(LegacyOptimizer::OptimizedLogicalPlan&& inputPlan) &&;

private:
    LogicalOperator placeOperatorRecursive(const LogicalOperator& op);
    LogicalOperator placeUnaryOperator(const LogicalOperator& op, const LogicalOperator& child);
    LogicalOperator placeBinaryOperator(const LogicalOperator& op, const LogicalOperator& leftChild, const LogicalOperator& rightChild);

    /// Returns the first node with left capacity on any paths between src and dest
    /// @param src start node
    /// @param dest destination node
    /// @throws PlacementFailure, if no such node exists in the topology
    [[nodiscard]] TopologyGraph::NodeId
    findFirstNodeWithCapacityOnPath(const TopologyGraph::NodeId& src, const TopologyGraph::NodeId& dest) const;

    /// Returns the first node with left capacity on any paths between the LCAs of two source nodes
    /// @param candidates paths from the LCA between two start nodes to a destination node
    /// @throws PlacementFailure, if no such node exists in the topology
    [[nodiscard]] TopologyGraph::NodeId
    findFirstNodeWithCapacityFromLCAs(const std::vector<TopologyGraph::LowestCommonAncestor>& candidates) const;
};
}
